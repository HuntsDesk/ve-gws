"""
Google Drive MCP Tools

This module provides MCP tools for interacting with Google Drive API.
"""

import asyncio
import logging
import io
import base64

from typing import Optional, List, Dict, Any, Callable, Awaitable, BinaryIO
from tempfile import NamedTemporaryFile, SpooledTemporaryFile
from urllib.parse import urlparse
from urllib.request import url2pathname
from pathlib import Path

import httpx
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from auth.service_decorator import require_google_service
from auth.oauth_config import is_stateless_mode
from core.attachment_storage import get_attachment_storage, get_attachment_url
from core.utils import (
    IMAGE_MIME_TYPES,
    encode_image_content,
    extract_office_xml_text,
    extract_pdf_text,
    handle_http_errors,
    validate_file_path,
)
from core.server import server
from core.config import get_transport_mode
from core.http_utils import (
    redact_url as _redact_url,
    ssrf_safe_stream as _ssrf_safe_stream,
)
from gdrive.drive_helpers import (
    DRIVE_QUERY_PATTERNS,
    FOLDER_MIME_TYPE,
    build_drive_list_params,
    check_public_link_permission,
    format_permission_info,
    get_drive_image_url,
    resolve_drive_item,
    resolve_file_type_mime,
    resolve_folder_id,
    validate_expiration_time,
    validate_share_role,
    validate_share_type,
)

logger = logging.getLogger(__name__)

DOWNLOAD_CHUNK_SIZE_BYTES = 256 * 1024  # 256 KB
UPLOAD_CHUNK_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (Google recommended minimum)
MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB safety limit for URL downloads


async def _stream_url_with_validation(
    url: str, write_chunk: Optional[Callable[[bytes], Awaitable[None]]] = None
) -> tuple[int, Optional[str]]:
    """Stream a remote file with shared status and size validation."""
    total_bytes = 0

    redacted_url = _redact_url(url)

    async with _ssrf_safe_stream(url) as resp:
        if resp.status_code != 200:
            request = getattr(resp, "request", None)
            if request is None:
                parsed_url = urlparse(url)
                request = httpx.Request(
                    "GET",
                    f"{parsed_url.scheme}://{redacted_url}",
                )
            raise httpx.HTTPStatusError(
                f"Failed to fetch file from URL: {redacted_url} (status {resp.status_code})",
                request=request,
                response=resp,
            )

        content_type = resp.headers.get("Content-Type")
        async for chunk in resp.aiter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
            total_bytes += len(chunk)
            if total_bytes > MAX_DOWNLOAD_BYTES:
                raise ValueError(
                    f"Download from {redacted_url} exceeded {MAX_DOWNLOAD_BYTES} byte limit "
                    f"({total_bytes} bytes)"
                )
            if write_chunk is not None:
                await write_chunk(chunk)

    return total_bytes, content_type


async def _download_url_to_bytes(
    url: str,
) -> tuple[BinaryIO, Optional[str]]:
    """Download a remote file into a spooled temporary file with bounded streaming."""
    spool = SpooledTemporaryFile(max_size=UPLOAD_CHUNK_SIZE_BYTES)

    try:

        async def _collect(chunk: bytes) -> None:
            await asyncio.to_thread(spool.write, chunk)

        _total_bytes, content_type = await _stream_url_with_validation(url, _collect)
        await asyncio.to_thread(spool.seek, 0)
        return spool, content_type
    except Exception:
        spool.close()
        raise


async def _get_file_size(file_obj: BinaryIO) -> int:
    """Measure a possibly spooled file off the event loop and restore position."""

    def _measure_size() -> int:
        file_obj.seek(0, io.SEEK_END)
        size = file_obj.tell()
        file_obj.seek(0)
        return size

    return await asyncio.to_thread(_measure_size)


@server.tool()
@handle_http_errors("search_drive_files", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def search_drive_files(
    service,
    user_google_email: str,
    query: str,
    page_size: int = 10,
    page_token: Optional[str] = None,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
    file_type: Optional[str] = None,
    detailed: bool = True,
    order_by: Optional[str] = None,
) -> str:
    """Search Drive (including shared drives) for files and folders.

    Free-text queries are auto-wrapped in `fullText contains '...'`;
    structured Drive queries pass through as-is. For listing a single
    folder by parent use list_drive_items. For file content use
    get_drive_file_content. Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        query: Free text (auto-wrapped) or a Drive query expression like
            `name contains 'Q3' and mimeType = 'application/pdf' and
            modifiedTime > '2026-01-01T00:00:00'`. Owner-based queries
            ("x@y.com in owners") do NOT work inside shared drives —
            search by modifiedTime and order_by="modifiedTime desc"
            instead.
        page_size: Max files returned, 1-1000. Default 10.
        page_token: Cursor from a prior response's `nextPageToken`.
        drive_id: Shared drive ID to restrict the search. Omit for My
            Drive + shared-with-me.
        include_items_from_all_drives: True (default) to include shared
            drive items when drive_id is omitted.
        corpora: Scope — "user", "domain", "drive", or "allDrives".
            Defaults to "drive" when drive_id is set. Prefer "user" or
            "drive" over "allDrives" for performance.
        file_type: Friendly alias ("folder", "doc", "sheet", "slides",
            "form", "drawing", "pdf", "shortcut", "script", "site",
            "jamboard") or raw MIME type ("application/pdf"). Adds a
            mimeType filter.
        detailed: True (default) includes size, modified time, webViewLink
            per file; False returns just name/ID/type.
        order_by: Comma-separated sort keys: createdTime, folder,
            modifiedByMeTime, modifiedTime, name, name_natural,
            quotaBytesUsed, recency, sharedWithMeTime, starred,
            viewedByMeTime. Append " desc" to reverse. Example:
            "folder,modifiedTime desc,name".

    Returns:
        Formatted list with one line per hit, plus a `nextPageToken: ...`
        line when more results exist.
    """
    logger.info(
        f"[search_drive_files] Invoked. Email: '{user_google_email}', Query: '{query}', file_type: '{file_type}'"
    )

    # Check if the query looks like a structured Drive query or free text
    # Look for Drive API operators and structured query patterns
    is_structured_query = any(pattern.search(query) for pattern in DRIVE_QUERY_PATTERNS)

    if is_structured_query:
        final_query = query
        logger.info(
            f"[search_drive_files] Using structured query as-is: '{final_query}'"
        )
    else:
        # For free text queries, wrap in fullText contains
        escaped_query = query.replace("'", "\\'")
        final_query = f"fullText contains '{escaped_query}'"
        logger.info(
            f"[search_drive_files] Reformatting free text query '{query}' to '{final_query}'"
        )

    if file_type is not None:
        mime = resolve_file_type_mime(file_type)
        final_query = f"({final_query}) and mimeType = '{mime}'"
        logger.info(f"[search_drive_files] Added mimeType filter: '{mime}'")

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
        page_token=page_token,
        detailed=detailed,
        order_by=order_by,
    )

    results = await asyncio.to_thread(service.files().list(**list_params).execute)
    files = results.get("files", [])
    if not files:
        return f"No files found for '{query}'."

    next_token = results.get("nextPageToken")
    header = f"Found {len(files)} files for {user_google_email} matching '{query}':"
    formatted_files_text_parts = [header]
    for item in files:
        if detailed:
            size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
            formatted_files_text_parts.append(
                f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}{size_str}, Modified: {item.get("modifiedTime", "N/A")}) Link: {item.get("webViewLink", "#")}'
            )
        else:
            formatted_files_text_parts.append(
                f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]})'
            )
    if next_token:
        formatted_files_text_parts.append(f"nextPageToken: {next_token}")
    text_output = "\n".join(formatted_files_text_parts)
    return text_output


@server.tool()
@handle_http_errors("get_drive_file_content", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_file_content(
    service,
    user_google_email: str,
    file_id: str,
) -> str:
    """Download a Drive file and return its text (auto-extracting per type).

    Use this when you need file text; for a URL to the raw bytes use
    get_drive_file_download_url, for metadata use get_file_metadata (in
    hosted clients) or list fields. Handles shared drives. Extraction:
    Google Docs/Sheets/Slides export to text/CSV; Office .docx/.xlsx/.pptx
    unzipped and parsed; PDFs extracted with pypdf (scanned PDFs fall back
    to a download hint); images returned as base64 for multimodal
    clients; other files decoded as UTF-8 or flagged binary. Requires the
    drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file ID from search_drive_files, list_drive_items,
            or the URL like drive.google.com/file/d/<id>/view.

    Returns:
        Block with filename, ID, MIME type, webViewLink, then a
        "--- CONTENT ---" section containing the extracted text or a
        bracketed note for binary/unsupported content.
    """
    logger.info(f"[get_drive_file_content] Invoked. File ID: '{file_id}'")

    resolved_file_id, file_metadata = await resolve_drive_item(
        service,
        file_id,
        extra_fields="name, webViewLink",
    )
    file_id = resolved_file_id
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    export_mime_type = {
        "application/vnd.google-apps.document": "text/plain",
        "application/vnd.google-apps.spreadsheet": "text/csv",
        "application/vnd.google-apps.presentation": "text/plain",
    }.get(mime_type)

    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    loop = asyncio.get_event_loop()
    done = False
    while not done:
        status, done = await loop.run_in_executor(None, downloader.next_chunk)

    file_content_bytes = fh.getvalue()

    # Attempt Office XML extraction only for actual Office XML files
    office_mime_types = {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    if mime_type in office_mime_types:
        # Offload Office XML extraction to a thread to avoid blocking the event loop
        office_text = await asyncio.to_thread(
            extract_office_xml_text, file_content_bytes, mime_type
        )
        if office_text:
            body_text = office_text
        else:
            # Fallback: try UTF-8; otherwise flag binary
            try:
                body_text = file_content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                body_text = (
                    f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                    f"{len(file_content_bytes)} bytes]"
                )
    elif mime_type == "application/pdf":
        # Offload PDF text extraction to a thread to avoid blocking the event loop
        pdf_text = await asyncio.to_thread(extract_pdf_text, file_content_bytes)
        if pdf_text:
            body_text = pdf_text
        else:
            body_text = (
                f"[Could not extract text from PDF ({len(file_content_bytes)} bytes) "
                f"- the file may be scanned/image-only. "
                f"Use get_drive_file_download_url to get a direct download link instead.]"
            )
    elif mime_type in IMAGE_MIME_TYPES:
        body_text = encode_image_content(file_content_bytes, mime_type)
    else:
        # For non-Office files (including Google native files), try UTF-8 decode directly
        try:
            body_text = file_content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            body_text = (
                f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                f"{len(file_content_bytes)} bytes]"
            )

    # Assemble response
    header = (
        f'File: "{file_name}" (ID: {file_id}, Type: {mime_type})\n'
        f"Link: {file_metadata.get('webViewLink', '#')}\n\n--- CONTENT ---\n"
    )
    return header + body_text


@server.tool()
@handle_http_errors(
    "get_drive_file_download_url", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def get_drive_file_download_url(
    service,
    user_google_email: str,
    file_id: str,
    export_format: Optional[str] = None,
) -> str:
    """Save a Drive file to disk (or expose a temporary URL).

    Side effects: writes a file to the configured attachment storage
    (stdio mode) or publishes a download URL valid for 1 hour (HTTP
    mode). For file text content use get_drive_file_content instead;
    use this when you specifically need the binary file or an export.
    Google-native files are exported — Docs → PDF or DOCX; Sheets → XLSX,
    PDF, or CSV; Slides → PDF or PPTX. Other files download as-is.
    Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file ID from search_drive_files or a URL like
            drive.google.com/file/d/<id>/view.
        export_format: Export target for Google-native files. Docs:
            "pdf" (default) or "docx". Sheets: "xlsx" (default), "pdf",
            or "csv". Slides: "pdf" (default) or "pptx". Ignored for
            non-native files.

    Returns:
        Block with filename, file ID, size in KB, MIME type, and either
        a local file path (stdio) or a 1-hour download URL (HTTP), plus
        a note when a native file was exported.
    """
    logger.info(
        f"[get_drive_file_download_url] Invoked. File ID: '{file_id}', Export format: {export_format}"
    )

    # Resolve shortcuts and get file metadata
    resolved_file_id, file_metadata = await resolve_drive_item(
        service,
        file_id,
        extra_fields="name, webViewLink, mimeType",
    )
    file_id = resolved_file_id
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")

    # Determine export format for Google native files
    export_mime_type = None
    output_filename = file_name
    output_mime_type = mime_type

    if mime_type == "application/vnd.google-apps.document":
        # Google Docs
        if export_format == "docx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".docx"):
                output_filename = f"{Path(output_filename).stem}.docx"
        else:
            # Default to PDF
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"

    elif mime_type == "application/vnd.google-apps.spreadsheet":
        # Google Sheets
        if export_format == "csv":
            export_mime_type = "text/csv"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".csv"):
                output_filename = f"{Path(output_filename).stem}.csv"
        elif export_format == "pdf":
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"
        else:
            # Default to XLSX
            export_mime_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            output_mime_type = export_mime_type
            if not output_filename.endswith(".xlsx"):
                output_filename = f"{Path(output_filename).stem}.xlsx"

    elif mime_type == "application/vnd.google-apps.presentation":
        # Google Slides
        if export_format == "pptx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pptx"):
                output_filename = f"{Path(output_filename).stem}.pptx"
        else:
            # Default to PDF
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"

    # Download the file
    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    loop = asyncio.get_event_loop()
    done = False
    while not done:
        status, done = await loop.run_in_executor(None, downloader.next_chunk)

    file_content_bytes = fh.getvalue()
    size_bytes = len(file_content_bytes)
    size_kb = size_bytes / 1024 if size_bytes else 0

    # Check if we're in stateless mode (can't save files)
    if is_stateless_mode():
        result_lines = [
            "File downloaded successfully!",
            f"File: {file_name}",
            f"File ID: {file_id}",
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
            f"MIME Type: {output_mime_type}",
            "\n⚠️ Stateless mode: File storage disabled.",
            "\nBase64-encoded content (first 100 characters shown):",
            f"{base64.b64encode(file_content_bytes[:100]).decode('utf-8')}...",
        ]
        logger.info(
            f"[get_drive_file_download_url] Successfully downloaded {size_kb:.1f} KB file (stateless mode)"
        )
        return "\n".join(result_lines)

    # Save file to local disk and return file path
    try:
        storage = get_attachment_storage()

        # Encode bytes to base64 (as expected by AttachmentStorage)
        base64_data = base64.urlsafe_b64encode(file_content_bytes).decode("utf-8")

        # Save attachment to local disk
        result = storage.save_attachment(
            base64_data=base64_data,
            filename=output_filename,
            mime_type=output_mime_type,
        )

        result_lines = [
            "File downloaded successfully!",
            f"File: {file_name}",
            f"File ID: {file_id}",
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
            f"MIME Type: {output_mime_type}",
        ]

        if get_transport_mode() == "stdio":
            result_lines.append(f"\n📎 Saved to: {result.path}")
            result_lines.append(
                "\nThe file has been saved to disk and can be accessed directly via the file path."
            )
        else:
            download_url = get_attachment_url(result.file_id)
            result_lines.append(f"\n📎 Download URL: {download_url}")
            result_lines.append("\nThe file will expire after 1 hour.")

        if export_mime_type:
            result_lines.append(
                f"\nNote: Google native file exported to {output_mime_type} format."
            )

        logger.info(
            f"[get_drive_file_download_url] Successfully saved {size_kb:.1f} KB file to {result.path}"
        )
        return "\n".join(result_lines)

    except Exception as e:
        logger.error(f"[get_drive_file_download_url] Failed to save file: {e}")
        return (
            f"Error: Failed to save file for download.\n"
            f"File was downloaded successfully ({size_kb:.1f} KB) but could not be saved.\n\n"
            f"Error details: {str(e)}"
        )


@server.tool()
@handle_http_errors("list_drive_items", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_drive_items(
    service,
    user_google_email: str,
    folder_id: str = "root",
    page_size: int = 100,
    page_token: Optional[str] = None,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
    file_type: Optional[str] = None,
    detailed: bool = True,
    order_by: Optional[str] = None,
) -> str:
    """List files in one Drive folder (children of folder_id).

    Use this to browse by folder; for content-based search use
    search_drive_files. Scoped to a folder's direct children. If
    drive_id is set, folder_id is interpreted inside that shared drive.
    Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        folder_id: Folder ID to list. "root" = My Drive root. For a
            shared drive, pass the drive ID to list its root or a folder
            ID within it. Default "root".
        page_size: Max items returned, 1-1000. Default 100.
        page_token: Cursor from a prior response's `nextPageToken`.
        drive_id: Shared drive ID to scope the listing. Omit for My
            Drive + shared-with-me.
        include_items_from_all_drives: True (default) to include shared
            drive items when drive_id is omitted.
        corpora: "user", "drive", or "allDrives". Defaults to "drive"
            when drive_id is set.
        file_type: Friendly alias ("folder", "doc", "sheet", "slides",
            "pdf", etc.) or raw MIME type. Filters to that type only.
        detailed: True (default) includes size, modified time,
            webViewLink; False returns just name/ID/type.
        order_by: Comma-separated sort keys with optional " desc", e.g.
            "folder,modifiedTime desc". Valid keys: createdTime, folder,
            modifiedByMeTime, modifiedTime, name, name_natural,
            quotaBytesUsed, recency, sharedWithMeTime, starred,
            viewedByMeTime.

    Returns:
        Formatted list with one line per item, plus a `nextPageToken: ...`
        line when more results exist.
    """
    logger.info(
        f"[list_drive_items] Invoked. Email: '{user_google_email}', Folder ID: '{folder_id}', File Type: '{file_type}'"
    )

    resolved_folder_id = await resolve_folder_id(service, folder_id)
    final_query = f"'{resolved_folder_id}' in parents and trashed=false"

    if file_type is not None:
        mime = resolve_file_type_mime(file_type)
        final_query = f"({final_query}) and mimeType = '{mime}'"
        logger.info(f"[list_drive_items] Added mimeType filter: '{mime}'")

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
        page_token=page_token,
        detailed=detailed,
        order_by=order_by,
    )

    results = await asyncio.to_thread(service.files().list(**list_params).execute)
    files = results.get("files", [])
    if not files:
        return f"No items found in folder '{folder_id}'."

    next_token = results.get("nextPageToken")
    header = (
        f"Found {len(files)} items in folder '{folder_id}' for {user_google_email}:"
    )
    formatted_items_text_parts = [header]
    for item in files:
        if detailed:
            size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
            formatted_items_text_parts.append(
                f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}{size_str}, Modified: {item.get("modifiedTime", "N/A")}) Link: {item.get("webViewLink", "#")}'
            )
        else:
            formatted_items_text_parts.append(
                f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]})'
            )
    if next_token:
        formatted_items_text_parts.append(f"nextPageToken: {next_token}")
    text_output = "\n".join(formatted_items_text_parts)
    return text_output


async def _create_drive_folder_impl(
    service,
    user_google_email: str,
    folder_name: str,
    parent_folder_id: str = "root",
) -> str:
    """Internal implementation for create_drive_folder. Used by tests."""
    resolved_folder_id = await resolve_folder_id(service, parent_folder_id)
    file_metadata = {
        "name": folder_name,
        "parents": [resolved_folder_id],
        "mimeType": FOLDER_MIME_TYPE,
    }
    created_file = await asyncio.to_thread(
        service.files()
        .create(
            body=file_metadata,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )
    link = created_file.get("webViewLink", "")
    return (
        f"Successfully created folder '{created_file.get('name', folder_name)}' (ID: {created_file.get('id', 'N/A')}) "
        f"in folder '{parent_folder_id}' for {user_google_email}. Link: {link}"
    )


@server.tool()
@handle_http_errors("create_drive_folder", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_drive_folder(
    service,
    user_google_email: str,
    folder_name: str,
    parent_folder_id: str = "root",
) -> str:
    """Create a new folder in Drive (or inside a shared drive).

    Side effects: creates a folder owned by the authenticated user (or by
    the shared drive when parent_folder_id lives in one). To upload files
    into the new folder use create_drive_file with folder_id set to the
    returned ID. Requires the drive.file OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        folder_name: Display name for the new folder. Forward slashes are
            treated as literal characters, not nesting.
        parent_folder_id: Parent folder ID. "root" for My Drive root, or a
            folder ID within a shared drive for shared-drive folders.

    Returns:
        Confirmation with the new folder's name, ID, and webViewLink.
    """
    logger.info(
        f"[create_drive_folder] Invoked. Email: '{user_google_email}', Folder: '{folder_name}', Parent: '{parent_folder_id}'"
    )
    return await _create_drive_folder_impl(
        service, user_google_email, folder_name, parent_folder_id
    )


@server.tool()
@handle_http_errors("create_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_drive_file(
    service,
    user_google_email: str,
    file_name: str,
    content: Optional[str] = None,  # Now explicitly Optional
    folder_id: str = "root",
    mime_type: str = "text/plain",
    fileUrl: Optional[str] = None,  # Now explicitly Optional
) -> str:
    """Upload a file to Drive from content, a URL, or a local path.

    Side effects: creates a new Drive file. To convert source files
    (Markdown, DOCX, etc.) into native Google Docs use import_to_google_doc
    instead. For a brand-new empty Google Doc/Sheet/Slide use
    create_doc/create_spreadsheet/create_presentation. Requires the
    drive.file OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_name: Name for the new Drive file (include the file extension
            for clarity, though the MIME type is authoritative).
        content: Text content for the new file. Mutually exclusive with
            fileUrl. Provide one of content or fileUrl.
        folder_id: Parent folder ID. "root" = My Drive root; for shared
            drives pass a folder ID inside that drive. Default "root".
        mime_type: MIME type of the uploaded bytes. Default
            "text/plain". When fileUrl is used and the server responds
            with a Content-Type, it overrides this.
        fileUrl: Source URL — supports file:// (local path), http://, and
            https://. When provided, the bytes are streamed into Drive.
            SSRF-protected with size limits.

    Returns:
        Confirmation with the new file's name, ID, parent folder, and
        webViewLink.
    """
    logger.info(
        f"[create_drive_file] Invoked. Email: '{user_google_email}', File Name: {file_name}, Folder ID: {folder_id}, fileUrl: {fileUrl}"
    )

    if content is None and fileUrl is None and mime_type != FOLDER_MIME_TYPE:
        raise Exception("You must provide either 'content' or 'fileUrl'.")

    # Create folder (no content or media_body). Prefer create_drive_folder for new code.
    if mime_type == FOLDER_MIME_TYPE:
        return await _create_drive_folder_impl(
            service, user_google_email, file_name, folder_id
        )

    file_data = None
    resolved_folder_id = await resolve_folder_id(service, folder_id)

    file_metadata = {
        "name": file_name,
        "parents": [resolved_folder_id],
        "mimeType": mime_type,
    }

    # Prefer fileUrl if both are provided
    if fileUrl:
        logger.info(f"[create_drive_file] Fetching file from URL: {fileUrl}")

        # Check if this is a file:// URL
        parsed_url = urlparse(fileUrl)
        if parsed_url.scheme == "file":
            # Handle file:// URL - read from local filesystem
            logger.info(
                "[create_drive_file] Detected file:// URL, reading from local filesystem"
            )
            transport_mode = get_transport_mode()
            running_streamable = transport_mode == "streamable-http"
            if running_streamable:
                logger.warning(
                    "[create_drive_file] file:// URL requested while server runs in streamable-http mode. Ensure the file path is accessible to the server (e.g., Docker volume) or use an HTTP(S) URL."
                )

            # Convert file:// URL to a cross-platform local path
            raw_path = parsed_url.path or ""
            netloc = parsed_url.netloc
            if netloc and netloc.lower() != "localhost":
                raw_path = f"//{netloc}{raw_path}"
            file_path = url2pathname(raw_path)

            # Validate path safety and verify file exists
            path_obj = validate_file_path(file_path)
            if not path_obj.exists():
                extra = (
                    " The server is running via streamable-http, so file:// URLs must point to files inside the container or remote host."
                    if running_streamable
                    else ""
                )
                raise Exception(f"Local file does not exist: {file_path}.{extra}")
            if not path_obj.is_file():
                extra = (
                    " In streamable-http/Docker deployments, mount the file into the container or provide an HTTP(S) URL."
                    if running_streamable
                    else ""
                )
                raise Exception(f"Path is not a file: {file_path}.{extra}")

            logger.info(f"[create_drive_file] Reading local file: {file_path}")

            # Read file and upload
            file_data = await asyncio.to_thread(path_obj.read_bytes)
            total_bytes = len(file_data)
            logger.info(f"[create_drive_file] Read {total_bytes} bytes from local file")

            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype=mime_type,
                resumable=True,
                chunksize=UPLOAD_CHUNK_SIZE_BYTES,
            )

            logger.info("[create_drive_file] Starting upload to Google Drive...")
            created_file = await asyncio.to_thread(
                service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id, name, webViewLink",
                    supportsAllDrives=True,
                )
                .execute
            )
        # Handle HTTP/HTTPS URLs
        elif parsed_url.scheme in ("http", "https"):
            # when running in stateless mode, deployment may not have access to local file system
            if is_stateless_mode():
                with SpooledTemporaryFile(max_size=UPLOAD_CHUNK_SIZE_BYTES) as spool:

                    async def _write_spool(chunk: bytes) -> None:
                        await asyncio.to_thread(spool.write, chunk)

                    _total, content_type = await _stream_url_with_validation(
                        fileUrl, _write_spool
                    )
                    await asyncio.to_thread(spool.seek, 0)

                    # Try to get MIME type from Content-Type header
                    if content_type and content_type != "application/octet-stream":
                        mime_type = content_type
                        file_metadata["mimeType"] = content_type
                        logger.info(
                            f"[create_drive_file] Using MIME type from Content-Type header: {content_type}"
                        )

                    media = MediaIoBaseUpload(
                        spool,
                        mimetype=mime_type,
                        resumable=True,
                        chunksize=UPLOAD_CHUNK_SIZE_BYTES,
                    )

                    created_file = await asyncio.to_thread(
                        service.files()
                        .create(
                            body=file_metadata,
                            media_body=media,
                            fields="id, name, webViewLink",
                            supportsAllDrives=True,
                        )
                        .execute
                    )
            else:
                # Stream download to temp file with SSRF protection, then upload
                with NamedTemporaryFile() as temp_file:

                    async def _write_chunk(chunk: bytes) -> None:
                        await asyncio.to_thread(temp_file.write, chunk)

                    total_bytes, content_type = await _stream_url_with_validation(
                        fileUrl, _write_chunk
                    )

                    logger.info(
                        f"[create_drive_file] Downloaded {total_bytes} bytes "
                        f"from URL before upload."
                    )

                    if content_type and content_type != "application/octet-stream":
                        mime_type = content_type
                        file_metadata["mimeType"] = mime_type
                        logger.info(
                            f"[create_drive_file] Using MIME type from "
                            f"Content-Type header: {mime_type}"
                        )

                    # Reset file pointer to beginning for upload
                    temp_file.seek(0)

                    media = MediaIoBaseUpload(
                        temp_file,
                        mimetype=mime_type,
                        resumable=True,
                        chunksize=UPLOAD_CHUNK_SIZE_BYTES,
                    )

                    logger.info(
                        "[create_drive_file] Starting upload to Google Drive..."
                    )
                    created_file = await asyncio.to_thread(
                        service.files()
                        .create(
                            body=file_metadata,
                            media_body=media,
                            fields="id, name, webViewLink",
                            supportsAllDrives=True,
                        )
                        .execute
                    )
        else:
            if not parsed_url.scheme:
                raise Exception(
                    "fileUrl is missing a URL scheme. Use file://, http://, or https://."
                )
            raise Exception(
                f"Unsupported URL scheme '{parsed_url.scheme}'. Only file://, http://, and https:// are supported."
            )
    elif content is not None:
        file_data = content.encode("utf-8")
        media = io.BytesIO(file_data)

        created_file = await asyncio.to_thread(
            service.files()
            .create(
                body=file_metadata,
                media_body=MediaIoBaseUpload(media, mimetype=mime_type, resumable=True),
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )

    link = created_file.get("webViewLink", "No link available")
    confirmation_message = f"Successfully created file '{created_file.get('name', file_name)}' (ID: {created_file.get('id', 'N/A')}) in folder '{folder_id}' for {user_google_email}. Link: {link}"
    logger.info(f"Successfully created file. Link: {link}")
    return confirmation_message


# Mapping of file extensions to source MIME types for Google Docs conversion
GOOGLE_DOCS_IMPORT_FORMATS = {
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".txt": "text/plain",
    ".text": "text/plain",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".rtf": "application/rtf",
    ".odt": "application/vnd.oasis.opendocument.text",
}

GOOGLE_DOCS_MIME_TYPE = "application/vnd.google-apps.document"


def _detect_source_format(file_name: str, content: Optional[str] = None) -> str:
    """
    Detect the source MIME type based on file extension.
    Falls back to text/plain if unknown.
    """
    ext = Path(file_name).suffix.lower()
    if ext in GOOGLE_DOCS_IMPORT_FORMATS:
        return GOOGLE_DOCS_IMPORT_FORMATS[ext]

    # If content is provided and looks like markdown, use markdown
    if content and (content.startswith("#") or "```" in content or "**" in content):
        return "text/markdown"

    return "text/plain"


@server.tool()
@handle_http_errors("import_to_google_doc", service_type="drive")
@require_google_service("drive", "drive_file")
async def import_to_google_doc(
    service,
    user_google_email: str,
    file_name: str,
    content: Optional[str] = None,
    file_path: Optional[str] = None,
    file_url: Optional[str] = None,
    source_format: Optional[str] = None,
    folder_id: str = "root",
) -> str:
    """Convert a source file into a native Google Doc on upload.

    Drive performs the conversion server-side, preserving headings, lists,
    inline formatting, tables, etc. Use this (not create_drive_file) when
    you want a real Google Doc editable in the web UI. For adding content
    to an existing Doc use insert_doc_markdown or insert_doc_elements.
    Requires the drive.file OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_name: Display name for the resulting Google Doc (extension
            is stripped).
        content: Inline text for text formats (md, txt, html). Mutually
            exclusive with file_path and file_url.
        file_path: Local path or file:// URL to a binary source
            (docx/odt/rtf). Path safety validated.
        file_url: http:// or https:// URL to stream the source from.
            SSRF-protected with size limits.
        source_format: Override for format detection: "md"/"markdown",
            "docx", "txt", "html", "rtf", "odt". Auto-detected from
            file_name extension or content heuristics when omitted.
        folder_id: Parent folder ID. Default "root".

    Returns:
        str: Confirmation message with the new Google Doc link.

    Examples:
        # Import markdown content directly
        import_to_google_doc(file_name="My Doc.md", content="# Title\\n\\nHello **world**")

        # Import a local DOCX file
        import_to_google_doc(file_name="Report", file_path="/path/to/report.docx")

        # Import from URL
        import_to_google_doc(file_name="Remote Doc", file_url="https://example.com/doc.md")
    """
    logger.info(
        f"[import_to_google_doc] Invoked. Email: '{user_google_email}', "
        f"File Name: '{file_name}', Source Format: '{source_format}', Folder ID: '{folder_id}'"
    )

    # Validate inputs
    source_count = sum(1 for x in [content, file_path, file_url] if x is not None)
    if source_count == 0:
        raise ValueError(
            "You must provide one of: 'content', 'file_path', or 'file_url'."
        )
    if source_count > 1:
        raise ValueError("Provide only one of: 'content', 'file_path', or 'file_url'.")

    # Determine source MIME type
    if source_format:
        # Normalize format hint
        format_key = f".{source_format.lower().lstrip('.')}"
        if format_key in GOOGLE_DOCS_IMPORT_FORMATS:
            source_mime_type = GOOGLE_DOCS_IMPORT_FORMATS[format_key]
        else:
            raise ValueError(
                f"Unsupported source_format: '{source_format}'. "
                f"Supported: {', '.join(ext.lstrip('.') for ext in GOOGLE_DOCS_IMPORT_FORMATS.keys())}"
            )
    else:
        # Auto-detect from file_name, file_path, or file_url
        detection_name = file_path or file_url or file_name
        source_mime_type = _detect_source_format(detection_name, content)

    logger.info(f"[import_to_google_doc] Detected source MIME type: {source_mime_type}")

    # Clean up file name (remove extension since it becomes a Google Doc)
    doc_name = Path(file_name).stem if Path(file_name).suffix else file_name

    # Resolve folder
    resolved_folder_id = await resolve_folder_id(service, folder_id)

    # File metadata - destination is Google Docs format
    file_metadata = {
        "name": doc_name,
        "parents": [resolved_folder_id],
        "mimeType": GOOGLE_DOCS_MIME_TYPE,  # Target format = Google Docs
    }

    file_data: bytes
    remote_file_data: Optional[BinaryIO] = None
    remote_content_type: Optional[str] = None

    # Handle content (string input for text formats)
    if content is not None:
        file_data = content.encode("utf-8")
        logger.info(f"[import_to_google_doc] Using content: {len(file_data)} bytes")

    # Handle file_path (local file)
    elif file_path is not None:
        parsed_url = urlparse(file_path)

        # Handle file:// URL format
        if parsed_url.scheme == "file":
            raw_path = parsed_url.path or ""
            netloc = parsed_url.netloc
            if netloc and netloc.lower() != "localhost":
                raw_path = f"//{netloc}{raw_path}"
            actual_path = url2pathname(raw_path)
        elif parsed_url.scheme == "":
            # Regular path
            actual_path = file_path
        else:
            raise ValueError(
                f"file_path should be a local path or file:// URL, got: {file_path}"
            )

        path_obj = validate_file_path(actual_path)
        if not path_obj.exists():
            raise FileNotFoundError(f"File not found: {actual_path}")
        if not path_obj.is_file():
            raise ValueError(f"Path is not a file: {actual_path}")

        file_data = await asyncio.to_thread(path_obj.read_bytes)
        logger.info(f"[import_to_google_doc] Read local file: {len(file_data)} bytes")

        # Re-detect format from actual file if not specified
        if not source_format:
            source_mime_type = _detect_source_format(actual_path)
            logger.info(
                f"[import_to_google_doc] Re-detected from path: {source_mime_type}"
            )

    # Handle file_url (remote file)
    elif file_url is not None:
        parsed_url = urlparse(file_url)
        if parsed_url.scheme not in ("http", "https"):
            raise ValueError(f"file_url must be http:// or https://, got: {file_url}")

        # SSRF protection: block internal/private network URLs and validate redirects
        remote_file_data, remote_content_type = await _download_url_to_bytes(file_url)

    # Upload with conversion
    if remote_file_data is not None:
        with remote_file_data:
            remote_size = await _get_file_size(remote_file_data)

            logger.info(
                f"[import_to_google_doc] Downloaded from URL: {remote_size} bytes"
            )

            # Prefer the Content-Type from the download; fall back to URL-based detection
            if not source_format:
                ct_base = (remote_content_type or "").split(";", 1)[0].strip()
                if ct_base and ct_base != "application/octet-stream":
                    source_mime_type = ct_base
                    logger.info(
                        f"[import_to_google_doc] Using Content-Type from response: {source_mime_type}"
                    )
                else:
                    source_mime_type = _detect_source_format(file_url)
                    logger.info(
                        f"[import_to_google_doc] Detected from URL path: {source_mime_type}"
                    )

            media = MediaIoBaseUpload(
                remote_file_data,
                mimetype=source_mime_type,  # Source format
                resumable=True,
                chunksize=UPLOAD_CHUNK_SIZE_BYTES,
            )

            logger.info(
                f"[import_to_google_doc] Uploading to Google Drive with conversion: "
                f"{source_mime_type} → {GOOGLE_DOCS_MIME_TYPE}"
            )

            created_file = await asyncio.to_thread(
                service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id, name, webViewLink, mimeType",
                    supportsAllDrives=True,
                )
                .execute
            )
    else:
        media = MediaIoBaseUpload(
            io.BytesIO(file_data),
            mimetype=source_mime_type,  # Source format
            resumable=True,
            chunksize=UPLOAD_CHUNK_SIZE_BYTES,
        )

        logger.info(
            f"[import_to_google_doc] Uploading to Google Drive with conversion: "
            f"{source_mime_type} → {GOOGLE_DOCS_MIME_TYPE}"
        )

        created_file = await asyncio.to_thread(
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, webViewLink, mimeType",
                supportsAllDrives=True,
            )
            .execute
        )

    result_mime = created_file.get("mimeType", "unknown")
    if result_mime != GOOGLE_DOCS_MIME_TYPE:
        logger.warning(
            f"[import_to_google_doc] Conversion may have failed. "
            f"Expected {GOOGLE_DOCS_MIME_TYPE}, got {result_mime}"
        )

    link = created_file.get("webViewLink", "No link available")
    doc_id = created_file.get("id", "N/A")

    confirmation = (
        f"✅ Successfully imported '{doc_name}' as Google Doc\n"
        f"   Document ID: {doc_id}\n"
        f"   Source format: {source_mime_type}\n"
        f"   Folder: {folder_id}\n"
        f"   Link: {link}"
    )

    logger.info(f"[import_to_google_doc] Success. Link: {link}")
    return confirmation


@server.tool()
@handle_http_errors(
    "get_drive_file_permissions", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def get_drive_file_permissions(
    service,
    user_google_email: str,
    file_id: str,
) -> str:
    """Inspect a Drive file's sharing permissions and public-link status.

    Use this to audit who can access a file before sharing externally. To
    change sharing use set_drive_file_permissions or manage_drive_access.
    For a quick public-vs-private check by filename use
    check_drive_file_public_access. Requires the drive.readonly OAuth
    scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file ID from search_drive_files or a shareable
            URL.

    Returns:
        Block with file metadata (name, ID, type, size, modifiedTime),
        sharing status, per-permission details (type, role, email,
        domain, expiration), view/download URLs, and a verdict on
        whether the file can be embedded in Google Docs (requires
        "Anyone with the link").
    """
    logger.info(
        f"[get_drive_file_permissions] Checking file {file_id} for {user_google_email}"
    )

    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    try:
        # Get comprehensive file metadata including permissions with details
        file_metadata = await asyncio.to_thread(
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, size, modifiedTime, owners, "
                "permissions(id, type, role, emailAddress, domain, expirationTime, permissionDetails), "
                "webViewLink, webContentLink, shared, sharingUser, viewersCanCopyContent",
                supportsAllDrives=True,
            )
            .execute
        )

        # Format the response
        output_parts = [
            f"File: {file_metadata.get('name', 'Unknown')}",
            f"ID: {file_id}",
            f"Type: {file_metadata.get('mimeType', 'Unknown')}",
            f"Size: {file_metadata.get('size', 'N/A')} bytes",
            f"Modified: {file_metadata.get('modifiedTime', 'N/A')}",
            "",
            "Sharing Status:",
            f"  Shared: {file_metadata.get('shared', False)}",
        ]

        # Add sharing user if available
        sharing_user = file_metadata.get("sharingUser")
        if sharing_user:
            output_parts.append(
                f"  Shared by: {sharing_user.get('displayName', 'Unknown')} ({sharing_user.get('emailAddress', 'Unknown')})"
            )

        # Process permissions
        permissions = file_metadata.get("permissions", [])
        if permissions:
            output_parts.append(f"  Number of permissions: {len(permissions)}")
            output_parts.append("  Permissions:")
            for perm in permissions:
                output_parts.append(f"    - {format_permission_info(perm)}")
        else:
            output_parts.append("  No additional permissions (private file)")

        # Add URLs
        output_parts.extend(
            [
                "",
                "URLs:",
                f"  View Link: {file_metadata.get('webViewLink', 'N/A')}",
            ]
        )

        # webContentLink is only available for files that can be downloaded
        web_content_link = file_metadata.get("webContentLink")
        if web_content_link:
            output_parts.append(f"  Direct Download Link: {web_content_link}")

        has_public_link = check_public_link_permission(permissions)

        if has_public_link:
            output_parts.extend(
                [
                    "",
                    "✅ This file is shared with 'Anyone with the link' - it can be inserted into Google Docs",
                ]
            )
        else:
            output_parts.extend(
                [
                    "",
                    "❌ This file is NOT shared with 'Anyone with the link' - it cannot be inserted into Google Docs",
                    "   To fix: Right-click the file in Google Drive → Share → Anyone with the link → Viewer",
                ]
            )

        return "\n".join(output_parts)

    except Exception as e:
        logger.error(f"Error getting file permissions: {e}")
        return f"Error getting file permissions: {e}"


@server.tool()
@handle_http_errors(
    "check_drive_file_public_access", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def check_drive_file_public_access(
    service,
    user_google_email: str,
    file_name: str,
) -> str:
    """Search by filename and report whether the file is publicly linked.

    Quick helper for Google Docs embedding — a file must have "Anyone with
    the link" access before insert_doc_image can render it. If multiple
    files match the name, checks the first. For a specific file use
    get_drive_file_permissions. Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_name: Exact display name (case-sensitive) as shown in Drive.

    Returns:
        Block showing file name/ID/type, shared flag, and a verdict:
        either an embeddable URL to pass to insert_doc_image, or steps
        to enable public sharing.
    """
    logger.info(f"[check_drive_file_public_access] Searching for {file_name}")

    # Search for the file
    escaped_name = file_name.replace("'", "\\'")
    query = f"name = '{escaped_name}'"

    list_params = {
        "q": query,
        "pageSize": 10,
        "fields": "files(id, name, mimeType, webViewLink)",
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }

    results = await asyncio.to_thread(service.files().list(**list_params).execute)

    files = results.get("files", [])
    if not files:
        return f"No file found with name '{file_name}'"

    if len(files) > 1:
        output_parts = [f"Found {len(files)} files with name '{file_name}':"]
        for f in files:
            output_parts.append(f"  - {f['name']} (ID: {f['id']})")
        output_parts.append("\nChecking the first file...")
        output_parts.append("")
    else:
        output_parts = []

    # Check permissions for the first file
    file_id = files[0]["id"]
    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    # Get detailed permissions
    file_metadata = await asyncio.to_thread(
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, permissions, webViewLink, webContentLink, shared",
            supportsAllDrives=True,
        )
        .execute
    )

    permissions = file_metadata.get("permissions", [])

    has_public_link = check_public_link_permission(permissions)

    output_parts.extend(
        [
            f"File: {file_metadata['name']}",
            f"ID: {file_id}",
            f"Type: {file_metadata['mimeType']}",
            f"Shared: {file_metadata.get('shared', False)}",
            "",
        ]
    )

    if has_public_link:
        output_parts.extend(
            [
                "✅ PUBLIC ACCESS ENABLED - This file can be inserted into Google Docs",
                f"Use with insert_doc_image_url: {get_drive_image_url(file_id)}",
            ]
        )
    else:
        output_parts.extend(
            [
                "❌ NO PUBLIC ACCESS - Cannot insert into Google Docs",
                "Fix: Drive → Share → 'Anyone with the link' → 'Viewer'",
            ]
        )

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("update_drive_file", is_read_only=False, service_type="drive")
@require_google_service("drive", "drive_file")
async def update_drive_file(
    service,
    user_google_email: str,
    file_id: str,
    # File metadata updates
    name: Optional[str] = None,
    description: Optional[str] = None,
    mime_type: Optional[str] = None,
    # Folder organization
    add_parents: Optional[str] = None,  # Comma-separated folder IDs to add
    remove_parents: Optional[str] = None,  # Comma-separated folder IDs to remove
    # File status
    starred: Optional[bool] = None,
    trashed: Optional[bool] = None,
    # Sharing and permissions
    writers_can_share: Optional[bool] = None,
    copy_requires_writer_permission: Optional[bool] = None,
    # Custom properties
    properties: Optional[dict] = None,  # User-visible custom properties
) -> str:
    """Update a Drive file's metadata, folder parents, and flags.

    Side effects: mutates the file (rename, move via add/remove_parents,
    trash/untrash, star). Does NOT upload new content — for content use
    a native-app tool or create_drive_file. trashed=True is reversible
    with trashed=False until the file is permanently deleted. Requires
    the drive.file OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file ID to update.
        name: New display name.
        description: New description text.
        mime_type: New MIME type (changing this rarely works without
            also uploading matching content).
        add_parents: Comma-separated folder IDs to add the file into
            (effectively moves when combined with remove_parents).
        remove_parents: Comma-separated folder IDs to detach from.
        starred: True to star, False to unstar.
        trashed: True moves to Trash (soft-delete), False restores.
        writers_can_share: Whether editors may re-share the file.
        copy_requires_writer_permission: When True, copies require
            writer access (reader copy/export blocked).
        properties: Dict of user-visible custom key-value pairs attached
            to the file.

    Returns:
        Confirmation listing which fields were updated.
    """
    logger.info(f"[update_drive_file] Updating file {file_id} for {user_google_email}")

    current_file_fields = (
        "name, description, mimeType, parents, starred, trashed, webViewLink, "
        "writersCanShare, copyRequiresWriterPermission, properties"
    )
    resolved_file_id, current_file = await resolve_drive_item(
        service,
        file_id,
        extra_fields=current_file_fields,
    )
    file_id = resolved_file_id

    # Build the update body with only specified fields
    update_body = {}
    if name is not None:
        update_body["name"] = name
    if description is not None:
        update_body["description"] = description
    if mime_type is not None:
        update_body["mimeType"] = mime_type
    if starred is not None:
        update_body["starred"] = starred
    if trashed is not None:
        update_body["trashed"] = trashed
    if writers_can_share is not None:
        update_body["writersCanShare"] = writers_can_share
    if copy_requires_writer_permission is not None:
        update_body["copyRequiresWriterPermission"] = copy_requires_writer_permission
    if properties is not None:
        update_body["properties"] = properties

    async def _resolve_parent_arguments(parent_arg: Optional[str]) -> Optional[str]:
        if not parent_arg:
            return None
        parent_ids = [part.strip() for part in parent_arg.split(",") if part.strip()]
        if not parent_ids:
            return None

        resolved_ids = []
        for parent in parent_ids:
            resolved_parent = await resolve_folder_id(service, parent)
            resolved_ids.append(resolved_parent)
        return ",".join(resolved_ids)

    resolved_add_parents = await _resolve_parent_arguments(add_parents)
    resolved_remove_parents = await _resolve_parent_arguments(remove_parents)

    # Build query parameters for parent changes
    query_params = {
        "fileId": file_id,
        "supportsAllDrives": True,
        "fields": "id, name, description, mimeType, parents, starred, trashed, webViewLink, writersCanShare, copyRequiresWriterPermission, properties",
    }

    if resolved_add_parents:
        query_params["addParents"] = resolved_add_parents
    if resolved_remove_parents:
        query_params["removeParents"] = resolved_remove_parents

    # Only include body if there are updates
    if update_body:
        query_params["body"] = update_body

    # Perform the update
    updated_file = await asyncio.to_thread(
        service.files().update(**query_params).execute
    )

    # Build response message
    output_parts = [
        f"✅ Successfully updated file: {updated_file.get('name', current_file['name'])}"
    ]
    output_parts.append(f"   File ID: {file_id}")

    # Report what changed
    changes = []
    if name is not None and name != current_file.get("name"):
        changes.append(f"   • Name: '{current_file.get('name')}' → '{name}'")
    if description is not None:
        old_desc_value = current_file.get("description")
        new_desc_value = description
        should_report_change = (old_desc_value or "") != (new_desc_value or "")
        if should_report_change:
            old_desc_display = (
                old_desc_value if old_desc_value not in (None, "") else "(empty)"
            )
            new_desc_display = (
                new_desc_value if new_desc_value not in (None, "") else "(empty)"
            )
            changes.append(f"   • Description: {old_desc_display} → {new_desc_display}")
    if add_parents:
        changes.append(f"   • Added to folder(s): {add_parents}")
    if remove_parents:
        changes.append(f"   • Removed from folder(s): {remove_parents}")
    current_starred = current_file.get("starred")
    if starred is not None and starred != current_starred:
        star_status = "starred" if starred else "unstarred"
        changes.append(f"   • File {star_status}")
    current_trashed = current_file.get("trashed")
    if trashed is not None and trashed != current_trashed:
        trash_status = "moved to trash" if trashed else "restored from trash"
        changes.append(f"   • File {trash_status}")
    current_writers_can_share = current_file.get("writersCanShare")
    if writers_can_share is not None and writers_can_share != current_writers_can_share:
        share_status = "can" if writers_can_share else "cannot"
        changes.append(f"   • Writers {share_status} share the file")
    current_copy_requires_writer_permission = current_file.get(
        "copyRequiresWriterPermission"
    )
    if (
        copy_requires_writer_permission is not None
        and copy_requires_writer_permission != current_copy_requires_writer_permission
    ):
        copy_status = (
            "requires" if copy_requires_writer_permission else "doesn't require"
        )
        changes.append(f"   • Copying {copy_status} writer permission")
    if properties:
        changes.append(f"   • Updated custom properties: {properties}")

    if changes:
        output_parts.append("")
        output_parts.append("Changes applied:")
        output_parts.extend(changes)
    else:
        output_parts.append("   (No changes were made)")

    output_parts.append("")
    output_parts.append(f"View file: {updated_file.get('webViewLink', '#')}")

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("get_drive_shareable_link", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_shareable_link(
    service,
    user_google_email: str,
    file_id: str,
) -> str:
    """Fetch the webViewLink and current permissions for a Drive item.

    Read-only — does NOT change sharing. To modify sharing use
    manage_drive_access. For a fuller permissions audit use
    get_drive_file_permissions. Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file or folder ID.

    Returns:
        Block with file name/ID/type, shared flag, View URL, optional
        Download URL, and each permission entry (type, role, email or
        domain, expiration).
    """
    logger.info(
        f"[get_drive_shareable_link] Invoked. Email: '{user_google_email}', File ID: '{file_id}'"
    )

    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    file_metadata = await asyncio.to_thread(
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink, webContentLink, shared, "
            "permissions(id, type, role, emailAddress, domain, expirationTime)",
            supportsAllDrives=True,
        )
        .execute
    )

    output_parts = [
        f"File: {file_metadata.get('name', 'Unknown')}",
        f"ID: {file_id}",
        f"Type: {file_metadata.get('mimeType', 'Unknown')}",
        f"Shared: {file_metadata.get('shared', False)}",
        "",
        "Links:",
        f"  View: {file_metadata.get('webViewLink', 'N/A')}",
    ]

    web_content_link = file_metadata.get("webContentLink")
    if web_content_link:
        output_parts.append(f"  Download: {web_content_link}")

    permissions = file_metadata.get("permissions", [])
    if permissions:
        output_parts.append("")
        output_parts.append("Current permissions:")
        for perm in permissions:
            output_parts.append(f"  - {format_permission_info(perm)}")

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("manage_drive_access", is_read_only=False, service_type="drive")
@require_google_service("drive", "drive_file")
async def manage_drive_access(
    service,
    user_google_email: str,
    file_id: str,
    action: str,
    share_with: Optional[str] = None,
    role: Optional[str] = None,
    share_type: str = "user",
    permission_id: Optional[str] = None,
    recipients: Optional[List[Dict[str, Any]]] = None,
    send_notification: bool = True,
    email_message: Optional[str] = None,
    expiration_time: Optional[str] = None,
    allow_file_discovery: Optional[bool] = None,
    new_owner_email: Optional[str] = None,
    move_to_new_owners_root: bool = False,
) -> str:
    """Grant, batch-grant, update, revoke, or transfer ownership on a Drive item.

    Side effects: all actions mutate permissions; transfer_owner
    permanently changes the file's owner. Notification emails are sent
    per send_notification. For read-only inspection use
    get_drive_file_permissions. Requires the drive.file OAuth scope (or
    higher for cross-domain transfers).

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file or folder ID.
        action: "grant", "grant_batch", "update", "revoke", or
            "transfer_owner".
        share_with: For "grant" — recipient email (user/group) or domain
            name (domain). Omit for share_type="anyone".
        role: For "grant" (default "reader") and "update": "reader",
            "commenter", or "writer".
        share_type: For "grant": "user", "group", "domain", or "anyone".
            Default "user".
        permission_id: Required for "update" and "revoke". Get it from
            get_drive_file_permissions.
        recipients: For "grant_batch": list of objects with keys email
            (or domain for domain shares), role, share_type,
            expiration_time.
        send_notification: Send the recipient an email. Default True.
            Applies to grant/grant_batch user/group shares.
        email_message: Custom body appended to the notification email.
        expiration_time: RFC3339 deadline ("2026-06-01T00:00:00Z") after
            which the permission auto-revokes. Applies to grant/update.
        allow_file_discovery: For domain/anyone shares, True = indexable
            in search, False = link-only.
        new_owner_email: Required for "transfer_owner". Must be inside
            the same Workspace domain in most cases.
        move_to_new_owners_root: After transfer, move the file to the
            new owner's My Drive root. Default False.

    Returns:
        Confirmation with the applied permission details, or (for
        grant_batch) a per-recipient success/failure summary.
    """
    valid_actions = ("grant", "grant_batch", "update", "revoke", "transfer_owner")
    if action not in valid_actions:
        raise ValueError(
            f"Invalid action '{action}'. Must be one of: {', '.join(valid_actions)}"
        )

    logger.info(
        f"[manage_drive_access] Invoked. Email: '{user_google_email}', "
        f"File ID: '{file_id}', Action: '{action}'"
    )

    # --- grant: share with a single recipient ---
    if action == "grant":
        effective_role = role or "reader"
        validate_share_role(effective_role)
        validate_share_type(share_type)

        if share_type in ("user", "group") and not share_with:
            raise ValueError(f"share_with is required for share_type '{share_type}'")
        if share_type == "domain" and not share_with:
            raise ValueError(
                "share_with (domain name) is required for share_type 'domain'"
            )

        resolved_file_id, file_metadata = await resolve_drive_item(
            service, file_id, extra_fields="name, webViewLink"
        )
        file_id = resolved_file_id

        permission_body: Dict[str, Any] = {
            "type": share_type,
            "role": effective_role,
        }
        if share_type in ("user", "group"):
            permission_body["emailAddress"] = share_with
        elif share_type == "domain":
            permission_body["domain"] = share_with

        if expiration_time:
            validate_expiration_time(expiration_time)
            permission_body["expirationTime"] = expiration_time

        if share_type in ("domain", "anyone") and allow_file_discovery is not None:
            permission_body["allowFileDiscovery"] = allow_file_discovery

        create_params: Dict[str, Any] = {
            "fileId": file_id,
            "body": permission_body,
            "supportsAllDrives": True,
            "fields": "id, type, role, emailAddress, domain, expirationTime",
        }
        if share_type in ("user", "group"):
            create_params["sendNotificationEmail"] = send_notification
            if email_message:
                create_params["emailMessage"] = email_message

        created_permission = await asyncio.to_thread(
            service.permissions().create(**create_params).execute
        )

        return "\n".join(
            [
                f"Successfully shared '{file_metadata.get('name', 'Unknown')}'",
                "",
                "Permission created:",
                f"  - {format_permission_info(created_permission)}",
                "",
                f"View link: {file_metadata.get('webViewLink', 'N/A')}",
            ]
        )

    # --- grant_batch: share with multiple recipients ---
    if action == "grant_batch":
        if not recipients:
            raise ValueError("recipients list is required for 'grant_batch' action")

        resolved_file_id, file_metadata = await resolve_drive_item(
            service, file_id, extra_fields="name, webViewLink"
        )
        file_id = resolved_file_id

        results: List[str] = []
        success_count = 0
        failure_count = 0

        for recipient in recipients:
            r_share_type = recipient.get("share_type", "user")

            if r_share_type == "domain":
                domain = recipient.get("domain")
                if not domain:
                    results.append("  - Skipped: missing domain for domain share")
                    failure_count += 1
                    continue
                identifier = domain
            else:
                r_email = recipient.get("email")
                if not r_email:
                    results.append("  - Skipped: missing email address")
                    failure_count += 1
                    continue
                identifier = r_email

            r_role = recipient.get("role", "reader")
            try:
                validate_share_role(r_role)
            except ValueError as e:
                results.append(f"  - {identifier}: Failed - {e}")
                failure_count += 1
                continue

            try:
                validate_share_type(r_share_type)
            except ValueError as e:
                results.append(f"  - {identifier}: Failed - {e}")
                failure_count += 1
                continue

            r_perm_body: Dict[str, Any] = {
                "type": r_share_type,
                "role": r_role,
            }
            if r_share_type == "domain":
                r_perm_body["domain"] = identifier
            else:
                r_perm_body["emailAddress"] = identifier

            if recipient.get("expiration_time"):
                try:
                    validate_expiration_time(recipient["expiration_time"])
                    r_perm_body["expirationTime"] = recipient["expiration_time"]
                except ValueError as e:
                    results.append(f"  - {identifier}: Failed - {e}")
                    failure_count += 1
                    continue

            r_create_params: Dict[str, Any] = {
                "fileId": file_id,
                "body": r_perm_body,
                "supportsAllDrives": True,
                "fields": "id, type, role, emailAddress, domain, expirationTime",
            }
            if r_share_type in ("user", "group"):
                r_create_params["sendNotificationEmail"] = send_notification
                if email_message:
                    r_create_params["emailMessage"] = email_message

            try:
                created_perm = await asyncio.to_thread(
                    service.permissions().create(**r_create_params).execute
                )
                results.append(f"  - {format_permission_info(created_perm)}")
                success_count += 1
            except HttpError as e:
                results.append(f"  - {identifier}: Failed - {str(e)}")
                failure_count += 1

        output_parts = [
            f"Batch share results for '{file_metadata.get('name', 'Unknown')}'",
            "",
            f"Summary: {success_count} succeeded, {failure_count} failed",
            "",
            "Results:",
        ]
        output_parts.extend(results)
        output_parts.extend(
            [
                "",
                f"View link: {file_metadata.get('webViewLink', 'N/A')}",
            ]
        )
        return "\n".join(output_parts)

    # --- update: modify an existing permission ---
    if action == "update":
        if not permission_id:
            raise ValueError("permission_id is required for 'update' action")
        if not role and not expiration_time:
            raise ValueError(
                "Must provide at least one of: role, expiration_time for 'update' action"
            )

        if role:
            validate_share_role(role)
        if expiration_time:
            validate_expiration_time(expiration_time)

        resolved_file_id, file_metadata = await resolve_drive_item(
            service, file_id, extra_fields="name"
        )
        file_id = resolved_file_id

        effective_role = role
        if not effective_role:
            current_permission = await asyncio.to_thread(
                service.permissions()
                .get(
                    fileId=file_id,
                    permissionId=permission_id,
                    supportsAllDrives=True,
                    fields="role",
                )
                .execute
            )
            effective_role = current_permission.get("role")

        update_body: Dict[str, Any] = {"role": effective_role}
        if expiration_time:
            update_body["expirationTime"] = expiration_time

        updated_permission = await asyncio.to_thread(
            service.permissions()
            .update(
                fileId=file_id,
                permissionId=permission_id,
                body=update_body,
                supportsAllDrives=True,
                fields="id, type, role, emailAddress, domain, expirationTime",
            )
            .execute
        )

        return "\n".join(
            [
                f"Successfully updated permission on '{file_metadata.get('name', 'Unknown')}'",
                "",
                "Updated permission:",
                f"  - {format_permission_info(updated_permission)}",
            ]
        )

    # --- revoke: remove an existing permission ---
    if action == "revoke":
        if not permission_id:
            raise ValueError("permission_id is required for 'revoke' action")

        resolved_file_id, file_metadata = await resolve_drive_item(
            service, file_id, extra_fields="name"
        )
        file_id = resolved_file_id

        await asyncio.to_thread(
            service.permissions()
            .delete(
                fileId=file_id,
                permissionId=permission_id,
                supportsAllDrives=True,
            )
            .execute
        )

        return "\n".join(
            [
                f"Successfully removed permission from '{file_metadata.get('name', 'Unknown')}'",
                "",
                f"Permission ID '{permission_id}' has been revoked.",
            ]
        )

    # --- transfer_owner: transfer file ownership ---
    # action == "transfer_owner"
    if not new_owner_email:
        raise ValueError("new_owner_email is required for 'transfer_owner' action")

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name, owners"
    )
    file_id = resolved_file_id

    current_owners = file_metadata.get("owners", [])
    current_owner_emails = [o.get("emailAddress", "") for o in current_owners]

    transfer_body: Dict[str, Any] = {
        "type": "user",
        "role": "owner",
        "emailAddress": new_owner_email,
    }

    await asyncio.to_thread(
        service.permissions()
        .create(
            fileId=file_id,
            body=transfer_body,
            transferOwnership=True,
            moveToNewOwnersRoot=move_to_new_owners_root,
            supportsAllDrives=True,
            fields="id, type, role, emailAddress",
        )
        .execute
    )

    output_parts = [
        f"Successfully transferred ownership of '{file_metadata.get('name', 'Unknown')}'",
        "",
        f"New owner: {new_owner_email}",
        f"Previous owner(s): {', '.join(current_owner_emails) or 'Unknown'}",
    ]
    if move_to_new_owners_root:
        output_parts.append(f"File moved to {new_owner_email}'s My Drive root.")
    output_parts.extend(["", "Note: Previous owner now has editor access."])

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("copy_drive_file", is_read_only=False, service_type="drive")
@require_google_service("drive", "drive_file")
async def copy_drive_file(
    service,
    user_google_email: str,
    file_id: str,
    new_name: Optional[str] = None,
    parent_folder_id: str = "root",
) -> str:
    """Duplicate a Drive file (including Google Docs/Sheets/Slides).

    Side effects: creates a new owned-by-caller file; formatting and
    content are preserved. For folders use copy_drive_folder (deep copy).
    For Google Docs specifically this is the standard "duplicate from
    template" pattern — copy, then edit via batch_update_doc.  Requires
    the drive.file OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file ID of the source.
        new_name: Name for the copy. Defaults to "Copy of <original>".
        parent_folder_id: Target folder ID. Default "root" (My Drive).
            Shared-drive folder IDs work.

    Returns:
        Confirmation with original ID, new file ID, new name, MIME type,
        parent folder, and webViewLink of the copy.
    """
    logger.info(
        f"[copy_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}', New name: '{new_name}', Parent folder: '{parent_folder_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name, webViewLink, mimeType"
    )
    file_id = resolved_file_id
    original_name = file_metadata.get("name", "Unknown File")

    resolved_folder_id = await resolve_folder_id(service, parent_folder_id)

    copy_body = {}
    if new_name:
        copy_body["name"] = new_name
    else:
        copy_body["name"] = f"Copy of {original_name}"

    if resolved_folder_id != "root":
        copy_body["parents"] = [resolved_folder_id]

    copied_file = await asyncio.to_thread(
        service.files()
        .copy(
            fileId=file_id,
            body=copy_body,
            supportsAllDrives=True,
            fields="id, name, webViewLink, mimeType, parents",
        )
        .execute
    )

    output_parts = [
        f"Successfully copied '{original_name}'",
        "",
        f"Original file ID: {file_id}",
        f"New file ID: {copied_file.get('id', 'N/A')}",
        f"New file name: {copied_file.get('name', 'Unknown')}",
        f"File type: {copied_file.get('mimeType', 'Unknown')}",
        f"Location: {parent_folder_id}",
        "",
        f"View copied file: {copied_file.get('webViewLink', 'N/A')}",
    ]

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors(
    "set_drive_file_permissions", is_read_only=False, service_type="drive"
)
@require_google_service("drive", "drive_file")
async def set_drive_file_permissions(
    service,
    user_google_email: str,
    file_id: str,
    link_sharing: Optional[str] = None,
    writers_can_share: Optional[bool] = None,
    copy_requires_writer_permission: Optional[bool] = None,
) -> str:
    """Toggle link-sharing and common file-level sharing controls.

    Side effects: mutates sharing policy. Use this for high-level toggles
    ("anyone with the link", editor share rights, viewer copy-prevention).
    For per-user/group permission changes use manage_drive_access. At
    least one of the three flags must be set. Requires the drive.file
    OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        file_id: Drive file or folder ID.
        link_sharing: "off" removes anyone-with-link access; "reader",
            "commenter", or "writer" sets the link role. Omit to leave
            link sharing unchanged.
        writers_can_share: True lets editors re-share; False restricts
            sharing to owner only.
        copy_requires_writer_permission: True blocks viewers/commenters
            from copy/print/download; False allows.

    Returns:
        Summary of each permission change applied, grouped by file-level
        vs link-sharing settings.
    """
    logger.info(
        f"[set_drive_file_permissions] Invoked. Email: '{user_google_email}', "
        f"File ID: '{file_id}', Link sharing: '{link_sharing}', "
        f"Writers can share: {writers_can_share}, Copy restriction: {copy_requires_writer_permission}"
    )

    if (
        link_sharing is None
        and writers_can_share is None
        and copy_requires_writer_permission is None
    ):
        raise ValueError(
            "Must provide at least one of: link_sharing, writers_can_share, copy_requires_writer_permission"
        )

    valid_link_sharing = {"off", "reader", "commenter", "writer"}
    if link_sharing is not None and link_sharing not in valid_link_sharing:
        raise ValueError(
            f"Invalid link_sharing '{link_sharing}'. Must be one of: {', '.join(sorted(valid_link_sharing))}"
        )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name, webViewLink"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown")

    output_parts = [f"Permission settings updated for '{file_name}'", ""]
    changes_made = []

    # Handle file-level settings via files().update()
    file_update_body = {}
    if writers_can_share is not None:
        file_update_body["writersCanShare"] = writers_can_share
    if copy_requires_writer_permission is not None:
        file_update_body["copyRequiresWriterPermission"] = (
            copy_requires_writer_permission
        )

    if file_update_body:
        await asyncio.to_thread(
            service.files()
            .update(
                fileId=file_id,
                body=file_update_body,
                supportsAllDrives=True,
                fields="id",
            )
            .execute
        )
        if writers_can_share is not None:
            state = "allowed" if writers_can_share else "restricted to owner"
            changes_made.append(f"  - Editors sharing: {state}")
        if copy_requires_writer_permission is not None:
            state = "restricted" if copy_requires_writer_permission else "allowed"
            changes_made.append(f"  - Viewers copy/print/download: {state}")

    # Handle link sharing via permissions API
    if link_sharing is not None:
        current_permissions = await asyncio.to_thread(
            service.permissions()
            .list(
                fileId=file_id,
                supportsAllDrives=True,
                fields="permissions(id, type, role)",
            )
            .execute
        )
        anyone_perms = [
            p
            for p in current_permissions.get("permissions", [])
            if p.get("type") == "anyone"
        ]

        if link_sharing == "off":
            if anyone_perms:
                for perm in anyone_perms:
                    await asyncio.to_thread(
                        service.permissions()
                        .delete(
                            fileId=file_id,
                            permissionId=perm["id"],
                            supportsAllDrives=True,
                        )
                        .execute
                    )
                changes_made.append(
                    "  - Link sharing: disabled (restricted to specific people)"
                )
            else:
                changes_made.append("  - Link sharing: already off (no change)")
        else:
            if anyone_perms:
                await asyncio.to_thread(
                    service.permissions()
                    .update(
                        fileId=file_id,
                        permissionId=anyone_perms[0]["id"],
                        body={
                            "role": link_sharing,
                            "allowFileDiscovery": False,
                        },
                        supportsAllDrives=True,
                        fields="id, type, role",
                    )
                    .execute
                )
                changes_made.append(f"  - Link sharing: updated to '{link_sharing}'")
            else:
                await asyncio.to_thread(
                    service.permissions()
                    .create(
                        fileId=file_id,
                        body={
                            "type": "anyone",
                            "role": link_sharing,
                            "allowFileDiscovery": False,
                        },
                        supportsAllDrives=True,
                        fields="id, type, role",
                    )
                    .execute
                )
                changes_made.append(f"  - Link sharing: enabled as '{link_sharing}'")

    output_parts.append("Changes:")
    if changes_made:
        output_parts.extend(changes_made)
    else:
        output_parts.append("  - No changes (already configured)")
    output_parts.extend(["", f"View link: {file_metadata.get('webViewLink', 'N/A')}"])

    return "\n".join(output_parts)


async def _list_folder_children(service, folder_id: str) -> List[Dict[str, Any]]:
    """
    List all non-trashed children of a Drive folder, paginating as needed.
    Returns a list of file resources with id, name, mimeType.
    """
    children: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        resp = await asyncio.to_thread(
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageSize=200,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute
        )
        children.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return children


async def _copy_folder_tree(
    service,
    source_folder_id: str,
    destination_parent_id: str,
    new_folder_name: str,
    stats: Dict[str, int],
    errors: List[str],
) -> str:
    """
    Recursively copy a folder tree. Returns the new folder ID.
    Updates stats['folders'] and stats['files'] counters.
    Errors (per-file) are collected into `errors` without aborting the whole op.
    """
    create_body = {
        "name": new_folder_name,
        "mimeType": FOLDER_MIME_TYPE,
        "parents": [destination_parent_id],
    }
    new_folder = await asyncio.to_thread(
        service.files()
        .create(body=create_body, fields="id, name", supportsAllDrives=True)
        .execute
    )
    new_folder_id = new_folder["id"]
    stats["folders"] += 1

    children = await _list_folder_children(service, source_folder_id)
    for child in children:
        child_name = child.get("name", "unknown")
        child_id = child["id"]
        try:
            if child.get("mimeType") == FOLDER_MIME_TYPE:
                await _copy_folder_tree(
                    service, child_id, new_folder_id, child_name, stats, errors
                )
            else:
                await asyncio.to_thread(
                    service.files()
                    .copy(
                        fileId=child_id,
                        body={"name": child_name, "parents": [new_folder_id]},
                        supportsAllDrives=True,
                        fields="id, name",
                    )
                    .execute
                )
                stats["files"] += 1
        except HttpError as exc:
            errors.append(f"{child_name} ({child_id}): {exc}")
    return new_folder_id


@server.tool()
@handle_http_errors("copy_drive_folder", service_type="drive")
@require_google_service("drive", "drive")
async def copy_drive_folder(
    service,
    user_google_email: str,
    source_folder_id: str,
    destination_parent_id: str = "root",
    new_folder_name: Optional[str] = None,
) -> str:
    """
    Recursively copy a Drive folder (and all its contents) to a new location.

    Walks the source folder tree, creating the same structure under the destination
    and copying every file. Sequential to avoid rate-limit errors.

    Args:
        source_folder_id: ID of the folder to copy.
        destination_parent_id: Parent folder ID where the new copy goes. "root" by default.
        new_folder_name: Optional name for the top-level copied folder. Defaults to
            "Copy of [original name]".

    Returns:
        Summary with new folder ID, counts of folders/files copied, and any errors.
    """
    logger.info(
        f"[copy_drive_folder] src='{source_folder_id}' dest='{destination_parent_id}' name='{new_folder_name}'"
    )

    # Resolve source folder
    source_meta = await asyncio.to_thread(
        service.files()
        .get(fileId=source_folder_id, fields="id, name, mimeType", supportsAllDrives=True)
        .execute
    )
    if source_meta.get("mimeType") != FOLDER_MIME_TYPE:
        return (
            f"Error: source_folder_id '{source_folder_id}' is not a folder "
            f"(got mimeType '{source_meta.get('mimeType')}'). Use copy_drive_file instead."
        )
    source_name = source_meta.get("name", "Unknown")
    top_level_name = new_folder_name or f"Copy of {source_name}"

    resolved_dest = await resolve_folder_id(service, destination_parent_id)

    stats = {"folders": 0, "files": 0}
    errors: List[str] = []
    new_root_id = await _copy_folder_tree(
        service, source_folder_id, resolved_dest, top_level_name, stats, errors
    )

    link = f"https://drive.google.com/drive/folders/{new_root_id}"
    lines = [
        f"Recursively copied folder '{source_name}' -> '{top_level_name}' for {user_google_email}.",
        f"  Source: {source_folder_id}",
        f"  New folder ID: {new_root_id}",
        f"  Folders created: {stats['folders']}",
        f"  Files copied: {stats['files']}",
        f"  Errors: {len(errors)}",
        f"  Link: {link}",
    ]
    if errors:
        lines.append("")
        lines.append("Errors:")
        for err in errors[:20]:
            lines.append(f"  - {err}")
        if len(errors) > 20:
            lines.append(f"  ...and {len(errors) - 20} more.")
    return "\n".join(lines)


@server.tool()
@handle_http_errors("get_drive_revisions", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_revisions(
    service,
    user_google_email: str,
    file_id: str,
    page_size: int = 25,
) -> str:
    """
    List the revision history for a Drive file, newest first.

    Returns each revision's ID, modification timestamp, last-modifying user
    (display name + email), size in bytes (when available), MIME type, and
    whether it's pinned via `keepForever`. Use this to discover revision IDs
    before calling `restore_drive_revision`, or to audit who changed what.

    Requires OAuth scope: `https://www.googleapis.com/auth/drive.readonly`
    (or broader). Read-only.

    **Limitation**: Google-native files (Docs, Sheets, Slides) expose revisions
    in the API list but their binary content is not retrievable — only
    non-native files (PDF, DOCX, images, etc.) support content restore. By
    default, Drive retains up to 100 revisions or 30 days, whichever comes
    first, unless a revision is pinned (`keepForever: true`).

    Args:
        file_id: Drive file ID (from a file URL like
            `drive.google.com/file/d/<file_id>/view`, or from `search_drive_files`,
            or from `get_drive_file_metadata`).
        page_size: Maximum number of revisions to return. Clamped to `[1, 1000]`.
            Default `25`. No pagination token support in this tool — if the
            file has more than `page_size` revisions, only the most recent are
            returned.

    Returns:
        Multi-line string. First line: "Revisions for file <id> (<count>):".
        Each subsequent line: "  - <revId> | <modifiedTime ISO8601> | <user or
        email> | size: <bytes or n/a>[ [keepForever]]". If the file has no
        revisions or revisions aren't supported: "No revisions found for file
        <id> (or file does not support revisions)."
    """
    logger.info(f"[get_drive_revisions] file='{file_id}' page_size={page_size}")

    result = await asyncio.to_thread(
        service.revisions()
        .list(
            fileId=file_id,
            pageSize=max(1, min(page_size, 1000)),
            fields="revisions(id, modifiedTime, lastModifyingUser(displayName, emailAddress), size, mimeType, keepForever)",
        )
        .execute
    )
    revisions = result.get("revisions", [])
    if not revisions:
        return f"No revisions found for file {file_id} (or file does not support revisions)."

    lines = [f"Revisions for file {file_id} ({len(revisions)}):"]
    for rev in revisions:
        user = rev.get("lastModifyingUser") or {}
        who = user.get("displayName") or user.get("emailAddress") or "unknown"
        size = rev.get("size", "n/a")
        keep = " [keepForever]" if rev.get("keepForever") else ""
        lines.append(
            f"  - {rev.get('id')} | {rev.get('modifiedTime')} | {who} | size: {size}{keep}"
        )
    return "\n".join(lines)


@server.tool()
@handle_http_errors("restore_drive_revision", service_type="drive")
@require_google_service("drive", "drive")
async def restore_drive_revision(
    service,
    user_google_email: str,
    file_id: str,
    revision_id: str,
) -> str:
    """
    Restore a Drive file's content to a previous revision.

    Downloads the raw bytes of the specified revision and re-uploads them as
    the file's current content. This creates a NEW revision identical to the
    old one (it does not rewind the revision history — older revisions remain
    accessible). Original file ID, name, and sharing permissions are preserved.

    Requires OAuth scope: `https://www.googleapis.com/auth/drive` (write).
    Large files (>100 MB) may take several seconds due to download+upload cycle.

    **Limitation**: Google-native files (Docs, Sheets, Slides — MIME type
    `application/vnd.google-apps.*`) do NOT expose raw revision content via
    the Drive API. Attempting to restore a native file returns an explanatory
    error. For those, open the file in Google Docs/Sheets/Slides and use the
    built-in "Version history" UI (File > Version history > See version history).
    Binary-content files (PDFs, DOCX, XLSX, images, ZIP, etc.) are fully
    supported.

    Args:
        file_id: Drive file ID (from a file URL like
            `drive.google.com/file/d/<file_id>/view`, or from
            `search_drive_files`). File must be a non-Google-native type.
        revision_id: ID of the revision to restore TO. Get it from
            `get_drive_revisions` — the `id` field on each revision entry.
            The revision must still be retained (pinned with `keepForever` OR
            within Drive's normal retention window).

    Returns:
        On success: "Restored file '<name>' (<id>) to revision '<revisionId>'.
        New version: <N>, modified: <ISO8601>." where `<N>` is the new version
        number assigned by Drive after the restore.
        On Google-native file: "Cannot restore revision for Google-native file
        '<name>' (mime: <mimeType>). Use the Google Docs/Sheets/Slides Version
        History UI for native files."
    """
    logger.info(f"[restore_drive_revision] file='{file_id}' rev='{revision_id}'")

    # Fetch current file metadata for mime type
    file_meta = await asyncio.to_thread(
        service.files()
        .get(fileId=file_id, fields="id, name, mimeType", supportsAllDrives=True)
        .execute
    )
    mime_type = file_meta.get("mimeType", "application/octet-stream")
    name = file_meta.get("name", "Unknown")
    if mime_type.startswith("application/vnd.google-apps"):
        return (
            f"Cannot restore revision for Google-native file '{name}' (mime: {mime_type}). "
            f"Use the Google Docs/Sheets/Slides Version History UI for native files."
        )

    # Download the revision content
    request = service.revisions().get_media(fileId=file_id, revisionId=revision_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=DOWNLOAD_CHUNK_SIZE_BYTES)
    done = False
    while not done:
        _, done = await asyncio.to_thread(downloader.next_chunk)
    buffer.seek(0)

    # Upload as the new current version
    media = MediaIoBaseUpload(buffer, mimetype=mime_type, resumable=False)
    updated = await asyncio.to_thread(
        service.files()
        .update(
            fileId=file_id,
            media_body=media,
            fields="id, name, modifiedTime, version",
            supportsAllDrives=True,
        )
        .execute
    )
    return (
        f"Restored file '{name}' ({file_id}) to revision '{revision_id}'. "
        f"New version: {updated.get('version', 'n/a')}, modified: {updated.get('modifiedTime', 'n/a')}."
    )
