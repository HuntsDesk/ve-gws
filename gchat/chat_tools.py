"""
Google Chat MCP Tools

This module provides MCP tools for interacting with Google Chat API.
"""

import base64
import logging
import asyncio
import ssl
from typing import Dict, List, Optional

import httpx
from googleapiclient.errors import HttpError

# Auth & server utilities
from auth.service_decorator import require_google_service, require_multiple_services
from core.server import server
from core.utils import TransientNetworkError, handle_http_errors

logger = logging.getLogger(__name__)

# In-memory cache for user ID → display name (bounded to avoid unbounded growth)
_SENDER_CACHE_MAX_SIZE = 256
_sender_name_cache: Dict[str, str] = {}
_SEARCH_MESSAGES_MAX_CONCURRENT_SPACE_FETCHES = 1
_SEARCH_MESSAGES_SSL_RETRIES = 3
_SEARCH_MESSAGES_RETRY_BASE_DELAY_SECONDS = 1


def _cache_sender(user_id: str, name: str) -> None:
    """Store a resolved sender name, evicting oldest entries if cache is full."""
    if len(_sender_name_cache) >= _SENDER_CACHE_MAX_SIZE:
        to_remove = list(_sender_name_cache.keys())[: _SENDER_CACHE_MAX_SIZE // 2]
        for k in to_remove:
            del _sender_name_cache[k]
    _sender_name_cache[user_id] = name


async def _resolve_sender(people_service, sender_obj: dict) -> str:
    """Resolve a Chat message sender to a display name.

    Fast path: use displayName if the API already provided it.
    Slow path: look up the user via the People API directory and cache the result.
    """
    # Fast path — Chat API sometimes provides displayName directly
    display_name = sender_obj.get("displayName")
    if display_name:
        return display_name

    user_id = sender_obj.get("name", "")  # e.g. "users/123456789"
    if not user_id:
        return "Unknown Sender"

    # Check cache
    if user_id in _sender_name_cache:
        return _sender_name_cache[user_id]

    # Try People API directory lookup
    # Chat API uses "users/ID" but People API expects "people/ID"
    people_resource = user_id.replace("users/", "people/", 1)
    if people_service:
        try:
            person = await asyncio.to_thread(
                people_service.people()
                .get(resourceName=people_resource, personFields="names,emailAddresses")
                .execute
            )
            names = person.get("names", [])
            if names:
                resolved = names[0].get("displayName", user_id)
                _cache_sender(user_id, resolved)
                return resolved
            # Fall back to email if no name
            emails = person.get("emailAddresses", [])
            if emails:
                resolved = emails[0].get("value", user_id)
                _cache_sender(user_id, resolved)
                return resolved
        except HttpError as e:
            logger.debug(f"People API lookup failed for {user_id}: {e}")
        except Exception as e:
            logger.debug(f"Unexpected error resolving {user_id}: {e}")

    # Final fallback
    _cache_sender(user_id, user_id)
    return user_id


async def _execute_chat_request(
    request_factory,
    *,
    request_label: str,
    retries: int = 1,
    semaphore: Optional[asyncio.Semaphore] = None,
):
    """Execute a Chat API request in a worker thread with optional SSL retries."""
    for attempt in range(retries):
        try:
            if semaphore is None:
                return await asyncio.to_thread(lambda: request_factory().execute())
            async with semaphore:
                return await asyncio.to_thread(lambda: request_factory().execute())
        except ssl.SSLError as e:
            if attempt == retries - 1:
                raise
            delay = _SEARCH_MESSAGES_RETRY_BASE_DELAY_SECONDS * (2**attempt)
            logger.warning(
                "[search_messages] SSL error during %s on attempt %s/%s: %s. Retrying in %s seconds.",
                request_label,
                attempt + 1,
                retries,
                e,
                delay,
            )
            await asyncio.sleep(delay)


def _extract_rich_links(msg: dict) -> List[str]:
    """Extract URLs from RICH_LINK annotations (smart chips).

    When a user pastes a Google Workspace URL in Chat and it renders as a
    smart chip, the URL is NOT in the text field — it's only available in
    the annotations array as a RICH_LINK with richLinkMetadata.uri.
    """
    text = msg.get("text", "")
    urls = []
    for ann in msg.get("annotations", []):
        if ann.get("type") == "RICH_LINK":
            uri = ann.get("richLinkMetadata", {}).get("uri", "")
            if uri and uri not in text:
                urls.append(uri)
    return urls


@server.tool()
@require_google_service("chat", "chat_spaces_readonly")
@handle_http_errors("list_spaces", service_type="chat")
async def list_spaces(
    service,
    user_google_email: str,
    page_size: int = 100,
    space_type: str = "all",  # "all", "room", "dm"
) -> str:
    """
    List Google Chat spaces (rooms and direct messages) the authenticated
    user is a member of.

    Use this to discover the `space_id` (in `spaces/<id>` format) needed
    for `get_messages`, `send_message`, and related Chat tools. The user
    only sees spaces they have joined — this will not surface public
    spaces in the workspace they haven't joined.

    Requires OAuth scope:
    `https://www.googleapis.com/auth/chat.spaces.readonly` (read-only).

    Args:
        user_google_email: The user's Google email address. Required.
        page_size: Maximum number of spaces to return in one call. Defaults
            to 100; Google's hard cap is 1000. No pagination token is
            exposed by this tool — request a larger page_size if a user is
            in more than 100 spaces.
        space_type: Filter by space type. One of:
            - `"all"` (default): both rooms and direct messages
            - `"room"`: multi-member named spaces (`SPACE`)
            - `"dm"`: 1:1 or group direct messages (`DIRECT_MESSAGE`)
            Any other value is treated as `"all"`.

    Returns:
        Multi-line string with:
        - Header: `Found <N> Chat spaces (type: <filter>):`
        - One line per space: `- <displayName> (ID: spaces/<id>, Type: <SPACE|DIRECT_MESSAGE|UNKNOWN>)`
        DMs typically have no `displayName`; those render as `Unnamed Space`.
        When zero results: `No Chat spaces found for type '<filter>'.`
    """
    logger.info(f"[list_spaces] Email={user_google_email}, Type={space_type}")

    # Build filter based on space_type
    filter_param = None
    if space_type == "room":
        filter_param = "spaceType = SPACE"
    elif space_type == "dm":
        filter_param = "spaceType = DIRECT_MESSAGE"

    request_params = {"pageSize": page_size}
    if filter_param:
        request_params["filter"] = filter_param

    response = await asyncio.to_thread(service.spaces().list(**request_params).execute)

    spaces = response.get("spaces", [])
    if not spaces:
        return f"No Chat spaces found for type '{space_type}'."

    output = [f"Found {len(spaces)} Chat spaces (type: {space_type}):"]
    for space in spaces:
        space_name = space.get("displayName", "Unnamed Space")
        space_id = space.get("name", "")
        space_type_actual = space.get("spaceType", "UNKNOWN")
        output.append(f"- {space_name} (ID: {space_id}, Type: {space_type_actual})")

    return "\n".join(output)


@server.tool()
@require_multiple_services(
    [
        {"service_type": "chat", "scopes": "chat_read", "param_name": "chat_service"},
        {
            "service_type": "people",
            "scopes": "contacts_read",
            "param_name": "people_service",
        },
    ]
)
@handle_http_errors("get_messages", service_type="chat")
async def get_messages(
    chat_service,
    people_service,
    user_google_email: str,
    space_id: str,
    page_size: int = 50,
    order_by: str = "createTime desc",
    message_filter: Optional[str] = None,
) -> str:
    """List messages in a Google Chat space with sender names resolved.

    Use this to read a room/DM's recent messages. For text search across
    spaces use search_messages. For sending messages use send_message.
    For attachment downloads use download_chat_attachment. Senders are
    resolved to display names via the People API (both chat.read and
    contacts.readonly OAuth scopes required).

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        space_id: Space resource name from list_spaces, formatted as
            "spaces/<id>".
        page_size: Max messages returned. Default 50.
        order_by: "createTime desc" (default, newest first) or
            "createTime" (oldest first).
        message_filter: Chat API filter expression. Supports createTime
            and thread.name, e.g.
            'createTime > "2026-03-18T00:00:00Z"' or
            'thread.name = spaces/X/threads/Y'. Full-text search is NOT
            supported here — use search_messages.

    Returns:
        Formatted block per message with timestamp, resolved sender name,
        text, rich-link URLs, attachments (with download instructions),
        thread reference (when threaded), emoji reactions, and the
        Message ID for follow-up calls.
    """
    logger.info(f"[get_messages] Space ID: '{space_id}' for user '{user_google_email}'")

    # Get space info first
    space_info = await asyncio.to_thread(
        chat_service.spaces().get(name=space_id).execute
    )
    space_name = space_info.get("displayName", "Unknown Space")

    # Get messages
    list_params = {"parent": space_id, "pageSize": page_size, "orderBy": order_by}
    if message_filter is not None:
        list_params["filter"] = message_filter
    response = await asyncio.to_thread(
        chat_service.spaces().messages().list(**list_params).execute
    )

    messages = response.get("messages", [])
    if not messages:
        return f"No messages found in space '{space_name}' (ID: {space_id})."

    # Pre-resolve unique senders in parallel
    sender_lookup = {}
    for msg in messages:
        s = msg.get("sender", {})
        key = s.get("name", "")
        if key and key not in sender_lookup:
            sender_lookup[key] = s
    resolved_names = await asyncio.gather(
        *[_resolve_sender(people_service, s) for s in sender_lookup.values()]
    )
    sender_map = dict(zip(sender_lookup.keys(), resolved_names))

    output = [f"Messages from '{space_name}' (ID: {space_id}):\n"]
    for msg in messages:
        sender_obj = msg.get("sender", {})
        sender_key = sender_obj.get("name", "")
        sender = sender_map.get(sender_key) or await _resolve_sender(
            people_service, sender_obj
        )
        create_time = msg.get("createTime", "Unknown Time")
        text_content = msg.get("text", "No text content")
        msg_name = msg.get("name", "")

        output.append(f"[{create_time}] {sender}:")
        output.append(f"  {text_content}")
        rich_links = _extract_rich_links(msg)
        for url in rich_links:
            output.append(f"  [linked: {url}]")
        # Show attachments
        attachments = msg.get("attachment", [])
        for idx, att in enumerate(attachments):
            att_name = att.get("contentName", "unnamed")
            att_type = att.get("contentType", "unknown type")
            att_resource = att.get("name", "")
            output.append(f"  [attachment {idx}: {att_name} ({att_type})]")
            if att_resource:
                output.append(
                    f"  Use download_chat_attachment(message_id='{msg_name}', attachment_index={idx}) to download"
                )
        # Show thread info if this is a threaded reply
        thread = msg.get("thread", {})
        if msg.get("threadReply") and thread.get("name"):
            output.append(f"  [thread: {thread['name']}]")
        # Show emoji reactions
        reactions = msg.get("emojiReactionSummaries", [])
        if reactions:
            parts = []
            for r in reactions:
                emoji = r.get("emoji", {})
                symbol = emoji.get("unicode", "")
                if not symbol:
                    ce = emoji.get("customEmoji", {})
                    symbol = f":{ce.get('uid', '?')}:"
                count = r.get("reactionCount", 0)
                parts.append(f"{symbol}x{count}")
            output.append(f"  [reactions: {', '.join(parts)}]")
        output.append(f"  (Message ID: {msg_name})\n")

    return "\n".join(output)


@server.tool()
@require_google_service("chat", "chat_write")
@handle_http_errors("send_message", service_type="chat")
async def send_message(
    service,
    user_google_email: str,
    space_id: str,
    message_text: str,
    thread_key: Optional[str] = None,
    thread_name: Optional[str] = None,
) -> str:
    """Post a text message to a Google Chat space (optionally threaded).

    Side effects: creates a new visible message in the space. For adding
    an emoji reaction to an existing message use create_reaction. For
    listing what's in a space use get_messages. Requires the
    chat.messages.create (chat_write) OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        space_id: Target space resource name ("spaces/<id>") from
            list_spaces.
        message_text: Plain text body. Supports Chat markdown (e.g.
            *bold*, _italic_, `code`).
        thread_key: App-defined thread key — messages with the same key
            thread together. If no thread exists with this key, a new
            one is created. Mutually exclusive with thread_name.
        thread_name: Resource name of an existing thread
            ("spaces/X/threads/Y") to reply to. Falls back to a new
            thread if the specified one is not found.

    Returns:
        Confirmation with the new Message ID (resource name) and
        createTime.
    """
    logger.info(f"[send_message] Email: '{user_google_email}', Space: '{space_id}'")

    message_body = {"text": message_text}

    request_params = {"parent": space_id, "body": message_body}

    # Thread reply support
    if thread_name:
        message_body["thread"] = {"name": thread_name}
        request_params["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
    elif thread_key:
        message_body["thread"] = {"threadKey": thread_key}
        request_params["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"

    message = await asyncio.to_thread(
        service.spaces().messages().create(**request_params).execute
    )

    message_name = message.get("name", "")
    create_time = message.get("createTime", "")

    msg = f"Message sent to space '{space_id}' by {user_google_email}. Message ID: {message_name}, Time: {create_time}"
    logger.info(
        f"Successfully sent message to space '{space_id}' by {user_google_email}"
    )
    return msg


@server.tool()
@require_multiple_services(
    [
        {"service_type": "chat", "scopes": "chat_read", "param_name": "chat_service"},
        {
            "service_type": "people",
            "scopes": "contacts_read",
            "param_name": "people_service",
        },
    ]
)
@handle_http_errors("search_messages", is_read_only=True, service_type="chat")
async def search_messages(
    chat_service,
    people_service,
    user_google_email: str,
    query: Optional[str] = None,
    space_id: Optional[str] = None,
    page_size: int = 25,
    time_filter: Optional[str] = None,
    max_spaces: int = 10,
) -> str:
    """Search Chat messages across one or many spaces by text and/or time.

    The Chat API does not support server-side full-text search, so this
    tool fetches messages per space (with optional createTime filter
    applied server-side) and does a case-insensitive substring match on
    message text client-side. For a single space list without filtering
    use get_messages. Requires both chat.read and contacts.readonly
    OAuth scopes (senders are resolved to names via People API).

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        query: Case-insensitive substring to match in message text. Omit
            to return messages by time only.
        space_id: Restrict search to one space ("spaces/<id>"). Omit to
            search across accessible spaces (capped by max_spaces).
        page_size: Max messages fetched per space. Default 25.
        time_filter: Chat API createTime expression, e.g.
            'createTime > "2026-03-18T00:00:00Z"' or a range joined
            with AND. Applied server-side.
        max_spaces: When space_id is omitted, cap on how many spaces
            are scanned. Default 10.

    Returns:
        Formatted list with one line per match: timestamp, sender, space
        name, truncated text (100 chars + ellipsis), plus rich-link
        and attachment markers inline.
    """
    logger.info(
        f"[search_messages] Email={user_google_email}, Query='{query}', TimeFilter='{time_filter}'"
    )

    # Google Chat messages.list supports time/thread filters, but not full-text
    # search. Apply only supported API filters, then filter message text below.
    filter_parts = []
    if time_filter:
        filter_parts.append(time_filter)
    filter_str = " AND ".join(filter_parts) if filter_parts else None

    search_terms = []
    if query:
        search_terms.append(f'text "{query}"')
    if time_filter:
        search_terms.append(time_filter)
    search_desc = " and ".join(search_terms) if search_terms else "all messages"

    # If specific space provided, search within that space
    if space_id:
        list_params = {"parent": space_id, "pageSize": page_size}
        if filter_str:
            list_params["filter"] = filter_str
        response = await _execute_chat_request(
            lambda: chat_service.spaces().messages().list(**list_params),
            request_label=f"fetching messages for {space_id}",
            retries=_SEARCH_MESSAGES_SSL_RETRIES,
        )
        messages = response.get("messages", [])
        context = f"space '{space_id}'"
    else:
        # Search across all accessible spaces
        spaces_response = await _execute_chat_request(
            lambda: chat_service.spaces().list(pageSize=100),
            request_label="listing accessible spaces",
            retries=_SEARCH_MESSAGES_SSL_RETRIES,
        )
        spaces = spaces_response.get("spaces", [])
        spaces_to_search = spaces[:max_spaces]
        fetch_semaphore = asyncio.Semaphore(
            _SEARCH_MESSAGES_MAX_CONCURRENT_SPACE_FETCHES
        )

        async def fetch_space_messages(space: dict) -> tuple[List[dict], bool]:
            try:
                list_params = {"parent": space.get("name"), "pageSize": page_size}
                if filter_str:
                    list_params["filter"] = filter_str
                response = await _execute_chat_request(
                    lambda: chat_service.spaces().messages().list(**list_params),
                    request_label=f"fetching messages for {space.get('name')}",
                    retries=_SEARCH_MESSAGES_SSL_RETRIES,
                    semaphore=fetch_semaphore,
                )
                msgs = response.get("messages", [])
                display = space.get("displayName", "Unknown")
                for msg in msgs:
                    msg["_space_name"] = display
                return msgs, False
            except HttpError as e:
                logger.debug(
                    "Skipping space %s during search: %s", space.get("name"), e
                )
                return [], False
            except ssl.SSLError as e:
                logger.warning(
                    "Skipping space %s during search after repeated SSL failures: %s",
                    space.get("name"),
                    e,
                )
                return [], True

        results = await asyncio.gather(
            *(fetch_space_messages(space) for space in spaces_to_search)
        )
        transient_failures = 0
        messages = []
        for batch, had_transient_failure in results:
            messages.extend(batch)
            transient_failures += int(had_transient_failure)
        if spaces_to_search and transient_failures == len(spaces_to_search):
            raise TransientNetworkError(
                "A transient SSL error occurred in 'search_messages' while searching Chat spaces. "
                "Please try again shortly."
            )
        context = "all accessible spaces"

    # Client-side text filtering (text: operator is not supported by the API)
    if query:
        query_lower = query.lower()
        messages = [m for m in messages if query_lower in (m.get("text") or "").lower()]

    if not messages:
        suffix = (
            f" Skipped {transient_failures} spaces due to repeated SSL failures."
            if "transient_failures" in locals() and transient_failures
            else ""
        )
        return f"No messages found matching '{search_desc}' in {context}.{suffix}"

    # Resolve senders sequentially. The underlying googleapiclient/httplib2
    # service objects are not safe to fan out heavily and can trigger SSL churn.
    sender_lookup = {}
    for msg in messages:
        s = msg.get("sender", {})
        key = s.get("name", "")
        if key and key not in sender_lookup:
            sender_lookup[key] = s
    sender_map = {}
    for key, sender_obj in sender_lookup.items():
        sender_map[key] = await _resolve_sender(people_service, sender_obj)

    output = [f"Found {len(messages)} messages matching '{search_desc}' in {context}:"]
    for msg in messages:
        sender_obj = msg.get("sender", {})
        sender_key = sender_obj.get("name", "")
        sender = sender_map.get(sender_key) or await _resolve_sender(
            people_service, sender_obj
        )
        create_time = msg.get("createTime", "Unknown Time")
        text_content = msg.get("text", "No text content")
        space_name = msg.get("_space_name", "Unknown Space")

        # Truncate long messages
        if len(text_content) > 100:
            text_content = text_content[:100] + "..."

        rich_links = _extract_rich_links(msg)
        links_suffix = "".join(f" [linked: {url}]" for url in rich_links)
        attachments = msg.get("attachment", [])
        att_suffix = "".join(
            f" [attachment: {a.get('contentName', 'unnamed')} ({a.get('contentType', 'unknown type')})]"
            for a in attachments
        )
        output.append(
            f"- [{create_time}] {sender} in '{space_name}': {text_content}{links_suffix}{att_suffix}"
        )

    return "\n".join(output)


@server.tool()
@require_google_service("chat", "chat_write")
@handle_http_errors("create_reaction", service_type="chat")
async def create_reaction(
    service,
    user_google_email: str,
    message_id: str,
    emoji_unicode: str,
) -> str:
    """Add an emoji reaction to a Chat message.

    Side effects: creates a reaction visible to everyone in the space.
    Custom Workspace emoji are not supported here (Unicode only). For
    posting a new message use send_message. Requires the chat_write
    OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        message_id: Message resource name
            ("spaces/<space>/messages/<msg>") from get_messages or
            search_messages.
        emoji_unicode: Single Unicode emoji character, e.g. "\U0001F44D"
            (thumbs up) or a literal emoji like a smiley.

    Returns:
        Confirmation with the emoji, target message ID, and new
        reaction resource name.
    """
    logger.info(f"[create_reaction] Message: '{message_id}', Emoji: '{emoji_unicode}'")

    reaction = await asyncio.to_thread(
        service.spaces()
        .messages()
        .reactions()
        .create(
            parent=message_id,
            body={"emoji": {"unicode": emoji_unicode}},
        )
        .execute
    )

    reaction_name = reaction.get("name", "")
    return f"Reacted with {emoji_unicode} on message {message_id}. Reaction ID: {reaction_name}"


@server.tool()
@handle_http_errors("download_chat_attachment", is_read_only=True, service_type="chat")
@require_google_service("chat", "chat_read")
async def download_chat_attachment(
    service,
    user_google_email: str,
    message_id: str,
    attachment_index: int = 0,
) -> str:
    """Download a Chat message attachment to disk or expose via URL.

    Side effects: writes a file to the configured attachment storage
    (stdio mode) or publishes a 1-hour download URL (HTTP mode). In
    stateless mode, returns a base64 preview. Use get_messages to
    discover the message ID and per-message attachment indices.
    Requires the chat_read OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        message_id: Message resource name
            ("spaces/<space>/messages/<msg>") from get_messages.
        attachment_index: 0-based index into the message's attachments
            list. Default 0 (first attachment).

    Returns:
        Block with filename, content type, size in KB, and either a
        local file path (stdio) or a 1-hour download URL (HTTP).
        Validation errors surface inline (no attachments, index out of
        range).
    """
    logger.info(
        f"[download_chat_attachment] Message: '{message_id}', Index: {attachment_index}"
    )

    # Fetch the message to get attachment metadata
    msg = await asyncio.to_thread(
        service.spaces().messages().get(name=message_id).execute
    )

    attachments = msg.get("attachment", [])
    if not attachments:
        return f"No attachments found on message {message_id}."

    if attachment_index < 0 or attachment_index >= len(attachments):
        return (
            f"Invalid attachment_index {attachment_index}. "
            f"Message has {len(attachments)} attachment(s) (0-{len(attachments) - 1})."
        )

    att = attachments[attachment_index]
    filename = att.get("contentName", "attachment")
    content_type = att.get("contentType", "application/octet-stream")
    source = att.get("source", "")

    # The media endpoint needs attachmentDataRef.resourceName (e.g.
    # "spaces/S/attachments/A"), NOT the attachment name which includes
    # the /messages/ segment and causes 400 errors.
    media_resource = att.get("attachmentDataRef", {}).get("resourceName", "")
    att_name = att.get("name", "")

    logger.info(
        f"[download_chat_attachment] Downloading '{filename}' ({content_type}), "
        f"source={source}, mediaResource={media_resource}, name={att_name}"
    )

    # Download the attachment binary data via the Chat API media endpoint.
    # We use httpx with the Bearer token directly because MediaIoBaseDownload
    # and AuthorizedHttp fail in OAuth 2.1 (no refresh_token). The attachment's
    # downloadUri points to chat.google.com which requires browser cookies.
    if not media_resource and not att_name:
        return f"No resource name available for attachment '{filename}'."

    # Prefer attachmentDataRef.resourceName for the media endpoint
    resource_name = media_resource or att_name
    download_url = f"https://chat.googleapis.com/v1/media/{resource_name}?alt=media"

    try:
        access_token = service._http.credentials.token
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(
                download_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if resp.status_code != 200:
                body = resp.text[:500]
                return (
                    f"Failed to download attachment '{filename}': "
                    f"HTTP {resp.status_code} from {download_url}\n{body}"
                )
            file_bytes = resp.content
    except Exception as e:
        return f"Failed to download attachment '{filename}': {e}"

    size_bytes = len(file_bytes)
    size_kb = size_bytes / 1024

    # Check if we're in stateless mode (can't save files)
    from auth.oauth_config import is_stateless_mode

    if is_stateless_mode():
        b64_preview = base64.urlsafe_b64encode(file_bytes).decode("utf-8")[:100]
        return "\n".join(
            [
                f"Attachment downloaded: {filename} ({content_type})",
                f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
                "",
                "Stateless mode: File storage disabled.",
                f"Base64 preview: {b64_preview}...",
            ]
        )

    # Save to local disk
    from core.attachment_storage import get_attachment_storage, get_attachment_url
    from core.config import get_transport_mode

    storage = get_attachment_storage()
    b64_data = base64.urlsafe_b64encode(file_bytes).decode("utf-8")
    result = storage.save_attachment(
        base64_data=b64_data, filename=filename, mime_type=content_type
    )

    result_lines = [
        f"Attachment downloaded: {filename}",
        f"Type: {content_type}",
        f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
    ]

    if get_transport_mode() == "stdio":
        result_lines.append(f"\nSaved to: {result.path}")
        result_lines.append(
            "\nThe file has been saved to disk and can be accessed directly via the file path."
        )
    else:
        download_url = get_attachment_url(result.file_id)
        result_lines.append(f"\nDownload URL: {download_url}")
        result_lines.append("\nThe file will expire after 1 hour.")

    logger.info(
        f"[download_chat_attachment] Saved {size_kb:.1f} KB attachment to {result.path}"
    )
    return "\n".join(result_lines)
