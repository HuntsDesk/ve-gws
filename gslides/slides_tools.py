"""
Google Slides MCP Tools

This module provides MCP tools for interacting with Google Slides API.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional


from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.comments import create_comment_tools
from gslides.slides_helpers import (
    build_element_properties,
    build_solid_fill,
    build_text_range,
    collect_text_element_ids,
    extract_notes_text,
    find_notes_shape_id,
    new_object_id,
    parse_hex_color_to_rgb,
    slides_batch_update,
)

logger = logging.getLogger(__name__)


@server.tool()
@handle_http_errors("create_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def create_presentation(
    service, user_google_email: str, title: str = "Untitled Presentation"
) -> str:
    """
    Create a new Google Slides presentation.

    Args:
        user_google_email (str): The user's Google email address. Required.
        title (str): The title for the new presentation. Defaults to "Untitled Presentation".

    Returns:
        str: Details about the created presentation including ID and URL.
    """
    logger.info(
        f"[create_presentation] Invoked. Email: '{user_google_email}', Title: '{title}'"
    )

    body = {"title": title}

    result = await asyncio.to_thread(service.presentations().create(body=body).execute)

    presentation_id = result.get("presentationId")
    presentation_url = f"https://docs.google.com/presentation/d/{presentation_id}/edit"

    confirmation_message = f"""Presentation Created Successfully for {user_google_email}:
- Title: {title}
- Presentation ID: {presentation_id}
- URL: {presentation_url}
- Slides: {len(result.get("slides", []))} slide(s) created"""

    logger.info(f"Presentation created successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("get_presentation", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_presentation(
    service, user_google_email: str, presentation_id: str
) -> str:
    """
    Get details about a Google Slides presentation.

    Args:
        user_google_email (str): The user's Google email address. Required.
        presentation_id (str): The ID of the presentation to retrieve.

    Returns:
        str: Details about the presentation including title, slides count, and metadata.
    """
    logger.info(
        f"[get_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}'"
    )

    result = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )

    title = result.get("title", "Untitled")
    slides = result.get("slides", [])
    page_size = result.get("pageSize", {})

    slides_info = []
    for i, slide in enumerate(slides, 1):
        slide_id = slide.get("objectId", "Unknown")
        page_elements = slide.get("pageElements", [])

        # Collect text from the slide whose JSON structure is very complicated
        # https://googleapis.github.io/google-api-python-client/docs/dyn/slides_v1.presentations.html#get
        slide_text = ""
        try:
            texts_from_elements = []
            for page_element in slide.get("pageElements", []):
                shape = page_element.get("shape", None)
                if shape and shape.get("text", None):
                    text = shape.get("text", None)
                    if text:
                        text_elements_in_shape = []
                        for text_element in text.get("textElements", []):
                            text_run = text_element.get("textRun", None)
                            if text_run:
                                content = text_run.get("content", None)
                                if content:
                                    start_index = text_element.get("startIndex", 0)
                                    text_elements_in_shape.append(
                                        (start_index, content)
                                    )

                        if text_elements_in_shape:
                            # Sort text elements within a single shape
                            text_elements_in_shape.sort(key=lambda item: item[0])
                            full_text_from_shape = "".join(
                                [item[1] for item in text_elements_in_shape]
                            )
                            texts_from_elements.append(full_text_from_shape)

            # cleanup text we collected
            slide_text = "\n".join(texts_from_elements)
            slide_text_rows = slide_text.split("\n")
            slide_text_rows = [row for row in slide_text_rows if len(row.strip()) > 0]
            if slide_text_rows:
                slide_text_rows = ["    > " + row for row in slide_text_rows]
                slide_text = "\n" + "\n".join(slide_text_rows)
            else:
                slide_text = ""
        except Exception as e:
            logger.warning(f"Failed to extract text from the slide {slide_id}: {e}")
            slide_text = f"<failed to extract text: {type(e)}, {e}>"

        slides_info.append(
            f"  Slide {i}: ID {slide_id}, {len(page_elements)} element(s), text: {slide_text if slide_text else 'empty'}"
        )

    confirmation_message = f"""Presentation Details for {user_google_email}:
- Title: {title}
- Presentation ID: {presentation_id}
- URL: https://docs.google.com/presentation/d/{presentation_id}/edit
- Total Slides: {len(slides)}
- Page Size: {page_size.get("width", {}).get("magnitude", "Unknown")} x {page_size.get("height", {}).get("magnitude", "Unknown")} {page_size.get("width", {}).get("unit", "")}

Slides Breakdown:
{chr(10).join(slides_info) if slides_info else "  No slides found"}"""

    logger.info(f"Presentation retrieved successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("batch_update_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def batch_update_presentation(
    service,
    user_google_email: str,
    presentation_id: str,
    requests: List[Dict[str, Any]],
) -> str:
    """
    Apply batch updates to a Google Slides presentation.

    Args:
        user_google_email (str): The user's Google email address. Required.
        presentation_id (str): The ID of the presentation to update.
        requests (List[Dict[str, Any]]): List of update requests to apply.

    Returns:
        str: Details about the batch update operation results.
    """
    logger.info(
        f"[batch_update_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}', Requests: {len(requests)}"
    )

    body = {"requests": requests}

    result = await asyncio.to_thread(
        service.presentations()
        .batchUpdate(presentationId=presentation_id, body=body)
        .execute
    )

    replies = result.get("replies", [])

    confirmation_message = f"""Batch Update Completed for {user_google_email}:
- Presentation ID: {presentation_id}
- URL: https://docs.google.com/presentation/d/{presentation_id}/edit
- Requests Applied: {len(requests)}
- Replies Received: {len(replies)}"""

    if replies:
        confirmation_message += "\n\nUpdate Results:"
        for i, reply in enumerate(replies, 1):
            if "createSlide" in reply:
                slide_id = reply["createSlide"].get("objectId", "Unknown")
                confirmation_message += (
                    f"\n  Request {i}: Created slide with ID {slide_id}"
                )
            elif "createShape" in reply:
                shape_id = reply["createShape"].get("objectId", "Unknown")
                confirmation_message += (
                    f"\n  Request {i}: Created shape with ID {shape_id}"
                )
            else:
                confirmation_message += f"\n  Request {i}: Operation completed"

    logger.info(f"Batch update completed successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("get_page", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page(
    service, user_google_email: str, presentation_id: str, page_object_id: str
) -> str:
    """
    Get details about a specific page (slide) in a presentation.

    Args:
        user_google_email (str): The user's Google email address. Required.
        presentation_id (str): The ID of the presentation.
        page_object_id (str): The object ID of the page/slide to retrieve.

    Returns:
        str: Details about the specific page including elements and layout.
    """
    logger.info(
        f"[get_page] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Page: '{page_object_id}'"
    )

    result = await asyncio.to_thread(
        service.presentations()
        .pages()
        .get(presentationId=presentation_id, pageObjectId=page_object_id)
        .execute
    )

    page_type = result.get("pageType", "Unknown")
    page_elements = result.get("pageElements", [])

    elements_info = []
    for element in page_elements:
        element_id = element.get("objectId", "Unknown")
        if "shape" in element:
            shape_type = element["shape"].get("shapeType", "Unknown")
            elements_info.append(f"  Shape: ID {element_id}, Type: {shape_type}")
        elif "table" in element:
            table = element["table"]
            rows = table.get("rows", 0)
            cols = table.get("columns", 0)
            elements_info.append(f"  Table: ID {element_id}, Size: {rows}x{cols}")
        elif "line" in element:
            line_type = element["line"].get("lineType", "Unknown")
            elements_info.append(f"  Line: ID {element_id}, Type: {line_type}")
        else:
            elements_info.append(f"  Element: ID {element_id}, Type: Unknown")

    confirmation_message = f"""Page Details for {user_google_email}:
- Presentation ID: {presentation_id}
- Page ID: {page_object_id}
- Page Type: {page_type}
- Total Elements: {len(page_elements)}

Page Elements:
{chr(10).join(elements_info) if elements_info else "  No elements found"}"""

    logger.info(f"Page retrieved successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("get_page_thumbnail", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page_thumbnail(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    thumbnail_size: str = "MEDIUM",
) -> str:
    """
    Generate a thumbnail URL for a specific page (slide) in a presentation.

    Args:
        user_google_email (str): The user's Google email address. Required.
        presentation_id (str): The ID of the presentation.
        page_object_id (str): The object ID of the page/slide.
        thumbnail_size (str): Size of thumbnail ("LARGE", "MEDIUM", "SMALL"). Defaults to "MEDIUM".

    Returns:
        str: URL to the generated thumbnail image.
    """
    logger.info(
        f"[get_page_thumbnail] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Page: '{page_object_id}', Size: '{thumbnail_size}'"
    )

    result = await asyncio.to_thread(
        service.presentations()
        .pages()
        .getThumbnail(
            presentationId=presentation_id,
            pageObjectId=page_object_id,
            thumbnailProperties_thumbnailSize=thumbnail_size,
            thumbnailProperties_mimeType="PNG",
        )
        .execute
    )

    thumbnail_url = result.get("contentUrl", "")

    confirmation_message = f"""Thumbnail Generated for {user_google_email}:
- Presentation ID: {presentation_id}
- Page ID: {page_object_id}
- Thumbnail Size: {thumbnail_size}
- Thumbnail URL: {thumbnail_url}

You can view or download the thumbnail using the provided URL."""

    logger.info(f"Thumbnail generated successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("format_slides_text", service_type="slides")
@require_google_service("slides", "slides")
async def format_slides_text(
    service,
    user_google_email: str,
    presentation_id: str,
    page_element_id: str,
    bold: Optional[bool] = None,
    italic: Optional[bool] = None,
    underline: Optional[bool] = None,
    strikethrough: Optional[bool] = None,
    font_family: Optional[str] = None,
    font_size: Optional[float] = None,
    text_color: Optional[str] = None,
    start_index: Optional[int] = None,
    end_index: Optional[int] = None,
) -> str:
    """
    Apply text formatting to text inside a slide element (text box, shape, placeholder).

    Args:
        user_google_email: The user's Google email address. Required.
        presentation_id: ID of the presentation.
        page_element_id: Object ID of the element containing the text.
        bold, italic, underline, strikethrough: Optional boolean formatting flags.
        font_family: Optional font family name (e.g., "Arial").
        font_size: Optional font size in points.
        text_color: Optional hex color (e.g., "#FF0000").
        start_index, end_index: Optional character range. If both omitted, formats ALL text.

    Returns:
        Confirmation string with the object ID formatted.
    """
    logger.info(
        f"[format_slides_text] pres='{presentation_id}' element='{page_element_id}'"
    )

    style: Dict[str, Any] = {}
    fields: List[str] = []
    if bold is not None:
        style["bold"] = bold
        fields.append("bold")
    if italic is not None:
        style["italic"] = italic
        fields.append("italic")
    if underline is not None:
        style["underline"] = underline
        fields.append("underline")
    if strikethrough is not None:
        style["strikethrough"] = strikethrough
        fields.append("strikethrough")
    if font_family is not None:
        style["fontFamily"] = font_family
        fields.append("fontFamily")
    if font_size is not None:
        style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")
    if text_color is not None:
        style["foregroundColor"] = {
            "opaqueColor": {"rgbColor": parse_hex_color_to_rgb(text_color)}
        }
        fields.append("foregroundColor")

    if not fields:
        return "No formatting options were provided — nothing to update."

    request = {
        "updateTextStyle": {
            "objectId": page_element_id,
            "style": style,
            "textRange": build_text_range(start_index, end_index),
            "fields": ",".join(fields),
        }
    }
    await slides_batch_update(service, presentation_id, [request])
    return (
        f"Applied text formatting to element '{page_element_id}' in presentation "
        f"'{presentation_id}' for {user_google_email}. Fields: {', '.join(fields)}."
    )


@server.tool()
@handle_http_errors("format_all_slides_text", service_type="slides")
@require_google_service("slides", "slides")
async def format_all_slides_text(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: Optional[str] = None,
    bold: Optional[bool] = None,
    italic: Optional[bool] = None,
    underline: Optional[bool] = None,
    strikethrough: Optional[bool] = None,
    font_family: Optional[str] = None,
    font_size: Optional[float] = None,
    text_color: Optional[str] = None,
) -> str:
    """
    Apply text formatting to EVERY text element on a slide or across the whole
    presentation.

    Walks the presentation, collects every page element that contains text,
    and issues one updateTextStyle request per element in a single batchUpdate.
    Use this to bulk-restyle fonts, colors, or emphasis without specifying
    every element ID individually.

    Args:
        presentation_id: ID of the presentation.
        page_object_id: Optional slide ID. If provided, only formats text on
            that one slide. If omitted, formats every slide in the presentation.
        bold, italic, underline, strikethrough: Optional formatting flags.
        font_family: Optional font family (e.g., "Arial").
        font_size: Optional size in points.
        text_color: Optional hex color (e.g., "#333333").
    """
    logger.info(
        f"[format_all_slides_text] pres='{presentation_id}' page='{page_object_id}'"
    )

    style: Dict[str, Any] = {}
    fields: List[str] = []
    if bold is not None:
        style["bold"] = bold
        fields.append("bold")
    if italic is not None:
        style["italic"] = italic
        fields.append("italic")
    if underline is not None:
        style["underline"] = underline
        fields.append("underline")
    if strikethrough is not None:
        style["strikethrough"] = strikethrough
        fields.append("strikethrough")
    if font_family is not None:
        style["fontFamily"] = font_family
        fields.append("fontFamily")
    if font_size is not None:
        style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")
    if text_color is not None:
        style["foregroundColor"] = {
            "opaqueColor": {"rgbColor": parse_hex_color_to_rgb(text_color)}
        }
        fields.append("foregroundColor")

    if not fields:
        return "No formatting options were provided — nothing to update."

    presentation = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )
    element_ids = collect_text_element_ids(presentation, page_object_id)
    if not element_ids:
        scope = f"slide '{page_object_id}'" if page_object_id else "presentation"
        return f"No text-bearing elements found in {scope} '{presentation_id}'."

    fields_str = ",".join(fields)
    requests = [
        {
            "updateTextStyle": {
                "objectId": eid,
                "style": style,
                "textRange": {"type": "ALL"},
                "fields": fields_str,
            }
        }
        for eid in element_ids
    ]
    await slides_batch_update(service, presentation_id, requests)
    scope = f"slide '{page_object_id}'" if page_object_id else "entire presentation"
    return (
        f"Applied text formatting to {len(element_ids)} element(s) across {scope} "
        f"of '{presentation_id}' for {user_google_email}. Fields: {fields_str}."
    )


@server.tool()
@handle_http_errors("format_slides_paragraph", service_type="slides")
@require_google_service("slides", "slides")
async def format_slides_paragraph(
    service,
    user_google_email: str,
    presentation_id: str,
    page_element_id: str,
    alignment: Optional[str] = None,
    line_spacing: Optional[float] = None,
    space_above: Optional[float] = None,
    space_below: Optional[float] = None,
    bullet_preset: Optional[str] = None,
    start_index: Optional[int] = None,
    end_index: Optional[int] = None,
) -> str:
    """
    Apply paragraph-level formatting (alignment, spacing, bullets) to text
    inside a shape, text box, or table cell on a Google Slide.

    Use this for paragraph concerns — alignment, line spacing, space above/below,
    bullet lists. For character-level styling (bold, font size, color), use
    `format_slides_text` instead. For styling the shape itself (fill, outline,
    shadow), use `style_slides_shape`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Idempotent: re-running with the same values is safe. No rate limits beyond
    Google Slides API defaults (~300 req/min per user).

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_element_id: Object ID of the target shape/text box/table cell.
            Get it from `get_presentation` or `get_page`. Must be a text-bearing
            element; passing an image element will return a no-op error.
        alignment: Paragraph horizontal alignment. One of `START` (left),
            `CENTER`, `END` (right), `JUSTIFIED`. Omit to leave unchanged.
        line_spacing: Line spacing as percentage — `100.0` = single-spaced,
            `115.0` = 1.15x (Google Docs default), `150.0` = 1.5x, `200.0` = double.
            Omit to leave unchanged.
        space_above: Points of space above each paragraph (e.g., `12` for
            ~12pt gap). Omit to leave unchanged.
        space_below: Points of space below each paragraph. Omit to leave unchanged.
        bullet_preset: Bullet list preset name from the Google Slides API, e.g.,
            `BULLET_DISC_CIRCLE_SQUARE`, `BULLET_DIAMONDX_ARROW3D_SQUARE`,
            `NUMBERED_DIGIT_ALPHA_ROMAN`, `NUMBERED_UPPERALPHA_ALPHA_ROMAN`.
            Pass `NONE` (uppercase) to REMOVE existing bullets. Omit to leave
            bullet state unchanged.
        start_index: Optional 0-based character offset within the element's
            text where formatting starts. When both start/end are omitted,
            formatting applies to ALL paragraphs in the element.
        end_index: Optional 0-based character offset (exclusive) where
            formatting ends. Must be greater than start_index if both provided.

    Returns:
        Summary string: "Applied paragraph formatting to element '<id>' in
        presentation '<id>' for <email> (<N> request(s))." When no formatting
        args are passed: "No paragraph formatting options were provided."

    Common usage patterns:
        - Title centered + bigger gap below:
            alignment=CENTER, space_below=12
        - Indented bullet list:
            bullet_preset=BULLET_DISC_CIRCLE_SQUARE
        - Strip bullets from a list that was pasted in:
            bullet_preset=NONE
    """
    logger.info(
        f"[format_slides_paragraph] pres='{presentation_id}' element='{page_element_id}'"
    )

    requests: List[Dict[str, Any]] = []
    text_range = build_text_range(start_index, end_index)

    para_style: Dict[str, Any] = {}
    para_fields: List[str] = []
    if alignment is not None:
        para_style["alignment"] = alignment
        para_fields.append("alignment")
    if line_spacing is not None:
        para_style["lineSpacing"] = line_spacing
        para_fields.append("lineSpacing")
    if space_above is not None:
        para_style["spaceAbove"] = {"magnitude": space_above, "unit": "PT"}
        para_fields.append("spaceAbove")
    if space_below is not None:
        para_style["spaceBelow"] = {"magnitude": space_below, "unit": "PT"}
        para_fields.append("spaceBelow")

    if para_fields:
        requests.append(
            {
                "updateParagraphStyle": {
                    "objectId": page_element_id,
                    "textRange": text_range,
                    "style": para_style,
                    "fields": ",".join(para_fields),
                }
            }
        )

    if bullet_preset is not None:
        if bullet_preset.upper() == "NONE":
            requests.append(
                {
                    "deleteParagraphBullets": {
                        "objectId": page_element_id,
                        "textRange": text_range,
                    }
                }
            )
        else:
            requests.append(
                {
                    "createParagraphBullets": {
                        "objectId": page_element_id,
                        "textRange": text_range,
                        "bulletPreset": bullet_preset,
                    }
                }
            )

    if not requests:
        return "No paragraph formatting options were provided."

    await slides_batch_update(service, presentation_id, requests)
    return (
        f"Applied paragraph formatting to element '{page_element_id}' in presentation "
        f"'{presentation_id}' for {user_google_email} ({len(requests)} request(s))."
    )


@server.tool()
@handle_http_errors("style_slides_shape", service_type="slides")
@require_google_service("slides", "slides")
async def style_slides_shape(
    service,
    user_google_email: str,
    presentation_id: str,
    page_element_id: str,
    fill_color: Optional[str] = None,
    fill_alpha: float = 1.0,
    outline_color: Optional[str] = None,
    outline_weight: Optional[float] = None,
    outline_dash_style: Optional[str] = None,
) -> str:
    """
    Style an existing shape's fill and outline on a Google Slide.

    Use this to change the look of a shape that already exists — background
    fill color/opacity, outline color, outline thickness, dash pattern.
    For paragraph-level text formatting inside the shape (alignment, bullets),
    use `format_slides_paragraph`. For character styling of text (bold, font
    size), use `format_slides_text`. To create the shape in the first place,
    use `create_slides_shape`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Only fields passed in will be updated — omitted args are left unchanged.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_element_id: Object ID of the target shape. Get it from
            `get_presentation.slides[].pageElements[].objectId` or `get_page`.
            Must be a shape element; table cells and images will error.
        fill_color: Hex color for the shape interior, e.g., `#FFCC00` or
            `FFCC00`. Omit to leave existing fill unchanged.
        fill_alpha: Opacity of the fill, 0.0 (transparent) to 1.0 (opaque).
            Default `1.0`. Only meaningful when `fill_color` is also set.
        outline_color: Hex color for the shape's outline/border, e.g., `#000000`.
            Omit to leave outline color unchanged.
        outline_weight: Outline thickness in points (e.g., `1.5`, `3`, `6`).
            Omit to leave outline weight unchanged.
        outline_dash_style: Line style for the outline. One of `SOLID`, `DASH`,
            `DOT`, `DASH_DOT`, `LONG_DASH`, `LONG_DASH_DOT`. Omit to leave
            unchanged.

    Returns:
        Summary string: "Styled shape '<id>' in presentation '<id>' for
        <email> (fields: <comma-separated list of changed fields>)." When no
        style args are passed: "No shape style options were provided."
    """
    logger.info(
        f"[style_slides_shape] pres='{presentation_id}' element='{page_element_id}'"
    )

    props: Dict[str, Any] = {}
    fields: List[str] = []

    if fill_color is not None:
        props["shapeBackgroundFill"] = build_solid_fill(fill_color, fill_alpha)
        fields.append("shapeBackgroundFill")

    outline: Dict[str, Any] = {}
    if outline_color is not None:
        outline["outlineFill"] = build_solid_fill(outline_color, 1.0)
    if outline_weight is not None:
        outline["weight"] = {"magnitude": outline_weight, "unit": "PT"}
    if outline_dash_style is not None:
        outline["dashStyle"] = outline_dash_style
    if outline:
        props["outline"] = outline
        fields.append("outline")

    if not fields:
        return "No shape style options were provided."

    request = {
        "updateShapeProperties": {
            "objectId": page_element_id,
            "shapeProperties": props,
            "fields": ",".join(fields),
        }
    }
    await slides_batch_update(service, presentation_id, [request])
    return (
        f"Styled shape '{page_element_id}' in presentation '{presentation_id}' "
        f"for {user_google_email} (fields: {', '.join(fields)})."
    )


@server.tool()
@handle_http_errors("set_slides_background", service_type="slides")
@require_google_service("slides", "slides")
async def set_slides_background(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    color: str,
    alpha: float = 1.0,
) -> str:
    """
    Set the background fill color of a single slide (the page itself).

    Changes the slide's own background — distinct from styling a shape placed
    on the slide. For per-shape fill, use `style_slides_shape`. To change
    background for multiple slides, call this tool once per slide.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Overrides any inherited master/layout background with a solid color.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the slide whose background to change.
            Get it from `get_presentation.slides[].objectId`. Must reference
            a slide page (not a master/layout).
        color: Hex color for the background, e.g., `#F5F5F5` or `F5F5F5`.
            Accepts `#RRGGBB` or `RRGGBB`.
        alpha: Opacity of the background fill, 0.0 (transparent) to 1.0
            (opaque). Default `1.0`.

    Returns:
        Summary string: "Set background of slide '<id>' to <color> in
        presentation '<id>' for <email>."
    """
    logger.info(
        f"[set_slides_background] pres='{presentation_id}' page='{page_object_id}'"
    )

    request = {
        "updatePageProperties": {
            "objectId": page_object_id,
            "pageProperties": {"pageBackgroundFill": build_solid_fill(color, alpha)},
            "fields": "pageBackgroundFill",
        }
    }
    await slides_batch_update(service, presentation_id, [request])
    return (
        f"Set background of slide '{page_object_id}' to {color} in presentation "
        f"'{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("create_slides_text_box", service_type="slides")
@require_google_service("slides", "slides")
async def create_slides_text_box(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    text: str,
    left: float = 914400,
    top: float = 914400,
    width: float = 3000000,
    height: float = 1000000,
    font_size: Optional[float] = None,
    bold: Optional[bool] = None,
    italic: Optional[bool] = None,
) -> str:
    """
    Create a text box on a slide at a given position (EMU units).

    EMU reference: 914400 EMU = 1 inch. Default size roughly 3.3in x 1.1in.

    Args:
        page_object_id: Object ID of the slide to add the text box to.
        text: Initial text content.
        left, top, width, height: Position and size in EMU.
        font_size, bold, italic: Optional text formatting.

    Returns:
        The object ID of the created text box.
    """
    logger.info(
        f"[create_slides_text_box] pres='{presentation_id}' page='{page_object_id}'"
    )

    object_id = new_object_id("textBox")
    requests: List[Dict[str, Any]] = [
        {
            "createShape": {
                "objectId": object_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": build_element_properties(
                    page_object_id, left, top, width, height
                ),
            }
        },
        {
            "insertText": {
                "objectId": object_id,
                "insertionIndex": 0,
                "text": text,
            }
        },
    ]

    style: Dict[str, Any] = {}
    fields: List[str] = []
    if font_size is not None:
        style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")
    if bold is not None:
        style["bold"] = bold
        fields.append("bold")
    if italic is not None:
        style["italic"] = italic
        fields.append("italic")
    if fields:
        requests.append(
            {
                "updateTextStyle": {
                    "objectId": object_id,
                    "style": style,
                    "textRange": {"type": "ALL"},
                    "fields": ",".join(fields),
                }
            }
        )

    await slides_batch_update(service, presentation_id, requests)
    return (
        f"Created text box '{object_id}' on slide '{page_object_id}' in presentation "
        f"'{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("create_slides_shape", service_type="slides")
@require_google_service("slides", "slides")
async def create_slides_shape(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    shape_type: str,
    left: float = 914400,
    top: float = 914400,
    width: float = 2000000,
    height: float = 2000000,
    fill_color: Optional[str] = None,
) -> str:
    """
    Create a new shape element (rectangle, ellipse, arrow, etc.) on a Google Slide.

    Use this to build layouts programmatically — callouts, diagrams, backgrounds.
    For a TEXT-focused box, use `create_slides_text_box` (simpler + auto-sized
    for text). For styling an EXISTING shape (outline, shadow, filled color),
    use `style_slides_shape`. To add text inside a shape after creation, use
    `batch_update_presentation` with `insertText`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Creates exactly one shape per call. Returns the new shape's object ID so
    you can reference it in follow-up calls (inserting text, setting fill, etc.).

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the slide where the shape will be placed.
            Get it from `get_presentation.slides[].objectId` or `get_page`.
        shape_type: Shape enum from Google's API. Common values:
            `RECTANGLE`, `ROUND_RECTANGLE`, `ELLIPSE`, `TRIANGLE`, `RIGHT_TRIANGLE`,
            `DIAMOND`, `PENTAGON`, `HEXAGON`, `OCTAGON`, `PARALLELOGRAM`, `TRAPEZOID`,
            `STAR_5`, `STAR_6`, `STAR_8`, `STAR_12`, `STAR_16`, `STAR_24`, `STAR_32`,
            `ARROW_RIGHT`, `ARROW_LEFT`, `ARROW_UP`, `ARROW_DOWN`, `LEFT_RIGHT_ARROW`,
            `CLOUD`, `SUN`, `MOON`, `HEART`, `LIGHTNING_BOLT`, `SPEECH`, `CLOUD_CALLOUT`.
            Full list: https://developers.google.com/slides/api/reference/rest/v1/pages/pageElements#Type
        left: X position (top-left corner) in EMUs (English Metric Units).
            Default `914400` EMU = 1 inch from the slide's left edge.
            Conversion: 1 inch = 914,400 EMU; 1 point = 12,700 EMU; 1 cm = 360,000 EMU.
        top: Y position (top-left corner) in EMUs. Default `914400` = 1 inch down.
        width: Shape width in EMUs. Default `2000000` ≈ 2.19 inches.
        height: Shape height in EMUs. Default `2000000` ≈ 2.19 inches.
        fill_color: Optional hex color for the shape interior, e.g., `#4285F4`.
            Accepts `#RRGGBB` or `RRGGBB`. Omit for the default transparent fill
            (shape renders as an outline only until you set a fill later).

    Returns:
        Summary string: "Created <shape_type> shape '<id>' on slide '<id>' in
        presentation '<id>' for <email>." The object ID in the string is the
        newly-created shape — capture it if you need to modify the shape later
        (add text, change fill, style borders, etc.).

    Example: centered 3-inch rectangle with blue fill:
        left=1828800 (2 inches), top=1371600 (1.5 inches),
        width=2743200 (3 inches), height=914400 (1 inch),
        shape_type='RECTANGLE', fill_color='#4285F4'
    """
    logger.info(
        f"[create_slides_shape] pres='{presentation_id}' page='{page_object_id}' shape='{shape_type}'"
    )

    object_id = new_object_id("shape")
    requests: List[Dict[str, Any]] = [
        {
            "createShape": {
                "objectId": object_id,
                "shapeType": shape_type,
                "elementProperties": build_element_properties(
                    page_object_id, left, top, width, height
                ),
            }
        }
    ]
    if fill_color is not None:
        requests.append(
            {
                "updateShapeProperties": {
                    "objectId": object_id,
                    "shapeProperties": {
                        "shapeBackgroundFill": build_solid_fill(fill_color)
                    },
                    "fields": "shapeBackgroundFill",
                }
            }
        )

    await slides_batch_update(service, presentation_id, requests)
    return (
        f"Created {shape_type} shape '{object_id}' on slide '{page_object_id}' in "
        f"presentation '{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("get_slides_speaker_notes", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_slides_speaker_notes(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: Optional[str] = None,
    slide_index: Optional[int] = None,
) -> str:
    """
    Read the speaker notes text from a single slide.

    Returns the plain-text contents of the slide's speaker-notes pane (the area
    shown to the presenter in Presenter View, hidden from the audience). Useful
    for auditing/exporting notes, reviewing coverage per slide, or piping into
    transcripts. To modify notes, use `update_slides_speaker_notes`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations.readonly`
    (or broader). Read-only — safe to call repeatedly.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the target slide. Either this OR
            `slide_index` is required. Get it from
            `get_presentation.slides[].objectId`. Preferred over `slide_index`
            because object IDs are stable across slide reordering.
        slide_index: 0-based position of the slide in the deck. Used only if
            `page_object_id` is not provided. Index 0 = first slide.

    Returns:
        When notes exist: "Speaker notes for slide '<id>' in presentation
        '<id>':\\n\\n<notes text>". When the slide has no notes: "No speaker
        notes on slide '<id>' in presentation '<id>'.". When neither locator
        resolves: "Slide not found in presentation '<id>'. Tried
        page_object_id=..., slide_index=...".
    """
    logger.info(
        f"[get_slides_speaker_notes] pres='{presentation_id}' page='{page_object_id}' idx={slide_index}"
    )

    result = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )
    slides = result.get("slides", [])

    target = None
    if page_object_id is not None:
        for s in slides:
            if s.get("objectId") == page_object_id:
                target = s
                break
    elif slide_index is not None:
        if 0 <= slide_index < len(slides):
            target = slides[slide_index]

    if target is None:
        return (
            f"Slide not found in presentation '{presentation_id}'. "
            f"Tried page_object_id={page_object_id}, slide_index={slide_index}."
        )

    notes = extract_notes_text(target)
    slide_id = target.get("objectId", "unknown")
    if not notes.strip():
        return f"No speaker notes on slide '{slide_id}' in presentation '{presentation_id}'."
    return (
        f"Speaker notes for slide '{slide_id}' in presentation '{presentation_id}':\n\n{notes}"
    )


@server.tool()
@handle_http_errors("update_slides_speaker_notes", service_type="slides")
@require_google_service("slides", "slides")
async def update_slides_speaker_notes(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    notes: str,
) -> str:
    """
    Replace the speaker notes on a slide (deletes existing, inserts new).

    Fully overwrites the slide's speaker-notes pane — this is NOT an append.
    Existing notes are deleted first, then the new text is inserted. Pass an
    empty string to clear notes without adding any. To read current notes
    before overwriting, use `get_slides_speaker_notes`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the target slide. Get it from
            `get_presentation.slides[].objectId`. Must reference an existing
            slide with a notes-page shape; if the slide has no notes shape
            (rare — some custom layouts), the call returns an error string
            rather than failing.
        notes: New speaker notes text to insert. Plain text only (no rich
            formatting). Replaces ALL existing notes on this slide. Pass `""`
            to clear without adding.

    Returns:
        Summary string: "Updated speaker notes on slide '<id>' in presentation
        '<id>' for <email>." If the slide lacks a notes shape: "No speaker-notes
        shape found on slide '<id>'. Unable to update notes." If both existing
        notes were empty and `notes` is empty: "No changes to apply."
    """
    logger.info(
        f"[update_slides_speaker_notes] pres='{presentation_id}' page='{page_object_id}'"
    )

    result = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )
    target = None
    for s in result.get("slides", []):
        if s.get("objectId") == page_object_id:
            target = s
            break
    if target is None:
        return f"Slide '{page_object_id}' not found in presentation '{presentation_id}'."

    notes_info = find_notes_shape_id(target)
    if not notes_info:
        return (
            f"No speaker-notes shape found on slide '{page_object_id}'. "
            f"Unable to update notes."
        )
    _, notes_shape_id = notes_info

    requests: List[Dict[str, Any]] = []
    existing = extract_notes_text(target)
    if existing:
        requests.append(
            {
                "deleteText": {
                    "objectId": notes_shape_id,
                    "textRange": {"type": "ALL"},
                }
            }
        )
    if notes:
        requests.append(
            {
                "insertText": {
                    "objectId": notes_shape_id,
                    "insertionIndex": 0,
                    "text": notes,
                }
            }
        )
    if not requests:
        return "No changes to apply."

    await slides_batch_update(service, presentation_id, requests)
    return (
        f"Updated speaker notes on slide '{page_object_id}' in presentation "
        f"'{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("insert_slides_image", service_type="slides")
@require_google_service("slides", "slides")
async def insert_slides_image(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
    image_url: str,
    left: float = 914400,
    top: float = 914400,
    width: float = 3000000,
    height: float = 2000000,
) -> str:
    """
    Insert an image onto a slide from a publicly accessible URL.

    Google Slides fetches the image from the URL at insert time and embeds a
    reference in the presentation. The URL must be publicly accessible (or
    accessible to Google's servers) at the moment of the call — private Drive
    URLs, signed URLs, and localhost URLs will fail. Supported formats: PNG,
    JPEG, GIF (Slides does not embed SVG).

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Returns the new image's object ID so you can reference it later (reposition,
    resize, delete, etc.). Image size limit: 50 MB, 25 megapixels.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the slide to place the image on. Get it
            from `get_presentation.slides[].objectId`.
        image_url: Publicly accessible HTTPS URL pointing to a PNG/JPEG/GIF.
            Google fetches this URL server-side; must return the image bytes
            directly (no login walls, redirects to interstitial pages, etc.).
        left: X position (top-left corner) in EMUs (English Metric Units).
            Default `914400` EMU = 1 inch from the slide's left edge.
            Conversion: 1 inch = 914,400 EMU; 1 point = 12,700 EMU.
        top: Y position (top-left corner) in EMUs. Default `914400` = 1 inch down.
        width: Image width in EMUs. Default `3000000` ≈ 3.28 inches. Image is
            stretched/compressed to this size; aspect ratio is NOT preserved
            automatically — compute width:height from the source image to avoid
            distortion.
        height: Image height in EMUs. Default `2000000` ≈ 2.19 inches.

    Returns:
        Summary string: "Inserted image '<id>' on slide '<id>' in presentation
        '<id>' for <email>." The object ID in the string is the newly-created
        image element — capture it for follow-up operations.
    """
    logger.info(
        f"[insert_slides_image] pres='{presentation_id}' page='{page_object_id}'"
    )

    object_id = new_object_id("image")
    request = {
        "createImage": {
            "objectId": object_id,
            "url": image_url,
            "elementProperties": build_element_properties(
                page_object_id, left, top, width, height
            ),
        }
    }
    await slides_batch_update(service, presentation_id, [request])
    return (
        f"Inserted image '{object_id}' on slide '{page_object_id}' in presentation "
        f"'{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("delete_slides_element", service_type="slides")
@require_google_service("slides", "slides")
async def delete_slides_element(
    service,
    user_google_email: str,
    presentation_id: str,
    object_id: str,
) -> str:
    """
    Delete any object from a Google Slides presentation by its object ID.

    Works on any deletable object: an entire slide, a shape, a text box, an
    image, a table, a chart, a video, a line, etc. Passing a slide's object ID
    removes the whole slide (and everything on it). Passing a page element's
    object ID removes only that element. Deletion is permanent via API — use
    the Slides UI's undo if you need to recover.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    **Not idempotent**: re-calling with the same ID after success returns an
    error because the object no longer exists.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        object_id: Object ID of the slide or page element to delete. Get slide
            IDs from `get_presentation.slides[].objectId`. Get element IDs
            from `get_presentation.slides[].pageElements[].objectId` or from
            the return value of creator tools (`create_slides_shape`,
            `insert_slides_image`, etc.). Cannot delete master/layout pages.

    Returns:
        Summary string: "Deleted element '<id>' from presentation '<id>' for
        <email>."
    """
    logger.info(f"[delete_slides_element] pres='{presentation_id}' obj='{object_id}'")
    await slides_batch_update(
        service, presentation_id, [{"deleteObject": {"objectId": object_id}}]
    )
    return (
        f"Deleted element '{object_id}' from presentation '{presentation_id}' "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("replace_slides_text", service_type="slides")
@require_google_service("slides", "slides")
async def replace_slides_text(
    service,
    user_google_email: str,
    presentation_id: str,
    find_text: str,
    replace_text: str,
    match_case: bool = True,
) -> str:
    """
    Find-and-replace a literal string across every text element in the deck.

    Scans all slides, text boxes, shapes, table cells, and speaker notes.
    Replaces every occurrence of `find_text` with `replace_text` in a single
    batch operation. Plain substring match — no regex, wildcards, or whole-word
    matching. To do scoped replacement within a single element, edit the text
    range directly via `format_slides_text` or `modify_doc_text` equivalents.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Idempotent: re-running after all matches are replaced is a no-op (returns
    0 occurrences).

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        find_text: Literal text to search for. Exact-match substring; no
            regex, no special characters. Must be non-empty (empty string
            errors). Newlines inside `find_text` only match if the original
            document has the same literal newline characters.
        replace_text: Text to substitute for each occurrence. Can be empty to
            effectively delete matches.
        match_case: When `True` (default), matching is case-sensitive (`Hello`
            won't match `hello`). When `False`, case-insensitive — any
            capitalization variant matches and is replaced by the literal
            `replace_text` verbatim (original casing is not preserved).

    Returns:
        Summary string: "Replaced <N> occurrence(s) of '<find_text>' with
        '<replace_text>' in presentation '<id>' for <email>." Where N is
        `0` if nothing matched.

    Common usage: template-filling — put `{{placeholder}}` markers in a slide
    template, then call this once per placeholder with the real values.
    """
    logger.info(
        f"[replace_slides_text] pres='{presentation_id}' find='{find_text}'"
    )
    result = await slides_batch_update(
        service,
        presentation_id,
        [
            {
                "replaceAllText": {
                    "containsText": {"text": find_text, "matchCase": match_case},
                    "replaceText": replace_text,
                }
            }
        ],
    )
    replies = result.get("replies", [])
    occurrences = 0
    if replies and "replaceAllText" in replies[0]:
        occurrences = replies[0]["replaceAllText"].get("occurrencesChanged", 0)
    return (
        f"Replaced {occurrences} occurrence(s) of '{find_text}' with '{replace_text}' "
        f"in presentation '{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("duplicate_slide", service_type="slides")
@require_google_service("slides", "slides")
async def duplicate_slide(
    service,
    user_google_email: str,
    presentation_id: str,
    page_object_id: str,
) -> str:
    """
    Duplicate a slide (or any single page element) within a presentation.

    Creates an exact copy — same layout, content, text, styling, speaker notes
    (for slides). The duplicate is inserted immediately after the source in
    slide order. Returns the new object's ID so you can modify the copy
    independently. To move the duplicate to a different position, chain with
    `reorder_slides`. To copy a slide into a DIFFERENT presentation, use the
    Drive copy + batch-update pattern (not this tool).

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Each call duplicates one object; to duplicate many, call in a loop.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        page_object_id: Object ID of the slide OR page element to duplicate.
            Get slide IDs from `get_presentation.slides[].objectId`. Get element
            IDs from `slides[].pageElements[].objectId`. Duplicating a slide
            clones everything on it; duplicating a shape clones just that shape.

    Returns:
        Summary string: "Duplicated '<original_id>' as '<new_id>' in
        presentation '<id>' for <email>." Parse out `<new_id>` (between
        `as '` and `'`) to reference the duplicate in follow-up calls.
    """
    logger.info(
        f"[duplicate_slide] pres='{presentation_id}' page='{page_object_id}'"
    )
    result = await slides_batch_update(
        service,
        presentation_id,
        [{"duplicateObject": {"objectId": page_object_id}}],
    )
    replies = result.get("replies", [])
    new_id = "unknown"
    if replies and "duplicateObject" in replies[0]:
        new_id = replies[0]["duplicateObject"].get("objectId", "unknown")
    return (
        f"Duplicated '{page_object_id}' as '{new_id}' in presentation "
        f"'{presentation_id}' for {user_google_email}."
    )


@server.tool()
@handle_http_errors("reorder_slides", service_type="slides")
@require_google_service("slides", "slides")
async def reorder_slides(
    service,
    user_google_email: str,
    presentation_id: str,
    slide_object_ids: List[str],
    insertion_index: int,
) -> str:
    """
    Move one or more slides to a new position in the deck.

    Reorders slides by inserting them at `insertion_index` in the slide list.
    When multiple slide IDs are passed, they are placed consecutively at the
    target position, preserving the order given in `slide_object_ids`. This
    tool only reorders slides — it does not reorder page elements inside a
    slide. For that, use `batch_update_presentation` with `updatePageElementZOrder`.

    Requires OAuth scope: `https://www.googleapis.com/auth/presentations` (write).
    Google Slides API enforces that all listed IDs must currently belong to
    the presentation; mixing element IDs with slide IDs is an error.

    Args:
        presentation_id: Google Slides presentation ID (from the URL after `/d/`).
        slide_object_ids: List of slide object IDs to move, in the order you
            want them to appear after the move. Each must be a slide page ID
            (from `get_presentation.slides[].objectId`), not a page element.
            Example: `["slide_3", "slide_1"]` will place slide_3 first, then
            slide_1 at `insertion_index`.
        insertion_index: 0-based position in the re-ordered deck where the
            moved slides start. `0` = move to the front. Index is computed
            AFTER removing the slides being moved — pass the final desired
            position, not adjusted math. To move slides to the end, use the
            current slide count (e.g., if the deck has 10 slides, `10` puts
            them last; the API clamps out-of-range values to end-of-deck).

    Returns:
        Summary string: "Moved <N> slide(s) to position <index> in presentation
        '<id>' for <email>."
    """
    logger.info(
        f"[reorder_slides] pres='{presentation_id}' ids={slide_object_ids} -> {insertion_index}"
    )
    await slides_batch_update(
        service,
        presentation_id,
        [
            {
                "updateSlidesPosition": {
                    "slideObjectIds": slide_object_ids,
                    "insertionIndex": insertion_index,
                }
            }
        ],
    )
    return (
        f"Moved {len(slide_object_ids)} slide(s) to position {insertion_index} in "
        f"presentation '{presentation_id}' for {user_google_email}."
    )


# Create comment management tools for slides
_comment_tools = create_comment_tools("presentation", "presentation_id")
list_presentation_comments = _comment_tools["list_comments"]
manage_presentation_comment = _comment_tools["manage_comment"]

# Aliases for backwards compatibility and intuitive naming
list_slide_comments = list_presentation_comments
manage_slide_comment = manage_presentation_comment
