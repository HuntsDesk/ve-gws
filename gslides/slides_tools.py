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
    Apply paragraph formatting to text in a slide element.

    Args:
        alignment: One of START, CENTER, END, JUSTIFIED.
        line_spacing: Line spacing as percentage (100.0 = single, 150.0 = 1.5x).
        space_above, space_below: Paragraph spacing in points.
        bullet_preset: Bullet style preset (e.g., BULLET_DISC_CIRCLE_SQUARE,
            NUMBERED_DIGIT_ALPHA_ROMAN). Pass "NONE" to remove bullets.
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
    Style a shape's fill and outline.

    Args:
        fill_color: Hex color for shape background (e.g., "#FFCC00").
        fill_alpha: Alpha 0.0-1.0 for fill.
        outline_color: Hex color for shape outline.
        outline_weight: Outline thickness in points.
        outline_dash_style: SOLID, DASH, DOT, DASH_DOT, LONG_DASH, LONG_DASH_DOT.
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
    Set the background color of a slide.

    Args:
        page_object_id: Object ID of the slide.
        color: Hex color (e.g., "#F5F5F5").
        alpha: 0.0-1.0.
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
    Create a shape on a slide.

    Args:
        shape_type: One of RECTANGLE, ROUND_RECTANGLE, ELLIPSE, TRIANGLE, DIAMOND,
            STAR_5, ARROW_RIGHT, etc. See Google Slides API Shape enum.
        left, top, width, height: Position/size in EMU.
        fill_color: Optional hex color for the shape fill.
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
    Read speaker notes from a slide.

    Args:
        page_object_id: Object ID of the slide. Either this or slide_index is required.
        slide_index: 0-based slide index. Used if page_object_id not provided.
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
    Replace the speaker notes on a slide.

    Args:
        page_object_id: Object ID of the slide.
        notes: New speaker notes text (replaces existing).
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
    Insert an image onto a slide from a public URL.

    Args:
        image_url: Publicly accessible image URL.
        left, top, width, height: Position/size in EMU.
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
    Delete a slide, shape, text box, image, or other page element.

    Args:
        object_id: Object ID of the element (or slide) to delete.
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
    Find and replace text throughout a presentation.

    Args:
        find_text: Text to search for.
        replace_text: Replacement text.
        match_case: Whether to match case (default True).
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
    Duplicate a slide (or any object) within a presentation.

    Args:
        page_object_id: Object ID of the slide/object to duplicate.

    Returns:
        The object ID of the new duplicate.
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
    Move one or more slides to a new position.

    Args:
        slide_object_ids: List of slide object IDs to move.
        insertion_index: 0-based index where the slides should be inserted.
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
