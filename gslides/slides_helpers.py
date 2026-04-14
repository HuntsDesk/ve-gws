"""
Helper functions for Google Slides tools.

Shared utilities used by slides_tools.py for color parsing, EMU positioning,
and batch update execution.
"""

import asyncio
import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


_HEX_COLOR_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")


def parse_hex_color_to_rgb(hex_color: str) -> Dict[str, float]:
    """
    Convert '#RRGGBB' or 'RRGGBB' hex string to Google Slides rgbColor dict.

    Returns {'red': 0.0-1.0, 'green': 0.0-1.0, 'blue': 0.0-1.0}.
    Raises ValueError on invalid input.
    """
    if not hex_color:
        raise ValueError("Color cannot be empty")
    match = _HEX_COLOR_RE.match(hex_color.strip())
    if not match:
        raise ValueError(
            f"Invalid hex color '{hex_color}'. Expected format: '#RRGGBB' or 'RRGGBB'."
        )
    hex_digits = match.group(1)
    r = int(hex_digits[0:2], 16) / 255.0
    g = int(hex_digits[2:4], 16) / 255.0
    b = int(hex_digits[4:6], 16) / 255.0
    return {"red": round(r, 4), "green": round(g, 4), "blue": round(b, 4)}


def build_element_properties(
    page_object_id: str,
    left: float,
    top: float,
    width: float,
    height: float,
    unit: str = "EMU",
) -> Dict[str, Any]:
    """
    Build the elementProperties dict used by createShape/createImage/createTextBox.

    Positions and sizes are in EMU by default (1 inch = 914400 EMU, 1 pt = 12700 EMU).
    """
    return {
        "pageObjectId": page_object_id,
        "size": {
            "width": {"magnitude": width, "unit": unit},
            "height": {"magnitude": height, "unit": unit},
        },
        "transform": {
            "scaleX": 1,
            "scaleY": 1,
            "translateX": left,
            "translateY": top,
            "unit": unit,
        },
    }


def build_solid_fill(hex_color: str, alpha: float = 1.0) -> Dict[str, Any]:
    """Build a solidFill dict for shape/page backgrounds."""
    return {
        "solidFill": {
            "color": {"rgbColor": parse_hex_color_to_rgb(hex_color)},
            "alpha": alpha,
        }
    }


def build_text_range(
    start_index: Optional[int] = None, end_index: Optional[int] = None
) -> Dict[str, Any]:
    """
    Build a textRange dict for Slides API.

    If both indices are None, returns {"type": "ALL"} to target all text.
    If only start is provided, uses FROM_START_INDEX semantics.
    Otherwise returns a FIXED_RANGE.
    """
    if start_index is None and end_index is None:
        return {"type": "ALL"}
    if end_index is None:
        return {"type": "FROM_START_INDEX", "startIndex": start_index or 0}
    return {
        "type": "FIXED_RANGE",
        "startIndex": start_index or 0,
        "endIndex": end_index,
    }


async def slides_batch_update(
    service, presentation_id: str, requests: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Execute a presentations().batchUpdate call in a thread."""
    body = {"requests": requests}
    return await asyncio.to_thread(
        service.presentations()
        .batchUpdate(presentationId=presentation_id, body=body)
        .execute
    )


def new_object_id(prefix: str) -> str:
    """Generate a deterministic-format short object ID for new shapes/images/text boxes."""
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def find_notes_shape_id(slide: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Locate the speaker-notes shape within a slide resource.

    Returns (notes_object_id, notes_shape_object_id) or None if not found.
    The notes_object_id is the objectId of the notes element in notesPage.pageElements
    that matches the notesObjectId from slideProperties.
    """
    slide_props = slide.get("slideProperties") or {}
    notes_page = slide_props.get("notesPage") or {}
    target_notes_object_id = slide_props.get("notesObjectId") or notes_page.get(
        "notesProperties", {}
    ).get("speakerNotesObjectId")
    if not target_notes_object_id:
        # Fallback: scan notesPage.pageElements for the first shape with text
        for element in notes_page.get("pageElements", []):
            if "shape" in element and "text" in element.get("shape", {}):
                return (element.get("objectId"), element.get("objectId"))
        return None
    for element in notes_page.get("pageElements", []):
        if element.get("objectId") == target_notes_object_id:
            return (target_notes_object_id, target_notes_object_id)
    # Not found in pageElements — return the slideProperties ID anyway
    return (target_notes_object_id, target_notes_object_id)


def extract_notes_text(slide: Dict[str, Any]) -> str:
    """Walk a slide's notesPage and concatenate all text run content."""
    notes_page = (slide.get("slideProperties") or {}).get("notesPage") or {}
    parts: List[str] = []
    for element in notes_page.get("pageElements", []):
        shape = element.get("shape") or {}
        text = shape.get("text") or {}
        for te in text.get("textElements", []):
            tr = te.get("textRun")
            if tr and "content" in tr:
                parts.append(tr["content"])
    return "".join(parts)


def collect_text_element_ids(
    presentation: Dict[str, Any],
    page_object_id: Optional[str] = None,
) -> List[str]:
    """
    Return every page-element objectId that contains text runs.

    If page_object_id is given, limit the search to that slide; otherwise
    scan every slide in the presentation. Notes shapes are excluded — use
    update_slides_speaker_notes to touch those.
    """
    ids: List[str] = []
    for slide in presentation.get("slides", []) or []:
        if page_object_id and slide.get("objectId") != page_object_id:
            continue
        for element in slide.get("pageElements", []) or []:
            shape = element.get("shape") or {}
            text = shape.get("text")
            if text and (text.get("textElements") or []):
                obj_id = element.get("objectId")
                if obj_id:
                    ids.append(obj_id)
    return ids
