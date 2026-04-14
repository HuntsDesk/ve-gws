"""Unit tests for gslides.slides_helpers."""

import pytest

from gslides.slides_helpers import (
    build_element_properties,
    build_solid_fill,
    build_text_range,
    collect_text_element_ids,
    extract_notes_text,
    find_notes_shape_id,
    new_object_id,
    parse_hex_color_to_rgb,
)


_PRESENTATION = {
    "slides": [
        {
            "objectId": "s1",
            "pageElements": [
                {
                    "objectId": "title1",
                    "shape": {"text": {"textElements": [{"textRun": {"content": "A"}}]}},
                },
                {"objectId": "empty", "shape": {"text": {}}},  # no textElements
                {"objectId": "image", "image": {}},  # not a shape
            ],
        },
        {
            "objectId": "s2",
            "pageElements": [
                {
                    "objectId": "body2",
                    "shape": {"text": {"textElements": [{"textRun": {"content": "B"}}]}},
                },
            ],
        },
    ]
}


def test_collect_text_element_ids_entire_presentation():
    ids = collect_text_element_ids(_PRESENTATION)
    assert ids == ["title1", "body2"]


def test_collect_text_element_ids_single_slide():
    ids = collect_text_element_ids(_PRESENTATION, page_object_id="s2")
    assert ids == ["body2"]


def test_collect_text_element_ids_missing_slide_returns_empty():
    assert collect_text_element_ids(_PRESENTATION, page_object_id="nope") == []


def test_collect_text_element_ids_empty_presentation():
    assert collect_text_element_ids({}) == []
    assert collect_text_element_ids({"slides": []}) == []


def test_parse_hex_color_valid():
    rgb = parse_hex_color_to_rgb("#FF0000")
    assert rgb == {"red": 1.0, "green": 0.0, "blue": 0.0}


def test_parse_hex_color_no_hash():
    rgb = parse_hex_color_to_rgb("00FF00")
    assert rgb == {"red": 0.0, "green": 1.0, "blue": 0.0}


def test_parse_hex_color_invalid_raises():
    with pytest.raises(ValueError):
        parse_hex_color_to_rgb("#GGGGGG")
    with pytest.raises(ValueError):
        parse_hex_color_to_rgb("")
    with pytest.raises(ValueError):
        parse_hex_color_to_rgb("123")


def test_build_element_properties_emu_default():
    props = build_element_properties("slide1", 100, 200, 300, 400)
    assert props["pageObjectId"] == "slide1"
    assert props["size"]["width"]["magnitude"] == 300
    assert props["size"]["width"]["unit"] == "EMU"
    assert props["transform"]["translateX"] == 100
    assert props["transform"]["translateY"] == 200
    assert props["transform"]["scaleX"] == 1
    assert props["transform"]["scaleY"] == 1


def test_build_solid_fill_includes_alpha():
    fill = build_solid_fill("#112233", alpha=0.5)
    assert fill["solidFill"]["alpha"] == 0.5
    assert "color" in fill["solidFill"]
    assert "rgbColor" in fill["solidFill"]["color"]


def test_build_text_range_all():
    assert build_text_range() == {"type": "ALL"}


def test_build_text_range_from_start():
    r = build_text_range(start_index=5)
    assert r == {"type": "FROM_START_INDEX", "startIndex": 5}


def test_build_text_range_fixed():
    r = build_text_range(start_index=5, end_index=10)
    assert r == {"type": "FIXED_RANGE", "startIndex": 5, "endIndex": 10}


def test_new_object_id_has_prefix_and_uniqueness():
    a = new_object_id("shape")
    b = new_object_id("shape")
    assert a.startswith("shape_")
    assert a != b


def test_extract_notes_text_empty():
    assert extract_notes_text({}) == ""
    assert extract_notes_text({"slideProperties": {"notesPage": {}}}) == ""


def test_extract_notes_text_joins_runs():
    slide = {
        "slideProperties": {
            "notesPage": {
                "pageElements": [
                    {
                        "shape": {
                            "text": {
                                "textElements": [
                                    {"textRun": {"content": "Hello "}},
                                    {"textRun": {"content": "world"}},
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }
    assert extract_notes_text(slide) == "Hello world"


def test_find_notes_shape_id_via_slide_properties():
    slide = {
        "slideProperties": {
            "notesObjectId": "notes_shape_1",
            "notesPage": {
                "pageElements": [
                    {"objectId": "notes_shape_1", "shape": {"text": {}}}
                ]
            },
        }
    }
    result = find_notes_shape_id(slide)
    assert result == ("notes_shape_1", "notes_shape_1")


def test_find_notes_shape_id_fallback_scan():
    slide = {
        "slideProperties": {
            "notesPage": {
                "pageElements": [
                    {"objectId": "other", "shape": {}},
                    {"objectId": "notes_shape_2", "shape": {"text": {}}},
                ]
            }
        }
    }
    result = find_notes_shape_id(slide)
    assert result == ("notes_shape_2", "notes_shape_2")


def test_find_notes_shape_id_none():
    assert find_notes_shape_id({"slideProperties": {"notesPage": {}}}) is None
