"""
Google Calendar MCP Tools

This module provides MCP tools for interacting with Google Calendar API.
"""

import datetime
import logging
import asyncio
import re
import uuid
import json
from typing import List, Optional, Dict, Any, Union

import pytz
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

from auth.service_decorator import require_google_service
from core.utils import handle_http_errors, StringList

from core.server import server


# Configure module logger
logger = logging.getLogger(__name__)


def _parse_reminders_json(
    reminders_input: Optional[Union[str, List[Dict[str, Any]]]], function_name: str
) -> List[Dict[str, Any]]:
    """
    Parse reminders from JSON string or list object and validate them.

    Args:
        reminders_input: JSON string containing reminder objects or list of reminder objects
        function_name: Name of calling function for logging

    Returns:
        List of validated reminder objects
    """
    if not reminders_input:
        return []

    # Handle both string (JSON) and list inputs
    if isinstance(reminders_input, str):
        try:
            reminders = json.loads(reminders_input)
            if not isinstance(reminders, list):
                logger.warning(
                    f"[{function_name}] Reminders must be a JSON array, got {type(reminders).__name__}"
                )
                return []
        except json.JSONDecodeError as e:
            logger.warning(f"[{function_name}] Invalid JSON for reminders: {e}")
            return []
    elif isinstance(reminders_input, list):
        reminders = reminders_input
    else:
        logger.warning(
            f"[{function_name}] Reminders must be a JSON string or list, got {type(reminders_input).__name__}"
        )
        return []

    # Validate reminders
    if len(reminders) > 5:
        logger.warning(
            f"[{function_name}] More than 5 reminders provided, truncating to first 5"
        )
        reminders = reminders[:5]

    validated_reminders = []
    for reminder in reminders:
        if (
            not isinstance(reminder, dict)
            or "method" not in reminder
            or "minutes" not in reminder
        ):
            logger.warning(
                f"[{function_name}] Invalid reminder format: {reminder}, skipping"
            )
            continue

        method = reminder["method"].lower()
        if method not in ["popup", "email"]:
            logger.warning(
                f"[{function_name}] Invalid reminder method '{method}', must be 'popup' or 'email', skipping"
            )
            continue

        minutes = reminder["minutes"]
        if not isinstance(minutes, int) or minutes < 0 or minutes > 40320:
            logger.warning(
                f"[{function_name}] Invalid reminder minutes '{minutes}', must be integer 0-40320, skipping"
            )
            continue

        validated_reminders.append({"method": method, "minutes": minutes})

    return validated_reminders


def _apply_transparency_if_valid(
    event_body: Dict[str, Any],
    transparency: Optional[str],
    function_name: str,
) -> None:
    """
    Apply transparency to the event body if the provided value is valid.

    Args:
        event_body: Event payload being constructed.
        transparency: Provided transparency value.
        function_name: Name of the calling function for logging context.
    """
    if transparency is None:
        return

    valid_transparency_values = ["opaque", "transparent"]
    if transparency in valid_transparency_values:
        event_body["transparency"] = transparency
        logger.info(f"[{function_name}] Set transparency to '{transparency}'")
    else:
        logger.warning(
            f"[{function_name}] Invalid transparency value '{transparency}', must be 'opaque' or 'transparent', skipping"
        )


def _apply_visibility_if_valid(
    event_body: Dict[str, Any],
    visibility: Optional[str],
    function_name: str,
) -> None:
    """
    Apply visibility to the event body if the provided value is valid.

    Args:
        event_body: Event payload being constructed.
        visibility: Provided visibility value.
        function_name: Name of the calling function for logging context.
    """
    if visibility is None:
        return

    valid_visibility_values = ["default", "public", "private", "confidential"]
    if visibility in valid_visibility_values:
        event_body["visibility"] = visibility
        logger.info(f"[{function_name}] Set visibility to '{visibility}'")
    else:
        logger.warning(
            f"[{function_name}] Invalid visibility value '{visibility}', must be 'default', 'public', 'private', or 'confidential', skipping"
        )


_VALID_AUTO_DECLINE_MODES = {
    "declineAllConflictingInvitations",
    "declineOnlyNewConflictingInvitations",
    "declineNone",
}

_VALID_FOCUS_TIME_CHAT_STATUSES = {
    "available",
    "doNotDisturb",
}


def _validate_auto_decline_mode(mode: Optional[str], function_name: str) -> str:
    """Validate and return auto decline mode, defaulting to declineAllConflictingInvitations.

    Args:
        mode: The auto decline mode to validate.
        function_name: Name of the calling function for error context.

    Returns:
        A valid auto decline mode string.
    """
    if mode is None:
        return "declineAllConflictingInvitations"
    if mode not in _VALID_AUTO_DECLINE_MODES:
        raise ValueError(
            f"[{function_name}] Invalid auto_decline_mode '{mode}'. "
            f"Must be one of: {', '.join(sorted(_VALID_AUTO_DECLINE_MODES))}"
        )
    return mode


def _preserve_existing_fields(
    event_body: Dict[str, Any],
    existing_event: Dict[str, Any],
    field_mappings: Dict[str, Any],
) -> None:
    """
    Helper function to preserve existing event fields when not explicitly provided.

    Args:
        event_body: The event body being built for the API call
        existing_event: The existing event data from the API
        field_mappings: Dict mapping field names to their new values (None means preserve existing)
    """
    for field_name, new_value in field_mappings.items():
        if new_value is None and field_name in existing_event:
            event_body[field_name] = existing_event[field_name]
            logger.info(f"[modify_event] Preserving existing {field_name}")
        elif new_value is not None:
            event_body[field_name] = new_value


def _get_meeting_link(item: Dict[str, Any]) -> str:
    """Extract video meeting link from event conference data or hangoutLink."""
    conference_data = item.get("conferenceData")
    if conference_data and "entryPoints" in conference_data:
        for entry_point in conference_data["entryPoints"]:
            if entry_point.get("entryPointType") == "video":
                uri = entry_point.get("uri", "")
                if uri:
                    return uri
    hangout_link = item.get("hangoutLink", "")
    if hangout_link:
        return hangout_link
    return ""


def _format_attendee_details(
    attendees: List[Dict[str, Any]], indent: str = "  "
) -> str:
    """
      Format attendee details including response status, organizer, and optional flags.

      Example output format:
      "  user@example.com: accepted
    manager@example.com: declined (organizer)
    optional-person@example.com: tentative (optional)"

      Args:
          attendees: List of attendee dictionaries from Google Calendar API
          indent: Indentation to use for newline-separated attendees (default: "  ")

      Returns:
          Formatted string with attendee details, or "None" if no attendees
    """
    if not attendees:
        return "None"

    attendee_details_list = []
    for a in attendees:
        email = a.get("email", "unknown")
        response_status = a.get("responseStatus", "unknown")
        optional = a.get("optional", False)
        organizer = a.get("organizer", False)

        detail_parts = [f"{email}: {response_status}"]
        if organizer:
            detail_parts.append("(organizer)")
        if optional:
            detail_parts.append("(optional)")

        attendee_details_list.append(" ".join(detail_parts))

    return f"\n{indent}".join(attendee_details_list)


def _format_attachment_details(
    attachments: List[Dict[str, Any]], indent: str = "  "
) -> str:
    """
    Format attachment details including file information.


    Args:
        attachments: List of attachment dictionaries from Google Calendar API
        indent: Indentation to use for newline-separated attachments (default: "  ")

    Returns:
        Formatted string with attachment details, or "None" if no attachments
    """
    if not attachments:
        return "None"

    attachment_details_list = []
    for att in attachments:
        title = att.get("title", "Untitled")
        file_url = att.get("fileUrl", "No URL")
        file_id = att.get("fileId", "No ID")
        mime_type = att.get("mimeType", "Unknown")

        attachment_info = (
            f"{title}\n"
            f"{indent}File URL: {file_url}\n"
            f"{indent}File ID: {file_id}\n"
            f"{indent}MIME Type: {mime_type}"
        )
        attachment_details_list.append(attachment_info)

    return f"\n{indent}".join(attachment_details_list)


# Helper function to ensure time strings for API calls are correctly formatted
def _correct_time_format_for_api(
    time_str: Optional[str], param_name: str, timezone: Optional[str] = None
) -> Optional[str]:
    """Normalize a time string into RFC3339 format suitable for the Google Calendar API."""
    if not time_str:
        return None

    logger.info(
        f"_correct_time_format_for_api: Processing {param_name} with value '{time_str}', timezone: '{timezone}'"
    )

    # Handle date-only format (YYYY-MM-DD)
    if len(time_str) == 10 and time_str.count("-") == 2:
        try:
            # Validate it's a proper date
            datetime.datetime.strptime(time_str, "%Y-%m-%d")
            # For date-only, convert using the provided timezone, or UTC if not provided
            if timezone:
                try:
                    tz = pytz.timezone(timezone)
                    # Parse the date and create a datetime at midnight in the specified timezone
                    date_obj = datetime.datetime.strptime(time_str, "%Y-%m-%d")
                    dt = tz.localize(date_obj)
                    # Convert to UTC and format as RFC3339
                    formatted = (
                        dt.astimezone(datetime.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except pytz.exceptions.UnknownTimeZoneError:
                    logger.warning(
                        f"Could not apply timezone '{timezone}', falling back to UTC for {param_name}"
                    )
                    formatted = f"{time_str}T00:00:00Z"
            else:
                formatted = f"{time_str}T00:00:00Z"
            logger.info(
                f"Formatting date-only {param_name} '{time_str}' to RFC3339: '{formatted}'"
            )
            return formatted
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like a date but is not valid YYYY-MM-DD. Using as is."
            )
            return time_str

    # Specifically address YYYY-MM-DDTHH:MM:SS by appending 'Z'
    if (
        len(time_str) == 19
        and time_str[10] == "T"
        and time_str.count(":") == 2
        and not (
            time_str.endswith("Z") or ("+" in time_str[10:]) or ("-" in time_str[10:])
        )
    ):
        try:
            # Validate the format before appending 'Z'
            datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            logger.info(
                f"Formatting {param_name} '{time_str}' by appending 'Z' for UTC."
            )
            return time_str + "Z"
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like it needs 'Z' but is not valid YYYY-MM-DDTHH:MM:SS. Using as is."
            )
            return time_str

    # If it already has timezone info or doesn't match our patterns, return as is
    logger.info(f"{param_name} '{time_str}' doesn't need formatting, using as is.")
    return time_str


def _strip_utc_offset(datetime_str: str) -> str:
    """Strip UTC offset from an RFC3339 dateTime string, returning a naive local time.

    When an IANA timezone (e.g. America/Los_Angeles) is provided alongside a dateTime,
    the Google Calendar API uses the explicit offset from dateTime for scheduling and
    only uses the IANA timezone for recurrence expansion. This means an LLM-generated
    offset that doesn't account for DST (e.g. -08:00 during PDT) will place the event
    at the wrong wall-clock time.

    By stripping the offset and keeping only the naive local time + IANA timeZone,
    Google Calendar resolves the correct DST-aware offset automatically.

    Examples:
        "2026-03-19T12:00:00-08:00" → "2026-03-19T12:00:00"
        "2026-03-19T12:00:00-07:00" → "2026-03-19T12:00:00"
        "2026-03-19T12:00:00Z"      → "2026-03-19T12:00:00"
        "2026-03-19T12:00:00"       → "2026-03-19T12:00:00" (no-op)
    """
    # Strip trailing Z
    if datetime_str.endswith("Z"):
        return datetime_str[:-1]
    # Strip +HH:MM or -HH:MM offset at end (e.g. -07:00, +05:30)
    return re.sub(r"[+-]\d{2}:\d{2}$", "", datetime_str)


@server.tool()
@handle_http_errors("list_calendars", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def list_calendars(service, user_google_email: str) -> str:
    """List every calendar the user owns or has access to.

    Use this to discover calendar IDs before calling get_events, manage_event,
    or create_calendar — calendar IDs (not names) are what those tools
    require. The user's main calendar is always addressable as "primary".
    Requires the calendar.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).

    Returns:
        Formatted list with one line per calendar showing summary, primary
        marker, and calendar ID. The ID is the value to pass to other tools
        as calendar_id.
    """
    logger.info(f"[list_calendars] Invoked. Email: '{user_google_email}'")

    calendar_list_response = await asyncio.to_thread(
        lambda: service.calendarList().list().execute()
    )
    items = calendar_list_response.get("items", [])
    if not items:
        return f"No calendars found for {user_google_email}."

    calendars_summary_list = [
        f'- "{cal.get("summary", "No Summary")}"{" (Primary)" if cal.get("primary") else ""} (ID: {cal["id"]})'
        for cal in items
    ]
    text_output = (
        f"Successfully listed {len(items)} calendars for {user_google_email}:\n"
        + "\n".join(calendars_summary_list)
    )
    logger.info(f"Successfully listed {len(items)} calendars for {user_google_email}.")
    return text_output


@server.tool()
@handle_http_errors("get_events", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def get_events(
    service,
    user_google_email: str,
    calendar_id: str = "primary",
    event_id: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 25,
    query: Optional[str] = None,
    detailed: bool = False,
    include_attachments: bool = False,
) -> str:
    """Fetch events from a calendar — one by ID, or a filtered range.

    Two modes: (1) pass event_id to retrieve a single event (range/query
    params ignored); (2) omit event_id to list events in a time window,
    optionally filtered by keyword. For free/busy scanning across many
    calendars use query_freebusy instead. For creating/updating events use
    manage_event. Requires the calendar.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        calendar_id: Calendar ID from list_calendars, or "primary" for the
            user's main calendar. Default "primary".
        event_id: Specific event ID to fetch. From a prior get_events call
            or a calendar URL like calendar.google.com/calendar/u/0/r/eventedit/<id>.
            When set, all range/query filters are ignored.
        time_min: Range start, RFC3339 (e.g. "2026-05-01T00:00:00Z" or
            "2026-05-01"). Defaults to now when omitted.
        time_max: Range end, RFC3339 exclusive. Omit for open-ended range
            (capped by max_results).
        max_results: Cap on events returned, 1-2500. Default 25.
        query: Free-text filter matched against summary, description, and
            location.
        detailed: False returns just summary + times + link; True adds
            description, location, attendees with response status, and
            organizer.
        include_attachments: When detailed=True, also include attachment
            fileId/fileUrl/mimeType/title for events with attached Drive
            files. Ignored when detailed=False.

    Returns:
        Formatted event list with summary, start/end, and link per event;
        or a single-event detail block when event_id is given.
    """
    logger.info(
        f"[get_events] Raw parameters - event_id: '{event_id}', time_min: '{time_min}', time_max: '{time_max}', query: '{query}', detailed: {detailed}, include_attachments: {include_attachments}"
    )

    # Handle single event retrieval
    if event_id:
        logger.info(f"[get_events] Retrieving single event with ID: {event_id}")
        event = await asyncio.to_thread(
            lambda: (
                service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )
        items = [event]
    else:
        # Handle multiple events retrieval with time filtering
        # Ensure time_min and time_max are correctly formatted for the API
        formatted_time_min = _correct_time_format_for_api(time_min, "time_min", None)
        if formatted_time_min:
            effective_time_min = formatted_time_min
        else:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            effective_time_min = utc_now.isoformat().replace("+00:00", "Z")
        if time_min is None:
            logger.info(
                f"time_min not provided, defaulting to current UTC time: {effective_time_min}"
            )
        else:
            logger.info(
                f"time_min processing: original='{time_min}', formatted='{formatted_time_min}', effective='{effective_time_min}'"
            )

        effective_time_max = _correct_time_format_for_api(time_max, "time_max", None)
        if time_max:
            logger.info(
                f"time_max processing: original='{time_max}', formatted='{effective_time_max}'"
            )

        logger.info(
            f"[get_events] Final API parameters - calendarId: '{calendar_id}', timeMin: '{effective_time_min}', timeMax: '{effective_time_max}', maxResults: {max_results}, query: '{query}'"
        )

        # Build the request parameters dynamically
        request_params = {
            "calendarId": calendar_id,
            "timeMin": effective_time_min,
            "timeMax": effective_time_max,
            "maxResults": max_results,
            "singleEvents": True,
            "orderBy": "startTime",
        }

        if query:
            request_params["q"] = query

        events_result = await asyncio.to_thread(
            lambda: service.events().list(**request_params).execute()
        )
        items = events_result.get("items", [])
    if not items:
        if event_id:
            return f"Event with ID '{event_id}' not found in calendar '{calendar_id}' for {user_google_email}."
        else:
            return f"No events found in calendar '{calendar_id}' for {user_google_email} for the specified time range."

    # Handle returning detailed output for a single event when requested
    if event_id and detailed:
        item = items[0]
        summary = item.get("summary", "No Title")
        start = item["start"].get("dateTime", item["start"].get("date"))
        end = item["end"].get("dateTime", item["end"].get("date"))
        link = item.get("htmlLink", "No Link")
        description = item.get("description", "No Description")
        location = item.get("location", "No Location")
        color_id = item.get("colorId", "None")
        attendees = item.get("attendees", [])
        attendee_emails = (
            ", ".join([a.get("email", "") for a in attendees]) if attendees else "None"
        )
        attendee_details_str = _format_attendee_details(attendees, indent="  ")

        meeting_link = _get_meeting_link(item)

        event_details = (
            f"Event Details:\n"
            f"- Title: {summary}\n"
            f"- Starts: {start}\n"
            f"- Ends: {end}\n"
            f"- Description: {description}\n"
            f"- Location: {location}\n"
            f"- Color ID: {color_id}\n"
        )
        if meeting_link:
            event_details += f"- Meeting Link: {meeting_link}\n"
        event_details += (
            f"- Attendees: {attendee_emails}\n"
            f"- Attendee Details: {attendee_details_str}\n"
        )

        if include_attachments:
            attachments = item.get("attachments", [])
            attachment_details_str = _format_attachment_details(
                attachments, indent="  "
            )
            event_details += f"- Attachments: {attachment_details_str}\n"

        event_details += f"- Event ID: {event_id}\n- Link: {link}"
        logger.info(
            f"[get_events] Successfully retrieved detailed event {event_id} for {user_google_email}."
        )
        return event_details

    # Handle multiple events or single event with basic output
    event_details_list = []
    for item in items:
        summary = item.get("summary", "No Title")
        start_time = item["start"].get("dateTime", item["start"].get("date"))
        end_time = item["end"].get("dateTime", item["end"].get("date"))
        link = item.get("htmlLink", "No Link")
        item_event_id = item.get("id", "No ID")

        if detailed:
            # Add detailed information for multiple events
            description = item.get("description", "No Description")
            location = item.get("location", "No Location")
            attendees = item.get("attendees", [])
            attendee_emails = (
                ", ".join([a.get("email", "") for a in attendees])
                if attendees
                else "None"
            )
            attendee_details_str = _format_attendee_details(attendees, indent="    ")

            meeting_link = _get_meeting_link(item)

            event_detail_parts = (
                f'- "{summary}" (Starts: {start_time}, Ends: {end_time})\n'
                f"  Description: {description}\n"
                f"  Location: {location}\n"
            )
            if meeting_link:
                event_detail_parts += f"  Meeting Link: {meeting_link}\n"
            event_detail_parts += (
                f"  Attendees: {attendee_emails}\n"
                f"  Attendee Details: {attendee_details_str}\n"
            )

            if include_attachments:
                attachments = item.get("attachments", [])
                attachment_details_str = _format_attachment_details(
                    attachments, indent="    "
                )
                event_detail_parts += f"  Attachments: {attachment_details_str}\n"

            event_detail_parts += f"  ID: {item_event_id} | Link: {link}"
            event_details_list.append(event_detail_parts)
        else:
            # Basic output format
            meeting_link = _get_meeting_link(item)
            basic_line = f'- "{summary}" (Starts: {start_time}, Ends: {end_time})'
            if meeting_link:
                basic_line += f" Meeting: {meeting_link}"
            basic_line += f" ID: {item_event_id} | Link: {link}"
            event_details_list.append(basic_line)

    if event_id:
        # Single event basic output
        text_output = (
            f"Successfully retrieved event from calendar '{calendar_id}' for {user_google_email}:\n"
            + "\n".join(event_details_list)
        )
    else:
        # Multiple events output
        text_output = (
            f"Successfully retrieved {len(items)} events from calendar '{calendar_id}' for {user_google_email}:\n"
            + "\n".join(event_details_list)
        )

    logger.info(f"Successfully retrieved {len(items)} events for {user_google_email}.")
    return text_output


# ---------------------------------------------------------------------------
# Internal implementation functions for event create/modify/delete.
# These are called by both the consolidated ``manage_event`` tool and the
# legacy single-action tools.
# ---------------------------------------------------------------------------


async def _create_event_impl(
    service,
    user_google_email: str,
    summary: str,
    start_time: str,
    end_time: str,
    calendar_id: str = "primary",
    description: Optional[str] = None,
    location: Optional[str] = None,
    attendees: Optional[List[str]] = None,
    timezone: Optional[str] = None,
    attachments: Optional[List[str]] = None,
    add_google_meet: bool = False,
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = None,
    use_default_reminders: bool = True,
    transparency: Optional[str] = None,
    visibility: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    guests_can_modify: Optional[bool] = None,
    guests_can_invite_others: Optional[bool] = None,
    guests_can_see_other_guests: Optional[bool] = None,
) -> str:
    """Internal implementation for creating a calendar event."""
    logger.info(
        f"[create_event] Invoked. Email: '{user_google_email}', Summary: {summary}"
    )
    logger.info(f"[create_event] Incoming attachments param: {attachments}")
    # If attachments value is a string, split by comma and strip whitespace
    if attachments and isinstance(attachments, str):
        attachments = [a.strip() for a in attachments.split(",") if a.strip()]
        logger.info(
            f"[create_event] Parsed attachments list from string: {attachments}"
        )
    # When an IANA timezone is provided, strip any UTC offset from dateTime values
    # so Google Calendar resolves the correct DST-aware offset from the IANA name.
    effective_start = start_time
    effective_end = end_time
    if timezone and "T" in start_time:
        effective_start = _strip_utc_offset(start_time)
    if timezone and "T" in end_time:
        effective_end = _strip_utc_offset(end_time)
    event_body: Dict[str, Any] = {
        "summary": summary,
        "start": (
            {"date": start_time}
            if "T" not in start_time
            else {"dateTime": effective_start}
        ),
        "end": (
            {"date": end_time} if "T" not in end_time else {"dateTime": effective_end}
        ),
    }
    if recurrence:
        event_body["recurrence"] = recurrence
    if location:
        event_body["location"] = location
    if description:
        event_body["description"] = description
    if timezone:
        if "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
        if "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone
    if attendees:
        event_body["attendees"] = [{"email": email} for email in attendees]

    # Handle reminders
    if reminders is not None or not use_default_reminders:
        # If custom reminders are provided, automatically disable default reminders
        effective_use_default = use_default_reminders and reminders is None

        reminder_data = {"useDefault": effective_use_default}
        if reminders is not None:
            validated_reminders = _parse_reminders_json(reminders, "create_event")
            if validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(
                    f"[create_event] Added {len(validated_reminders)} custom reminders"
                )
                if use_default_reminders:
                    logger.info(
                        "[create_event] Custom reminders provided - disabling default reminders"
                    )

        event_body["reminders"] = reminder_data

    # Handle transparency validation
    _apply_transparency_if_valid(event_body, transparency, "create_event")

    # Handle visibility validation
    _apply_visibility_if_valid(event_body, visibility, "create_event")

    # Handle guest permissions
    if guests_can_modify is not None:
        event_body["guestsCanModify"] = guests_can_modify
        logger.info(f"[create_event] Set guestsCanModify to {guests_can_modify}")
    if guests_can_invite_others is not None:
        event_body["guestsCanInviteOthers"] = guests_can_invite_others
        logger.info(
            f"[create_event] Set guestsCanInviteOthers to {guests_can_invite_others}"
        )
    if guests_can_see_other_guests is not None:
        event_body["guestsCanSeeOtherGuests"] = guests_can_see_other_guests
        logger.info(
            f"[create_event] Set guestsCanSeeOtherGuests to {guests_can_see_other_guests}"
        )

    if add_google_meet:
        request_id = str(uuid.uuid4())
        event_body["conferenceData"] = {
            "createRequest": {
                "requestId": request_id,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        }
        logger.info(
            f"[create_event] Adding Google Meet conference with request ID: {request_id}"
        )

    if attachments:
        # Accept both file URLs and file IDs. If a URL, extract the fileId.
        event_body["attachments"] = []
        drive_service = None
        try:
            try:
                drive_service = service._http and build(
                    "drive", "v3", http=service._http
                )
            except Exception as e:
                logger.warning(
                    f"Could not build Drive service for MIME type lookup: {e}"
                )
            for att in attachments:
                file_id = None
                if att.startswith("https://"):
                    # Match /d/<id>, /file/d/<id>, ?id=<id>
                    match = re.search(r"(?:/d/|/file/d/|id=)([\w-]+)", att)
                    file_id = match.group(1) if match else None
                    logger.info(
                        f"[create_event] Extracted file_id '{file_id}' from attachment URL '{att}'"
                    )
                else:
                    file_id = att
                    logger.info(
                        f"[create_event] Using direct file_id '{file_id}' for attachment"
                    )
                if file_id:
                    file_url = f"https://drive.google.com/open?id={file_id}"
                    mime_type = "application/vnd.google-apps.drive-sdk"
                    title = "Drive Attachment"
                    # Try to get the actual MIME type and filename from Drive
                    if drive_service:
                        try:
                            file_metadata = await asyncio.to_thread(
                                lambda: (
                                    drive_service.files()
                                    .get(
                                        fileId=file_id,
                                        fields="mimeType,name",
                                        supportsAllDrives=True,
                                    )
                                    .execute()
                                )
                            )
                            mime_type = file_metadata.get("mimeType", mime_type)
                            filename = file_metadata.get("name")
                            if filename:
                                title = filename
                                logger.info(
                                    f"[create_event] Using filename '{filename}' as attachment title"
                                )
                            else:
                                logger.info(
                                    "[create_event] No filename found, using generic title"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Could not fetch metadata for file {file_id}: {e}"
                            )
                    event_body["attachments"].append(
                        {
                            "fileUrl": file_url,
                            "title": title,
                            "mimeType": mime_type,
                        }
                    )
        finally:
            if drive_service:
                drive_service.close()
        created_event = await asyncio.to_thread(
            lambda: (
                service.events()
                .insert(
                    calendarId=calendar_id,
                    body=event_body,
                    supportsAttachments=True,
                    conferenceDataVersion=1 if add_google_meet else 0,
                )
                .execute()
            )
        )
    else:
        created_event = await asyncio.to_thread(
            lambda: (
                service.events()
                .insert(
                    calendarId=calendar_id,
                    body=event_body,
                    conferenceDataVersion=1 if add_google_meet else 0,
                )
                .execute()
            )
        )
    link = created_event.get("htmlLink", "No link available")
    confirmation_message = f"Successfully created event '{created_event.get('summary', summary)}' for {user_google_email}. Link: {link}"

    # Add Google Meet information if conference was created
    if add_google_meet and "conferenceData" in created_event:
        conference_data = created_event["conferenceData"]
        if "entryPoints" in conference_data:
            for entry_point in conference_data["entryPoints"]:
                if entry_point.get("entryPointType") == "video":
                    meet_link = entry_point.get("uri", "")
                    if meet_link:
                        confirmation_message += f" Google Meet: {meet_link}"
                        break

    logger.info(
        f"Event created successfully for {user_google_email}. ID: {created_event.get('id')}, Link: {link}"
    )
    return confirmation_message


def _normalize_attendees(
    attendees: Optional[Union[List[str], List[Dict[str, Any]]]],
) -> Optional[List[Dict[str, Any]]]:
    """
    Normalize attendees input to list of attendee objects.

    Accepts either:
    - List of email strings: ["user@example.com", "other@example.com"]
    - List of attendee objects: [{"email": "user@example.com", "responseStatus": "accepted"}]
    - Mixed list of both formats

    Returns list of attendee dicts with at minimum 'email' key.
    """
    if attendees is None:
        return None

    normalized = []
    for att in attendees:
        if isinstance(att, str):
            normalized.append({"email": att})
        elif isinstance(att, dict) and "email" in att:
            normalized.append(att)
        else:
            logger.warning(
                f"[_normalize_attendees] Invalid attendee format: {att}, skipping"
            )
    return normalized if normalized else None


async def _modify_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
    summary: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    description: Optional[str] = None,
    location: Optional[str] = None,
    attendees: Optional[Union[List[str], List[Dict[str, Any]]]] = None,
    timezone: Optional[str] = None,
    add_google_meet: Optional[bool] = None,
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = None,
    use_default_reminders: Optional[bool] = None,
    transparency: Optional[str] = None,
    visibility: Optional[str] = None,
    color_id: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    guests_can_modify: Optional[bool] = None,
    guests_can_invite_others: Optional[bool] = None,
    guests_can_see_other_guests: Optional[bool] = None,
) -> str:
    """Internal implementation for modifying a calendar event."""
    logger.info(
        f"[modify_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    # Build the event body with only the fields that are provided
    event_body: Dict[str, Any] = {}
    if summary is not None:
        event_body["summary"] = summary
    if start_time is not None:
        effective_start = start_time
        if timezone is not None and "T" in start_time:
            effective_start = _strip_utc_offset(start_time)
        event_body["start"] = (
            {"date": start_time}
            if "T" not in start_time
            else {"dateTime": effective_start}
        )
        if timezone is not None and "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
    if end_time is not None:
        effective_end = end_time
        if timezone is not None and "T" in end_time:
            effective_end = _strip_utc_offset(end_time)
        event_body["end"] = (
            {"date": end_time} if "T" not in end_time else {"dateTime": effective_end}
        )
        if timezone is not None and "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone
    if description is not None:
        event_body["description"] = description
    if location is not None:
        event_body["location"] = location

    # Normalize attendees - accepts both email strings and full attendee objects
    normalized_attendees = _normalize_attendees(attendees)
    if normalized_attendees is not None:
        event_body["attendees"] = normalized_attendees

    if color_id is not None:
        event_body["colorId"] = color_id
    if recurrence is not None:
        event_body["recurrence"] = recurrence

    # Handle reminders
    if reminders is not None or use_default_reminders is not None:
        reminder_data = {}
        if use_default_reminders is not None:
            reminder_data["useDefault"] = use_default_reminders
        else:
            # Preserve existing event's useDefault value if not explicitly specified
            try:
                existing_event = (
                    service.events()
                    .get(calendarId=calendar_id, eventId=event_id)
                    .execute()
                )
                reminder_data["useDefault"] = existing_event.get("reminders", {}).get(
                    "useDefault", True
                )
            except Exception as e:
                logger.warning(
                    f"[modify_event] Could not fetch existing event for reminders: {e}"
                )
                reminder_data["useDefault"] = (
                    True  # Fallback to True if unable to fetch
                )

        # If custom reminders are provided, automatically disable default reminders
        if reminders is not None:
            if reminder_data.get("useDefault", False):
                reminder_data["useDefault"] = False
                logger.info(
                    "[modify_event] Custom reminders provided - disabling default reminders"
                )

            validated_reminders = _parse_reminders_json(reminders, "modify_event")
            if reminders and not validated_reminders:
                logger.warning(
                    "[modify_event] Reminders provided but failed validation. No custom reminders will be set."
                )
            elif validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(
                    f"[modify_event] Updated reminders with {len(validated_reminders)} custom reminders"
                )

        event_body["reminders"] = reminder_data

    # Handle transparency validation
    _apply_transparency_if_valid(event_body, transparency, "modify_event")

    # Handle visibility validation
    _apply_visibility_if_valid(event_body, visibility, "modify_event")

    # Handle guest permissions
    if guests_can_modify is not None:
        event_body["guestsCanModify"] = guests_can_modify
        logger.info(f"[modify_event] Set guestsCanModify to {guests_can_modify}")
    if guests_can_invite_others is not None:
        event_body["guestsCanInviteOthers"] = guests_can_invite_others
        logger.info(
            f"[modify_event] Set guestsCanInviteOthers to {guests_can_invite_others}"
        )
    if guests_can_see_other_guests is not None:
        event_body["guestsCanSeeOtherGuests"] = guests_can_see_other_guests
        logger.info(
            f"[modify_event] Set guestsCanSeeOtherGuests to {guests_can_see_other_guests}"
        )

    if timezone is not None and "start" not in event_body and "end" not in event_body:
        # If timezone is provided but start/end times are not, we need to fetch the existing event
        # to apply the timezone correctly. This is a simplification; a full implementation
        # might handle this more robustly or require start/end with timezone.
        # For now, we'll log a warning and skip applying timezone if start/end are missing.
        logger.warning(
            "[modify_event] Timezone provided but start_time and end_time are missing. Timezone will not be applied unless start/end times are also provided."
        )

    if not event_body:
        message = "No fields provided to modify the event."
        logger.warning(f"[modify_event] {message}")
        raise Exception(message)

    # Log the event ID for debugging
    logger.info(
        f"[modify_event] Attempting to update event with ID: '{event_id}' in calendar '{calendar_id}'"
    )

    # Get the existing event to preserve fields that aren't being updated
    try:
        existing_event = await asyncio.to_thread(
            lambda: (
                service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )
        logger.info(
            "[modify_event] Successfully retrieved existing event before update"
        )

        # Preserve existing fields if not provided in the update
        _preserve_existing_fields(
            event_body,
            existing_event,
            {
                "summary": summary,
                "description": description,
                "location": location,
                # Use the already-normalized attendee objects (if provided); otherwise preserve existing
                "attendees": event_body.get("attendees"),
                "colorId": event_body.get("colorId"),
                "recurrence": recurrence,
            },
        )

        # Handle Google Meet conference data
        if add_google_meet is not None:
            if add_google_meet:
                # Add Google Meet
                request_id = str(uuid.uuid4())
                event_body["conferenceData"] = {
                    "createRequest": {
                        "requestId": request_id,
                        "conferenceSolutionKey": {"type": "hangoutsMeet"},
                    }
                }
                logger.info(
                    f"[modify_event] Adding Google Meet conference with request ID: {request_id}"
                )
            else:
                # Remove Google Meet by setting conferenceData to empty
                event_body["conferenceData"] = {}
                logger.info("[modify_event] Removing Google Meet conference")
        elif "conferenceData" in existing_event:
            # Preserve existing conference data if not specified
            event_body["conferenceData"] = existing_event["conferenceData"]
            logger.info("[modify_event] Preserving existing conference data")

    except HttpError as get_error:
        if get_error.resp.status == 404:
            logger.error(
                f"[modify_event] Event not found during pre-update verification: {get_error}"
            )
            message = f"Event not found during verification. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'. This may be due to incorrect ID format or the event no longer exists."
            raise Exception(message)
        else:
            logger.warning(
                f"[modify_event] Error during pre-update verification, but proceeding with update: {get_error}"
            )

    # Proceed with the update
    updated_event = await asyncio.to_thread(
        lambda: (
            service.events()
            .update(
                calendarId=calendar_id,
                eventId=event_id,
                body=event_body,
                conferenceDataVersion=1,
            )
            .execute()
        )
    )

    link = updated_event.get("htmlLink", "No link available")
    confirmation_message = f"Successfully modified event '{updated_event.get('summary', summary)}' (ID: {event_id}) for {user_google_email}. Link: {link}"

    # Add Google Meet information if conference was added
    if add_google_meet is True and "conferenceData" in updated_event:
        conference_data = updated_event["conferenceData"]
        if "entryPoints" in conference_data:
            for entry_point in conference_data["entryPoints"]:
                if entry_point.get("entryPointType") == "video":
                    meet_link = entry_point.get("uri", "")
                    if meet_link:
                        confirmation_message += f" Google Meet: {meet_link}"
                        break
    elif add_google_meet is False:
        confirmation_message += " (Google Meet removed)"

    logger.info(
        f"Event modified successfully for {user_google_email}. ID: {updated_event.get('id')}, Link: {link}"
    )
    return confirmation_message


async def _delete_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
) -> str:
    """Internal implementation for deleting a calendar event."""
    logger.info(
        f"[delete_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    # Log the event ID for debugging
    logger.info(
        f"[delete_event] Attempting to delete event with ID: '{event_id}' in calendar '{calendar_id}'"
    )

    # Try to get the event first to verify it exists
    try:
        await asyncio.to_thread(
            lambda: (
                service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )
        logger.info("[delete_event] Successfully verified event exists before deletion")
    except HttpError as get_error:
        if get_error.resp.status == 404:
            logger.error(
                f"[delete_event] Event not found during pre-delete verification: {get_error}"
            )
            message = f"Event not found during verification. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'. This may be due to incorrect ID format or the event no longer exists."
            raise Exception(message)
        else:
            logger.warning(
                f"[delete_event] Error during pre-delete verification, but proceeding with deletion: {get_error}"
            )

    # Proceed with the deletion
    await asyncio.to_thread(
        lambda: (
            service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        )
    )

    confirmation_message = f"Successfully deleted event (ID: {event_id}) from calendar '{calendar_id}' for {user_google_email}."
    logger.info(f"Event deleted successfully for {user_google_email}. ID: {event_id}")
    return confirmation_message


async def _rsvp_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    response: str,
    calendar_id: str = "primary",
    comment: Optional[str] = None,
    send_updates: str = "all",
) -> str:
    """Internal implementation for responding to a calendar event invitation."""
    valid_responses = {"accepted", "declined", "tentative", "needsAction"}
    if response not in valid_responses:
        raise ValueError(
            f"Invalid response '{response}'. Must be one of: {sorted(valid_responses)}"
        )

    valid_send_updates = {"all", "externalOnly", "none"}
    if send_updates not in valid_send_updates:
        raise ValueError(
            f"Invalid send_updates '{send_updates}'. Must be one of: {sorted(valid_send_updates)}"
        )

    existing_event = await asyncio.to_thread(
        lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    )

    attendees = existing_event.get("attendees")
    if not attendees:
        raise Exception("This event has no attendee list; cannot update RSVP.")

    if existing_event.get("organizer", {}).get("self"):
        raise Exception(
            "You are the organizer of this event. Organizers cannot respond to their own invitations."
        )

    user_index = next((i for i, a in enumerate(attendees) if a.get("self")), None)
    if user_index is None:
        raise Exception(
            f"{user_google_email} was not found in the event's attendee list."
        )

    updated_attendees = [dict(a) for a in attendees]
    updated_attendees[user_index]["responseStatus"] = response
    if comment is not None:
        updated_attendees[user_index]["comment"] = comment

    updated_event = await asyncio.to_thread(
        lambda: (
            service.events()
            .patch(
                calendarId=calendar_id,
                eventId=event_id,
                body={"attendees": updated_attendees},
                sendUpdates=send_updates,
            )
            .execute()
        )
    )

    summary = updated_event.get("summary", "Unknown event")
    logger.info(
        f"[rsvp_event] RSVP for '{summary}' (ID: {event_id}) set to '{response}' for {user_google_email}."
    )
    return f"Successfully updated RSVP for '{summary}' (ID: {event_id}) to '{response}' for {user_google_email}."


# ---------------------------------------------------------------------------
# Consolidated event management tool
# ---------------------------------------------------------------------------


@server.tool()
@handle_http_errors("manage_event", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def manage_event(
    service,
    user_google_email: str,
    action: str,
    summary: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    event_id: Optional[str] = None,
    calendar_id: str = "primary",
    description: Optional[str] = None,
    location: Optional[str] = None,
    attendees: Optional[Union[StringList, List[Dict[str, Any]]]] = None,
    timezone: Optional[str] = None,
    attachments: Optional[StringList] = None,
    add_google_meet: Optional[bool] = None,
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = None,
    use_default_reminders: Optional[bool] = None,
    transparency: Optional[str] = None,
    visibility: Optional[str] = None,
    color_id: Optional[str] = None,
    recurrence: Optional[StringList] = None,
    guests_can_modify: Optional[bool] = None,
    guests_can_invite_others: Optional[bool] = None,
    guests_can_see_other_guests: Optional[bool] = None,
    response: Optional[str] = None,
    rsvp_comment: Optional[str] = None,
    send_updates: Optional[str] = None,
) -> str:
    """Create, update, delete, or RSVP to a calendar event.

    Side effects: mutates calendar state on the account. Delete is
    destructive. Attendee email notifications follow send_updates. For
    read-only fetches use get_events; for focus-time blocks use
    manage_focus_time. Requires the calendar.events OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        action: "create", "update", "delete", or "rsvp". Case-insensitive.
        summary: Event title. Required for create; optional for update.
        start_time: RFC3339 start, e.g. "2026-05-01T15:00:00-04:00" or
            "2026-05-01" for all-day. Required for create.
        end_time: RFC3339 end (exclusive). Required for create.
        event_id: Event ID from get_events. Required for update, delete,
            rsvp.
        calendar_id: Calendar ID from list_calendars, or "primary".
        description: Event body text. Supports plain text and some HTML.
        location: Free-form location string or address.
        attendees: List of emails (e.g. ["alice@ex.com"]) or attendee
            objects (e.g. [{"email": "alice@ex.com", "optional": true}]).
        timezone: IANA zone like "America/New_York". Applied to start/end
            when they are tz-naive.
        attachments: Drive file IDs or sharable URLs — attached as event
            files visible to attendees.
        add_google_meet: True to attach a Meet conference, False on update
            to remove it.
        reminders: List of reminder objects like
            [{"method": "popup", "minutes": 10}] or a JSON string of same.
            Ignored when use_default_reminders=True.
        use_default_reminders: True (default on create) to use the
            calendar's default reminders. Set False to use `reminders`.
        transparency: "opaque" shows as busy; "transparent" shows as free.
        visibility: "default", "public", "private", or "confidential".
        color_id: Color index 1-11 (update only); see Calendar color map.
        recurrence: RFC5545 rules, e.g. ["RRULE:FREQ=WEEKLY;BYDAY=MO,WE;COUNT=10"].
        guests_can_modify: Allow attendees to edit the event.
        guests_can_invite_others: Allow attendees to invite more people.
        guests_can_see_other_guests: Allow attendees to see the guest list.
        response: RSVP value for action="rsvp" — "accepted", "declined",
            "tentative", or "needsAction".
        rsvp_comment: Optional note sent with the RSVP.
        send_updates: RSVP notification behavior — "all" (default),
            "externalOnly", or "none".

    Returns:
        Confirmation line with the event summary, ID, and an HTML link
        (create/update); or a deletion/RSVP confirmation.
    """
    action_lower = action.lower().strip()
    if action_lower == "create":
        if not summary or not start_time or not end_time:
            raise ValueError(
                "summary, start_time, and end_time are required for create action"
            )
        return await _create_event_impl(
            service=service,
            user_google_email=user_google_email,
            summary=summary,
            start_time=start_time,
            end_time=end_time,
            calendar_id=calendar_id,
            description=description,
            location=location,
            attendees=attendees,
            timezone=timezone,
            attachments=attachments,
            add_google_meet=add_google_meet or False,
            reminders=reminders,
            use_default_reminders=use_default_reminders
            if use_default_reminders is not None
            else True,
            transparency=transparency,
            visibility=visibility,
            guests_can_modify=guests_can_modify,
            guests_can_invite_others=guests_can_invite_others,
            guests_can_see_other_guests=guests_can_see_other_guests,
            recurrence=recurrence,
        )
    elif action_lower == "update":
        if not event_id:
            raise ValueError("event_id is required for update action")
        return await _modify_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
            summary=summary,
            start_time=start_time,
            end_time=end_time,
            description=description,
            location=location,
            attendees=attendees,
            timezone=timezone,
            add_google_meet=add_google_meet,
            reminders=reminders,
            use_default_reminders=use_default_reminders,
            transparency=transparency,
            visibility=visibility,
            color_id=color_id,
            recurrence=recurrence,
            guests_can_modify=guests_can_modify,
            guests_can_invite_others=guests_can_invite_others,
            guests_can_see_other_guests=guests_can_see_other_guests,
        )
    elif action_lower == "delete":
        if not event_id:
            raise ValueError("event_id is required for delete action")
        return await _delete_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
        )
    elif action_lower == "rsvp":
        if not event_id:
            raise ValueError("event_id is required for rsvp action")
        if not response:
            raise ValueError("response is required for rsvp action")
        return await _rsvp_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            response=response,
            calendar_id=calendar_id,
            comment=rsvp_comment,
            send_updates=send_updates or "all",
        )
    else:
        raise ValueError(
            f"Invalid action '{action_lower}'. Must be 'create', 'update', 'delete', or 'rsvp'."
        )


# ---------------------------------------------------------------------------
# Out of Office event management
# ---------------------------------------------------------------------------


def _ooo_time_entry(
    time_str: str, is_end: bool = False, timezone: Optional[str] = None
) -> Dict[str, str]:
    """Build a start/end dict for an OOO event.

    Google Calendar API requires dateTime (not date) for outOfOffice events.
    If a date-only string (YYYY-MM-DD) is given, convert it:
      - start → YYYY-MM-DDT00:00:00
      - end   → (next day)T00:00:00  (so a single date covers the full day)
    """
    if "T" not in time_str:
        # End date is already expected to be exclusive by the caller, so both
        # date-only forms convert to midnight on the provided day.
        time_str = f"{time_str}T00:00:00"
        logger.info(f"[ooo_time_entry] Converted date-only to dateTime: {time_str}")

    has_explicit_offset = time_str.endswith("Z") or bool(
        re.search(r"[+-]\d{2}:\d{2}$", time_str)
    )
    if not has_explicit_offset and not timezone:
        raise ValueError(
            "Out of Office events require either a timezone parameter or a "
            "start/end timestamp with an explicit UTC offset."
        )

    entry: Dict[str, str] = {"dateTime": time_str}
    if timezone:
        entry["timeZone"] = timezone
    return entry


async def _create_ooo_event_impl(
    service,
    user_google_email: str,
    start_time: str,
    end_time: str,
    calendar_id: str = "primary",
    summary: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for creating an Out of Office calendar event."""
    logger.info(
        f"[create_ooo_event] Invoked. Email: '{user_google_email}', Start: {start_time}, End: {end_time}"
    )

    effective_summary = summary or "Out of Office"
    effective_decline_mode = _validate_auto_decline_mode(
        auto_decline_mode, "create_ooo_event"
    )

    event_body: Dict[str, Any] = {
        "eventType": "outOfOffice",
        "summary": effective_summary,
        "start": _ooo_time_entry(start_time, is_end=False, timezone=timezone),
        "end": _ooo_time_entry(end_time, is_end=True, timezone=timezone),
        "outOfOfficeProperties": {
            "autoDeclineMode": effective_decline_mode,
            "declineMessage": decline_message or "",
        },
        "transparency": "opaque",
    }
    if recurrence:
        event_body["recurrence"] = recurrence

    created_event = await asyncio.to_thread(
        lambda: (
            service.events().insert(calendarId=calendar_id, body=event_body).execute()
        )
    )

    event_id = created_event.get("id", "N/A")
    link = created_event.get("htmlLink", "N/A")

    start_display = created_event.get("start", {}).get(
        "date", created_event.get("start", {}).get("dateTime", "N/A")
    )
    end_display = created_event.get("end", {}).get(
        "date", created_event.get("end", {}).get("dateTime", "N/A")
    )

    confirmation = (
        f"Successfully created Out of Office event for {user_google_email}.\n"
        f"- Summary: {effective_summary}\n"
        f"- Start: {start_display}\n"
        f"- End: {end_display}\n"
        f"- Auto-decline: {effective_decline_mode}\n"
        f"- Decline message: {decline_message or '(none)'}\n"
        f"- Event ID: {event_id}\n"
        f"- Link: {link}"
    )

    logger.info(
        f"OOO event created successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


async def _list_ooo_events_impl(
    service,
    user_google_email: str,
    calendar_id: str = "primary",
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 10,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for listing Out of Office calendar events."""
    logger.info(
        f"[list_ooo_events] Invoked. Email: '{user_google_email}', time_min: {time_min}, time_max: {time_max}, timezone: {timezone}"
    )

    formatted_time_min = _correct_time_format_for_api(time_min, "time_min", timezone)
    if formatted_time_min:
        effective_time_min = formatted_time_min
    else:
        if timezone:
            try:
                tz = pytz.timezone(timezone)
                now = datetime.datetime.now(tz)
                effective_time_min = (
                    now.astimezone(datetime.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except pytz.exceptions.UnknownTimeZoneError:
                logger.warning(
                    f"Could not apply timezone '{timezone}', falling back to UTC"
                )
                utc_now = datetime.datetime.now(datetime.timezone.utc)
                effective_time_min = utc_now.isoformat().replace("+00:00", "Z")
        else:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            effective_time_min = utc_now.isoformat().replace("+00:00", "Z")

    effective_time_max = _correct_time_format_for_api(time_max, "time_max", timezone)

    request_params: Dict[str, Any] = {
        "calendarId": calendar_id,
        "timeMin": effective_time_min,
        "maxResults": max_results,
        "singleEvents": True,
        "orderBy": "startTime",
        "eventTypes": ["outOfOffice"],
    }
    if effective_time_max:
        request_params["timeMax"] = effective_time_max

    events_result = await asyncio.to_thread(
        lambda: service.events().list(**request_params).execute()
    )
    items = events_result.get("items", [])

    if not items:
        return f"No out-of-office events found for {user_google_email}."

    lines = [f"Found {len(items)} out-of-office event(s) for {user_google_email}:\n"]
    for i, item in enumerate(items, 1):
        summary = item.get("summary", "Out of Office")
        start = item.get("start", {}).get(
            "date", item.get("start", {}).get("dateTime", "N/A")
        )
        end = item.get("end", {}).get(
            "date", item.get("end", {}).get("dateTime", "N/A")
        )
        event_id = item.get("id", "N/A")
        ooo_props = item.get("outOfOfficeProperties", {})
        decline_mode = ooo_props.get("autoDeclineMode", "N/A")
        decline_msg = ooo_props.get("declineMessage", "")

        lines.append(f'{i}. "{summary}" ({start} to {end})')
        lines.append(f"   Auto-decline: {decline_mode}")
        if decline_msg:
            lines.append(f"   Decline message: {decline_msg}")
        lines.append(f"   Event ID: {event_id}")
        lines.append("")

    return "\n".join(lines).rstrip()


async def _update_ooo_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    summary: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for updating an Out of Office calendar event."""
    logger.info(
        f"[update_ooo_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    existing_event = await asyncio.to_thread(
        lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    )

    if existing_event.get("eventType") != "outOfOffice":
        raise ValueError(
            f"Event '{event_id}' is not an Out of Office event (type: '{existing_event.get('eventType', 'default')}'). "
            f"Use manage_event to update regular events."
        )

    patch_body: Dict[str, Any] = {}

    if summary is not None:
        patch_body["summary"] = summary
    if start_time is not None:
        patch_body["start"] = _ooo_time_entry(
            start_time, is_end=False, timezone=timezone
        )
    if end_time is not None:
        patch_body["end"] = _ooo_time_entry(end_time, is_end=True, timezone=timezone)
    if recurrence is not None:
        patch_body["recurrence"] = recurrence

    if auto_decline_mode is not None or decline_message is not None:
        existing_ooo_props = existing_event.get("outOfOfficeProperties", {})
        patch_body["outOfOfficeProperties"] = {
            "autoDeclineMode": _validate_auto_decline_mode(
                auto_decline_mode, "update_ooo_event"
            )
            if auto_decline_mode is not None
            else existing_ooo_props.get(
                "autoDeclineMode", "declineAllConflictingInvitations"
            ),
            "declineMessage": decline_message
            if decline_message is not None
            else existing_ooo_props.get("declineMessage", ""),
        }

    if not patch_body:
        return f"No changes specified for Out of Office event '{event_id}'."

    updated_event = await asyncio.to_thread(
        lambda: (
            service.events()
            .patch(calendarId=calendar_id, eventId=event_id, body=patch_body)
            .execute()
        )
    )

    link = updated_event.get("htmlLink", "N/A")
    start_display = updated_event.get("start", {}).get(
        "date", updated_event.get("start", {}).get("dateTime", "N/A")
    )
    end_display = updated_event.get("end", {}).get(
        "date", updated_event.get("end", {}).get("dateTime", "N/A")
    )

    confirmation = (
        f"Successfully updated Out of Office event (ID: {event_id}) for {user_google_email}.\n"
        f"- Summary: {updated_event.get('summary', 'Out of Office')}\n"
        f"- Start: {start_display}\n"
        f"- End: {end_display}\n"
        f"- Link: {link}"
    )

    logger.info(
        f"OOO event updated successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


async def _delete_ooo_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
) -> str:
    """Internal implementation for deleting an Out of Office calendar event."""
    logger.info(
        f"[delete_ooo_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    try:
        existing_event = await asyncio.to_thread(
            lambda: (
                service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )
        if existing_event.get("eventType") != "outOfOffice":
            raise ValueError(
                f"Event '{event_id}' is not an Out of Office event (type: '{existing_event.get('eventType', 'default')}'). "
                f"Use manage_event to delete regular events."
            )
    except HttpError as get_error:
        if get_error.resp.status == 404:
            raise Exception(
                f"Event not found. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'."
            )
        else:
            raise

    await asyncio.to_thread(
        lambda: (
            service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        )
    )

    confirmation = f"Successfully deleted Out of Office event (ID: {event_id}) from calendar '{calendar_id}' for {user_google_email}."
    logger.info(
        f"OOO event deleted successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


@server.tool()
@handle_http_errors("manage_out_of_office", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def manage_out_of_office(
    service,
    user_google_email: str,
    action: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    summary: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    recurrence: Optional[StringList] = None,
    timezone: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 10,
    event_id: Optional[str] = None,
    calendar_id: str = "primary",
) -> str:
    """Create, list, update, or delete Out of Office events.

    OOO events are a special event type that auto-declines conflicting
    invitations and sets Workspace presence to "Out of office". They live
    on the primary calendar only. For normal events use manage_event; for
    focus-time blocks use manage_focus_time. Side effects: mutating actions
    may auto-decline existing/incoming invites based on auto_decline_mode.
    Requires the calendar.events OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        action: "create", "list", "update", or "delete". Case-insensitive.
        start_time: Start date/time for create/update. "YYYY-MM-DD" is
            auto-converted to midnight; RFC3339 ("2026-04-05T09:00:00-04:00")
            works for partial days.
        end_time: End (exclusive). For a single full day on Apr 5, pass
            start="2026-04-05", end="2026-04-06".
        summary: Display label. Defaults to "Out of Office".
        auto_decline_mode: "declineAllConflictingInvitations" (default),
            "declineOnlyNewConflictingInvitations", or "declineNone".
        decline_message: Body of auto-decline replies sent to organizers.
        recurrence: RFC5545 rules, e.g. ["RRULE:FREQ=WEEKLY;COUNT=10"].
        timezone: IANA zone like "America/New_York". Required when
            start/end are date-only or lack a UTC offset.
        time_min: List-action range start. Defaults to now. Recurring
            series expand to instances within the range.
        time_max: List-action range end.
        max_results: List-action cap. Default 10.
        event_id: Event ID from a prior list call. Required for update
            and delete.
        calendar_id: Default "primary". OOO lives on primary calendars —
            a user's primary ID or email works, secondary calendar IDs do
            not.

    Returns:
        Confirmation with event summary/ID/link (create/update),
        formatted list of OOO events (list), or deletion confirmation.
    """
    action_lower = action.lower().strip()
    if action_lower == "create":
        if not start_time or not end_time:
            raise ValueError("start_time and end_time are required for create action")
        return await _create_ooo_event_impl(
            service=service,
            user_google_email=user_google_email,
            start_time=start_time,
            end_time=end_time,
            calendar_id=calendar_id,
            summary=summary,
            auto_decline_mode=auto_decline_mode,
            decline_message=decline_message,
            recurrence=recurrence,
            timezone=timezone,
        )
    elif action_lower == "list":
        return await _list_ooo_events_impl(
            service=service,
            user_google_email=user_google_email,
            calendar_id=calendar_id,
            time_min=time_min,
            time_max=time_max,
            max_results=max_results,
            timezone=timezone,
        )
    elif action_lower == "update":
        if not event_id:
            raise ValueError("event_id is required for update action")
        return await _update_ooo_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
            start_time=start_time,
            end_time=end_time,
            summary=summary,
            auto_decline_mode=auto_decline_mode,
            decline_message=decline_message,
            recurrence=recurrence,
            timezone=timezone,
        )
    elif action_lower == "delete":
        if not event_id:
            raise ValueError("event_id is required for delete action")
        return await _delete_ooo_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
        )
    else:
        raise ValueError(
            f"Invalid action '{action_lower}'. Must be 'create', 'list', 'update', or 'delete'."
        )


# ---------------------------------------------------------------------------
# Focus Time event helpers
# ---------------------------------------------------------------------------


def _focus_time_time_entry(
    time_str: str, is_end: bool = False, timezone: Optional[str] = None
) -> Dict[str, str]:
    """Build a start/end dict for a Focus Time event.

    Google Calendar API requires dateTime (not date) for focusTime events.
    If a date-only string (YYYY-MM-DD) is given, convert it:
      - start → YYYY-MM-DDT00:00:00
      - end   → (next day)T00:00:00  (so a single date covers the full day)
    """
    if "T" not in time_str:
        time_str = f"{time_str}T00:00:00"
        logger.info(
            f"[focus_time_time_entry] Converted date-only to dateTime: {time_str}"
        )

    has_explicit_offset = time_str.endswith("Z") or bool(
        re.search(r"[+-]\d{2}:\d{2}$", time_str)
    )
    if not has_explicit_offset and not timezone:
        raise ValueError(
            "Focus Time events require either a timezone parameter or a "
            "start/end timestamp with an explicit UTC offset."
        )

    entry: Dict[str, str] = {"dateTime": time_str}
    if timezone:
        entry["timeZone"] = timezone
    return entry


def _validate_chat_status(
    chat_status: Optional[str], function_name: str
) -> Optional[str]:
    """Validate chat status for Focus Time events."""
    if chat_status is None:
        return None
    if chat_status not in _VALID_FOCUS_TIME_CHAT_STATUSES:
        raise ValueError(
            f"[{function_name}] Invalid chat_status '{chat_status}'. "
            f"Must be one of: {', '.join(sorted(_VALID_FOCUS_TIME_CHAT_STATUSES))}"
        )
    return chat_status


async def _create_focus_time_event_impl(
    service,
    user_google_email: str,
    start_time: str,
    end_time: str,
    calendar_id: str = "primary",
    summary: Optional[str] = None,
    description: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    chat_status: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for creating a Focus Time calendar event."""
    logger.info(
        f"[create_focus_time_event] Invoked. Email: '{user_google_email}', Start: {start_time}, End: {end_time}"
    )

    effective_summary = summary or "Focus Time"
    effective_decline_mode = _validate_auto_decline_mode(
        auto_decline_mode, "create_focus_time_event"
    )
    validated_chat_status = _validate_chat_status(
        chat_status or "doNotDisturb", "create_focus_time_event"
    )

    focus_time_props: Dict[str, str] = {
        "autoDeclineMode": effective_decline_mode,
        "declineMessage": decline_message or "",
    }
    if validated_chat_status:
        focus_time_props["chatStatus"] = validated_chat_status

    event_body: Dict[str, Any] = {
        "eventType": "focusTime",
        "summary": effective_summary,
        "start": _focus_time_time_entry(start_time, is_end=False, timezone=timezone),
        "end": _focus_time_time_entry(end_time, is_end=True, timezone=timezone),
        "focusTimeProperties": focus_time_props,
        "transparency": "opaque",
    }
    if description:
        event_body["description"] = description
    if recurrence:
        event_body["recurrence"] = recurrence

    created_event = await asyncio.to_thread(
        lambda: (
            service.events().insert(calendarId=calendar_id, body=event_body).execute()
        )
    )

    event_id = created_event.get("id", "N/A")
    link = created_event.get("htmlLink", "N/A")

    start_display = created_event.get("start", {}).get(
        "date", created_event.get("start", {}).get("dateTime", "N/A")
    )
    end_display = created_event.get("end", {}).get(
        "date", created_event.get("end", {}).get("dateTime", "N/A")
    )

    confirmation = (
        f"Successfully created Focus Time event for {user_google_email}.\n"
        f"- Summary: {effective_summary}\n"
        f"- Start: {start_display}\n"
        f"- End: {end_display}\n"
        f"- Auto-decline: {effective_decline_mode}\n"
        f"- Decline message: {decline_message or '(none)'}\n"
        f"- Chat status: {validated_chat_status or '(default)'}\n"
        f"- Event ID: {event_id}\n"
        f"- Link: {link}"
    )

    logger.info(
        f"Focus Time event created successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


async def _list_focus_time_events_impl(
    service,
    user_google_email: str,
    calendar_id: str = "primary",
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 10,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for listing Focus Time calendar events."""
    logger.info(
        f"[list_focus_time_events] Invoked. Email: '{user_google_email}', time_min: {time_min}, time_max: {time_max}, timezone: {timezone}"
    )

    formatted_time_min = _correct_time_format_for_api(time_min, "time_min", timezone)
    if formatted_time_min:
        effective_time_min = formatted_time_min
    else:
        if timezone:
            try:
                tz = pytz.timezone(timezone)
                now = datetime.datetime.now(tz)
                effective_time_min = (
                    now.astimezone(datetime.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except pytz.exceptions.UnknownTimeZoneError:
                logger.warning(
                    f"Could not apply timezone '{timezone}', falling back to UTC"
                )
                utc_now = datetime.datetime.now(datetime.timezone.utc)
                effective_time_min = utc_now.isoformat().replace("+00:00", "Z")
        else:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            effective_time_min = utc_now.isoformat().replace("+00:00", "Z")

    effective_time_max = _correct_time_format_for_api(time_max, "time_max", timezone)

    request_params: Dict[str, Any] = {
        "calendarId": calendar_id,
        "timeMin": effective_time_min,
        "maxResults": max_results,
        "singleEvents": True,
        "orderBy": "startTime",
        "eventTypes": ["focusTime"],
    }
    if effective_time_max:
        request_params["timeMax"] = effective_time_max

    events_result = await asyncio.to_thread(
        lambda: service.events().list(**request_params).execute()
    )
    items = events_result.get("items", [])

    if not items:
        return f"No Focus Time events found for {user_google_email}."

    lines = [f"Found {len(items)} Focus Time event(s) for {user_google_email}:\n"]
    for i, item in enumerate(items, 1):
        summary = item.get("summary", "Focus Time")
        start = item.get("start", {}).get(
            "date", item.get("start", {}).get("dateTime", "N/A")
        )
        end = item.get("end", {}).get(
            "date", item.get("end", {}).get("dateTime", "N/A")
        )
        event_id = item.get("id", "N/A")
        ft_props = item.get("focusTimeProperties", {})
        decline_mode = ft_props.get("autoDeclineMode", "N/A")
        decline_msg = ft_props.get("declineMessage", "")
        chat_st = ft_props.get("chatStatus", "")

        lines.append(f'{i}. "{summary}" ({start} to {end})')
        lines.append(f"   Auto-decline: {decline_mode}")
        if decline_msg:
            lines.append(f"   Decline message: {decline_msg}")
        if chat_st:
            lines.append(f"   Chat status: {chat_st}")
        lines.append(f"   Event ID: {event_id}")
        lines.append("")

    return "\n".join(lines).rstrip()


async def _update_focus_time_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    chat_status: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
    timezone: Optional[str] = None,
) -> str:
    """Internal implementation for updating a Focus Time calendar event."""
    logger.info(
        f"[update_focus_time_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    existing_event = await asyncio.to_thread(
        lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    )

    if existing_event.get("eventType") != "focusTime":
        raise ValueError(
            f"Event '{event_id}' is not a Focus Time event (type: '{existing_event.get('eventType', 'default')}'). "
            f"Use manage_event to update regular events."
        )

    patch_body: Dict[str, Any] = {}

    if summary is not None:
        patch_body["summary"] = summary
    if description is not None:
        patch_body["description"] = description
    if start_time is not None:
        patch_body["start"] = _focus_time_time_entry(
            start_time, is_end=False, timezone=timezone
        )
    if end_time is not None:
        patch_body["end"] = _focus_time_time_entry(
            end_time, is_end=True, timezone=timezone
        )
    if recurrence is not None:
        patch_body["recurrence"] = recurrence

    if (
        auto_decline_mode is not None
        or decline_message is not None
        or chat_status is not None
    ):
        existing_ft_props = existing_event.get("focusTimeProperties", {})
        updated_ft_props: Dict[str, str] = {
            "autoDeclineMode": _validate_auto_decline_mode(
                auto_decline_mode, "update_focus_time_event"
            )
            if auto_decline_mode is not None
            else existing_ft_props.get(
                "autoDeclineMode", "declineAllConflictingInvitations"
            ),
            "declineMessage": decline_message
            if decline_message is not None
            else existing_ft_props.get("declineMessage", ""),
        }
        if chat_status is not None:
            validated = _validate_chat_status(chat_status, "update_focus_time_event")
            updated_ft_props["chatStatus"] = validated
        elif existing_ft_props.get("chatStatus"):
            updated_ft_props["chatStatus"] = existing_ft_props["chatStatus"]
        patch_body["focusTimeProperties"] = updated_ft_props

    if not patch_body:
        return f"No changes specified for Focus Time event '{event_id}'."

    updated_event = await asyncio.to_thread(
        lambda: (
            service.events()
            .patch(calendarId=calendar_id, eventId=event_id, body=patch_body)
            .execute()
        )
    )

    link = updated_event.get("htmlLink", "N/A")
    start_display = updated_event.get("start", {}).get(
        "date", updated_event.get("start", {}).get("dateTime", "N/A")
    )
    end_display = updated_event.get("end", {}).get(
        "date", updated_event.get("end", {}).get("dateTime", "N/A")
    )

    confirmation = (
        f"Successfully updated Focus Time event (ID: {event_id}) for {user_google_email}.\n"
        f"- Summary: {updated_event.get('summary', 'Focus Time')}\n"
        f"- Start: {start_display}\n"
        f"- End: {end_display}\n"
        f"- Link: {link}"
    )

    logger.info(
        f"Focus Time event updated successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


async def _delete_focus_time_event_impl(
    service,
    user_google_email: str,
    event_id: str,
    calendar_id: str = "primary",
) -> str:
    """Internal implementation for deleting a Focus Time calendar event."""
    logger.info(
        f"[delete_focus_time_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    try:
        existing_event = await asyncio.to_thread(
            lambda: (
                service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )
        if existing_event.get("eventType") != "focusTime":
            raise ValueError(
                f"Event '{event_id}' is not a Focus Time event (type: '{existing_event.get('eventType', 'default')}'). "
                f"Use manage_event to delete regular events."
            )
    except HttpError as get_error:
        if get_error.resp.status == 404:
            raise Exception(
                f"Event not found. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'."
            )
        else:
            raise

    await asyncio.to_thread(
        lambda: (
            service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        )
    )

    confirmation = f"Successfully deleted Focus Time event (ID: {event_id}) from calendar '{calendar_id}' for {user_google_email}."
    logger.info(
        f"Focus Time event deleted successfully for {user_google_email}. ID: {event_id}"
    )
    return confirmation


@server.tool()
@handle_http_errors("manage_focus_time", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def manage_focus_time(
    service,
    user_google_email: str,
    action: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    auto_decline_mode: Optional[str] = None,
    decline_message: Optional[str] = None,
    chat_status: Optional[str] = None,
    recurrence: Optional[StringList] = None,
    timezone: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 10,
    event_id: Optional[str] = None,
    calendar_id: str = "primary",
) -> str:
    """Create, list, update, or delete Focus Time events.

    Focus Time is a special event type that auto-declines conflicting
    invitations and (by default) sets Google Chat to Do Not Disturb for
    the duration. Lives on the primary calendar only. For regular events
    use manage_event; for OOO use manage_out_of_office. Side effects:
    mutating actions may auto-decline existing/incoming invites and flip
    chat presence. Requires the calendar.events OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        action: "create", "list", "update", or "delete". Case-insensitive.
        start_time: Start date/time. "YYYY-MM-DD" auto-converts to
            midnight; RFC3339 works for partial days.
        end_time: End (exclusive). For a full day on Apr 5 pass
            start="2026-04-05", end="2026-04-06".
        summary: Display label. Defaults to "Focus Time".
        description: Optional body text for context.
        auto_decline_mode: "declineAllConflictingInvitations" (default),
            "declineOnlyNewConflictingInvitations", or "declineNone".
        decline_message: Body of auto-decline replies.
        chat_status: "doNotDisturb" (default) or "available".
        recurrence: RFC5545 rules, e.g. ["RRULE:FREQ=WEEKLY;COUNT=10"].
        timezone: IANA zone like "America/New_York". Required when
            start/end are date-only or lack a UTC offset.
        time_min: List-action range start. Defaults to now.
        time_max: List-action range end.
        max_results: List-action cap. Default 10.
        event_id: Event ID. Required for update and delete.
        calendar_id: Default "primary". Focus Time lives on primary
            calendars only — pass "primary" or the user's primary email.

    Returns:
        Confirmation with event summary/ID/link (create/update),
        formatted list (list), or deletion confirmation.
    """
    action_lower = action.lower().strip()
    if action_lower == "create":
        if not start_time or not end_time:
            raise ValueError("start_time and end_time are required for create action")
        return await _create_focus_time_event_impl(
            service=service,
            user_google_email=user_google_email,
            start_time=start_time,
            end_time=end_time,
            calendar_id=calendar_id,
            summary=summary,
            description=description,
            auto_decline_mode=auto_decline_mode,
            decline_message=decline_message,
            chat_status=chat_status,
            recurrence=recurrence,
            timezone=timezone,
        )
    elif action_lower == "list":
        return await _list_focus_time_events_impl(
            service=service,
            user_google_email=user_google_email,
            calendar_id=calendar_id,
            time_min=time_min,
            time_max=time_max,
            max_results=max_results,
            timezone=timezone,
        )
    elif action_lower == "update":
        if not event_id:
            raise ValueError("event_id is required for update action")
        return await _update_focus_time_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
            start_time=start_time,
            end_time=end_time,
            summary=summary,
            description=description,
            auto_decline_mode=auto_decline_mode,
            decline_message=decline_message,
            chat_status=chat_status,
            recurrence=recurrence,
            timezone=timezone,
        )
    elif action_lower == "delete":
        if not event_id:
            raise ValueError("event_id is required for delete action")
        return await _delete_focus_time_event_impl(
            service=service,
            user_google_email=user_google_email,
            event_id=event_id,
            calendar_id=calendar_id,
        )
    else:
        raise ValueError(
            f"Invalid action '{action_lower}'. Must be 'create', 'list', 'update', or 'delete'."
        )


# ---------------------------------------------------------------------------
# Legacy single-action tools (deprecated -- prefer ``manage_event``)
# ---------------------------------------------------------------------------


@server.tool()
@handle_http_errors("query_freebusy", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def query_freebusy(
    service,
    user_google_email: str,
    time_min: str,
    time_max: str,
    calendar_ids: Optional[StringList] = None,
    group_expansion_max: Optional[int] = None,
    calendar_expansion_max: Optional[int] = None,
) -> str:
    """Query busy-time windows across one or more calendars.

    Use this to find scheduling conflicts or free slots before creating an
    event — it returns only busy periods, not event details. For event
    details use get_events. This is the efficient way to compare
    availability across multiple people/rooms. Requires the
    calendar.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        time_min: Interval start, RFC3339 ("2026-05-12T10:00:00Z" or
            "2026-05-12").
        time_max: Interval end, RFC3339.
        calendar_ids: Calendars to query (primary calendar, room
            resource IDs, or colleague emails if you have access).
            Defaults to ["primary"].
        group_expansion_max: Cap on members expanded from a Google group
            identifier, up to 100.
        calendar_expansion_max: Cap on calendars returned, up to 50.

    Returns:
        Formatted block per calendar listing each busy period
        (start → end) or "Status: Free" when none exist. Errors per
        calendar (access denied, not found) are surfaced inline.
    """
    logger.info(
        f"[query_freebusy] Invoked. Email: '{user_google_email}', time_min: '{time_min}', time_max: '{time_max}'"
    )

    # Format time parameters
    formatted_time_min = _correct_time_format_for_api(time_min, "time_min", None)
    formatted_time_max = _correct_time_format_for_api(time_max, "time_max", None)

    # Default to primary calendar if no calendar IDs provided
    if not calendar_ids:
        calendar_ids = ["primary"]

    # Build the request body
    request_body: Dict[str, Any] = {
        "timeMin": formatted_time_min,
        "timeMax": formatted_time_max,
        "items": [{"id": cal_id} for cal_id in calendar_ids],
    }

    if group_expansion_max is not None:
        request_body["groupExpansionMax"] = group_expansion_max
    if calendar_expansion_max is not None:
        request_body["calendarExpansionMax"] = calendar_expansion_max

    logger.info(
        f"[query_freebusy] Request body: timeMin={formatted_time_min}, timeMax={formatted_time_max}, calendars={calendar_ids}"
    )

    # Execute the freebusy query
    freebusy_result = await asyncio.to_thread(
        lambda: service.freebusy().query(body=request_body).execute()
    )

    # Parse the response
    calendars = freebusy_result.get("calendars", {})
    time_min_result = freebusy_result.get("timeMin", formatted_time_min)
    time_max_result = freebusy_result.get("timeMax", formatted_time_max)

    if not calendars:
        return f"No free/busy information found for the requested calendars for {user_google_email}."

    # Format the output
    output_lines = [
        f"Free/Busy information for {user_google_email}:",
        f"Time range: {time_min_result} to {time_max_result}",
        "",
    ]

    for cal_id, cal_data in calendars.items():
        output_lines.append(f"Calendar: {cal_id}")

        # Check for errors
        errors = cal_data.get("errors", [])
        if errors:
            output_lines.append("  Errors:")
            for error in errors:
                domain = error.get("domain", "unknown")
                reason = error.get("reason", "unknown")
                output_lines.append(f"    - {domain}: {reason}")
            output_lines.append("")
            continue

        # Get busy periods
        busy_periods = cal_data.get("busy", [])
        if not busy_periods:
            output_lines.append("  Status: Free (no busy periods)")
        else:
            output_lines.append(f"  Busy periods: {len(busy_periods)}")
            for period in busy_periods:
                start = period.get("start", "Unknown")
                end = period.get("end", "Unknown")
                output_lines.append(f"    - {start} to {end}")

        output_lines.append("")

    result_text = "\n".join(output_lines)
    logger.info(
        f"[query_freebusy] Successfully retrieved free/busy information for {len(calendars)} calendar(s)"
    )
    return result_text


@server.tool()
@handle_http_errors("create_calendar", is_read_only=False, service_type="calendar")
@require_google_service("calendar", "calendar")
async def create_calendar(
    service,
    user_google_email: str,
    summary: str,
    description: Optional[str] = None,
    timezone: Optional[str] = None,
) -> str:
    """Create a new secondary calendar owned by the user.

    Side effects: creates a new calendar and adds it to the user's
    calendar list. To add events use manage_event with the returned
    calendar_id. To share the calendar with others, use the Calendar web
    UI or ACL APIs (not exposed by this tool). Requires the full
    calendar OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        summary: Calendar display name, e.g. "Client Meetings" or
            "Personal - Fitness".
        description: Optional longer description shown in calendar
            settings.
        timezone: IANA timezone string like "America/New_York" or
            "Europe/London". Defaults to the account's default timezone
            when omitted.

    Returns:
        Confirmation line containing the new calendar's ID (pass to other
        tools as calendar_id) and summary.
    """
    logger.info(
        f"[create_calendar] Invoked. Email: '{user_google_email}', summary: '{summary}'"
    )

    body: Dict[str, Any] = {"summary": summary}
    if description:
        body["description"] = description
    if timezone:
        body["timeZone"] = timezone

    result = await asyncio.to_thread(
        lambda: service.calendars().insert(body=body).execute()
    )

    calendar_id = result["id"]
    calendar_summary = result.get("summary", summary)
    logger.info(
        f"[create_calendar] Created calendar '{calendar_summary}' with ID: {calendar_id}"
    )
    return f"Created calendar '{calendar_summary}' (ID: {calendar_id})"
