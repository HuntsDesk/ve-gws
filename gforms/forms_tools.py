"""
Google Forms MCP Tools

This module provides MCP tools for interacting with Google Forms API.
"""

import logging
import asyncio
import json
from typing import List, Optional, Dict, Any


from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


def _extract_option_values(options: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract valid option objects from Forms choice option objects.

    Returns the full option dicts (preserving fields like ``isOther``,
    ``image``, ``goToAction``, and ``goToSectionId``) while filtering
    out entries that lack a truthy ``value``.
    """
    return [option for option in options if option.get("value")]


def _get_question_type(question: Dict[str, Any]) -> str:
    """Infer a stable question/item type label from a Forms question payload."""
    choice_question = question.get("choiceQuestion")
    if choice_question:
        return choice_question.get("type", "CHOICE")

    text_question = question.get("textQuestion")
    if text_question:
        return "PARAGRAPH" if text_question.get("paragraph") else "TEXT"

    if "rowQuestion" in question:
        return "GRID_ROW"
    if "scaleQuestion" in question:
        return "SCALE"
    if "dateQuestion" in question:
        return "DATE"
    if "timeQuestion" in question:
        return "TIME"
    if "fileUploadQuestion" in question:
        return "FILE_UPLOAD"
    if "ratingQuestion" in question:
        return "RATING"

    return "QUESTION"


def _serialize_form_item(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Serialize a Forms item with the key metadata agents need for edits."""
    serialized_item: Dict[str, Any] = {
        "index": index,
        "itemId": item.get("itemId"),
        "title": item.get("title", f"Question {index}"),
    }

    if item.get("description"):
        serialized_item["description"] = item["description"]

    if "questionItem" in item:
        question = item.get("questionItem", {}).get("question", {})
        serialized_item["type"] = _get_question_type(question)
        serialized_item["required"] = question.get("required", False)

        question_id = question.get("questionId")
        if question_id:
            serialized_item["questionId"] = question_id

        choice_question = question.get("choiceQuestion")
        if choice_question:
            serialized_item["options"] = _extract_option_values(
                choice_question.get("options", [])
            )

        return serialized_item

    if "questionGroupItem" in item:
        question_group = item.get("questionGroupItem", {})
        columns = _extract_option_values(
            question_group.get("grid", {}).get("columns", {}).get("options", [])
        )

        rows = []
        for question in question_group.get("questions", []):
            row: Dict[str, Any] = {
                "title": question.get("rowQuestion", {}).get("title", "")
            }
            row_question_id = question.get("questionId")
            if row_question_id:
                row["questionId"] = row_question_id
            row["required"] = question.get("required", False)
            rows.append(row)

        serialized_item["type"] = "GRID"
        serialized_item["grid"] = {"rows": rows, "columns": columns}
        return serialized_item

    if "pageBreakItem" in item:
        serialized_item["type"] = "PAGE_BREAK"
    elif "textItem" in item:
        serialized_item["type"] = "TEXT_ITEM"
    elif "imageItem" in item:
        serialized_item["type"] = "IMAGE"
    elif "videoItem" in item:
        serialized_item["type"] = "VIDEO"
    else:
        serialized_item["type"] = "UNKNOWN"

    return serialized_item


@server.tool()
@handle_http_errors("create_form", service_type="forms")
@require_google_service("forms", "forms")
async def create_form(
    service,
    user_google_email: str,
    title: str,
    description: Optional[str] = None,
    document_title: Optional[str] = None,
) -> str:
    """Create a new Google Form with title and optional description.

    Side effects: creates a new empty form owned by the user. To add
    questions/items afterward use batch_update_form with createItem
    requests; inspect the form with get_form. Requires the forms OAuth
    scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        title: Form title shown at the top of the form to respondents.
        description: Optional subtitle text shown under the title.
        document_title: Optional browser-tab/Drive title. Defaults to
            the form's `title` if omitted.

    Returns:
        Confirmation with the new form's title, form ID, edit URL, and
        public responder URL.
    """
    logger.info(f"[create_form] Invoked. Email: '{user_google_email}', Title: {title}")

    form_body: Dict[str, Any] = {"info": {"title": title}}

    if description:
        form_body["info"]["description"] = description

    if document_title:
        form_body["info"]["document_title"] = document_title

    created_form = await asyncio.to_thread(
        service.forms().create(body=form_body).execute
    )

    form_id = created_form.get("formId")
    edit_url = f"https://docs.google.com/forms/d/{form_id}/edit"
    responder_url = created_form.get(
        "responderUri", f"https://docs.google.com/forms/d/{form_id}/viewform"
    )

    confirmation_message = f"Successfully created form '{created_form.get('info', {}).get('title', title)}' for {user_google_email}. Form ID: {form_id}. Edit URL: {edit_url}. Responder URL: {responder_url}"
    logger.info(f"Form created successfully for {user_google_email}. ID: {form_id}")
    return confirmation_message


@server.tool()
@handle_http_errors("get_form", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def get_form(service, user_google_email: str, form_id: str) -> str:
    """
    Fetch a Google Form's metadata and full item list (questions, sections,
    grids, media items) by its form ID.

    Use this before editing a form with `batch_update_form` — the returned
    item indices and `itemId`/`questionId` values are the handles you pass
    into update/delete requests. For response data (submitted answers), use
    `list_form_responses` or `get_form_response` instead.

    Requires OAuth scope: `https://www.googleapis.com/auth/forms.body` or
    `forms.body.readonly` (read-only).

    Args:
        user_google_email: The user's Google email address. Required.
        form_id: The form ID — the string after `/forms/d/` in the edit URL
            (NOT the full URL). Example: `1FAIpQLSe...`. Both user-owned and
            shared forms work if the user has at least read access.

    Returns:
        Multi-line string with:
        - Title, description, document title
        - Form ID, edit URL, responder URL
        - Human-readable item summary: index, title, type, required flag
        - Full structured JSON of all items (indices, questionIds, options,
          grid rows/columns) suitable for feeding into `batch_update_form`

    Item types surfaced: `TEXT`, `PARAGRAPH`, `CHOICE` (radio/checkbox/dropdown),
    `GRID` (with rows+columns), `SCALE`, `DATE`, `TIME`, `FILE_UPLOAD`,
    `RATING`, `PAGE_BREAK`, `TEXT_ITEM`, `IMAGE`, `VIDEO`.
    """
    logger.info(f"[get_form] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    form = await asyncio.to_thread(service.forms().get(formId=form_id).execute)

    form_info = form.get("info", {})
    title = form_info.get("title", "No Title")
    description = form_info.get("description", "No Description")
    document_title = form_info.get("documentTitle", title)

    edit_url = f"https://docs.google.com/forms/d/{form_id}/edit"
    responder_url = form.get(
        "responderUri", f"https://docs.google.com/forms/d/{form_id}/viewform"
    )

    items = form.get("items", [])
    serialized_items = [
        _serialize_form_item(item, i) for i, item in enumerate(items, 1)
    ]

    items_summary = []
    for serialized_item in serialized_items:
        item_index = serialized_item["index"]
        item_title = serialized_item.get("title", f"Item {item_index}")
        item_type = serialized_item.get("type", "UNKNOWN")
        required_text = " (Required)" if serialized_item.get("required") else ""
        items_summary.append(
            f"  {item_index}. {item_title} [{item_type}]{required_text}"
        )

    items_summary_text = (
        "\n".join(items_summary) if items_summary else "  No items found"
    )
    items_text = json.dumps(serialized_items, indent=2) if serialized_items else "[]"

    result = f"""Form Details for {user_google_email}:
- Title: "{title}"
- Description: "{description}"
- Document Title: "{document_title}"
- Form ID: {form_id}
- Edit URL: {edit_url}
- Responder URL: {responder_url}
- Items ({len(items)} total):
{items_summary_text}
- Items (structured):
{items_text}"""

    logger.info(f"Successfully retrieved form for {user_google_email}. ID: {form_id}")
    return result


@server.tool()
@handle_http_errors("set_publish_settings", service_type="forms")
@require_google_service("forms", "forms")
async def set_publish_settings(
    service,
    user_google_email: str,
    form_id: str,
    publish_as_template: bool = False,
    require_authentication: bool = False,
) -> str:
    """Update a Google Form's publishing and auth requirements.

    Side effects: mutates publish settings — changes how the form is
    discoverable (template) and who can submit (auth required). Does
    NOT change which items are on the form; for that use
    batch_update_form. Requires the forms OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        form_id: Form ID from the edit URL after /forms/d/.
        publish_as_template: True lists the form as a template in the
            Workspace template gallery. Default False.
        require_authentication: True requires respondents to sign in
            with a Google account to view/submit (their email is
            captured). False allows anonymous access. Default False.

    Returns:
        Confirmation line listing the new flag values.
    """
    logger.info(
        f"[set_publish_settings] Invoked. Email: '{user_google_email}', Form ID: {form_id}"
    )

    settings_body = {
        "publishAsTemplate": publish_as_template,
        "requireAuthentication": require_authentication,
    }

    await asyncio.to_thread(
        service.forms().setPublishSettings(formId=form_id, body=settings_body).execute
    )

    confirmation_message = f"Successfully updated publish settings for form {form_id} for {user_google_email}. Publish as template: {publish_as_template}, Require authentication: {require_authentication}"
    logger.info(
        f"Publish settings updated successfully for {user_google_email}. Form ID: {form_id}"
    )
    return confirmation_message


@server.tool()
@handle_http_errors("get_form_response", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def get_form_response(
    service, user_google_email: str, form_id: str, response_id: str
) -> str:
    """
    Fetch a single submitted response to a Google Form, including all answers
    keyed by question ID.

    Use this when you already know the specific `responseId` (e.g., from a
    prior `list_form_responses` call or from a webhook/trigger). For bulk
    listing of all responses on a form, use `list_form_responses`. To look up
    which `questionId` maps to which question prompt, call `get_form` and
    read the item list.

    Requires OAuth scope: `https://www.googleapis.com/auth/forms.responses.readonly`.

    Args:
        user_google_email: The user's Google email address. Required.
        form_id: The form ID — the string after `/forms/d/` in the edit URL.
        response_id: The unique response ID returned by `list_form_responses`
            (field `responseId`). Opaque string assigned by Google at submit
            time; not the same as a row number.

    Returns:
        Multi-line string with:
        - Form ID, response ID
        - Created timestamp, lastSubmittedTime (differs from createTime when
          the responder edited their submission)
        - Answers block: one line per answered question, formatted as
          `Question ID <questionId>: <joined answer values>`. Questions the
          responder skipped are labeled `No answer provided`. Multi-select
          (checkbox) answers are joined with `, `.

    Note: only `textAnswers` are surfaced. File-upload answers, grade info,
    and per-question feedback live in the raw API response but are not
    emitted here — fetch via `service.forms().responses().get()` directly
    if you need them.
    """
    logger.info(
        f"[get_form_response] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Response ID: {response_id}"
    )

    response = await asyncio.to_thread(
        service.forms().responses().get(formId=form_id, responseId=response_id).execute
    )

    response_id = response.get("responseId", "Unknown")
    create_time = response.get("createTime", "Unknown")
    last_submitted_time = response.get("lastSubmittedTime", "Unknown")

    answers = response.get("answers", {})
    answer_details = []
    for question_id, answer_data in answers.items():
        question_response = answer_data.get("textAnswers", {}).get("answers", [])
        if question_response:
            answer_text = ", ".join([ans.get("value", "") for ans in question_response])
            answer_details.append(f"  Question ID {question_id}: {answer_text}")
        else:
            answer_details.append(f"  Question ID {question_id}: No answer provided")

    answers_text = "\n".join(answer_details) if answer_details else "  No answers found"

    result = f"""Form Response Details for {user_google_email}:
- Form ID: {form_id}
- Response ID: {response_id}
- Created: {create_time}
- Last Submitted: {last_submitted_time}
- Answers:
{answers_text}"""

    logger.info(
        f"Successfully retrieved response for {user_google_email}. Response ID: {response_id}"
    )
    return result


@server.tool()
@handle_http_errors("list_form_responses", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def list_form_responses(
    service,
    user_google_email: str,
    form_id: str,
    page_size: int = 10,
    page_token: Optional[str] = None,
) -> str:
    """
    List submitted responses for a Google Form with basic metadata
    (response IDs, timestamps, answer counts). Paginated.

    Use this to discover response IDs and submission times, then call
    `get_form_response` with a specific `responseId` to pull the full
    answer payload. For the form's structure (questions, options),
    use `get_form`.

    Requires OAuth scope: `https://www.googleapis.com/auth/forms.responses.readonly`.

    Args:
        user_google_email: The user's Google email address. Required.
        form_id: The form ID — the string after `/forms/d/` in the edit URL.
        page_size: Maximum number of responses per page. Defaults to 10.
            Google's hard cap is 5000; practical cap depends on response
            payload size. Use smaller values (10–100) for UI-facing calls
            and larger (500–5000) for batch export.
        page_token: Opaque token from a prior call's `Next page token` line.
            Omit to fetch the first page. Tokens are one-shot — never reuse
            the same token across sessions.

    Returns:
        Multi-line string with:
        - Form ID, total responses returned on this page
        - One line per response: index, responseId, createTime,
          lastSubmittedTime, answer count (number of questions answered)
        - Pagination footer: `Next page token: <token>` if more pages
          exist, otherwise `No more pages.`
        When zero responses exist: `No responses found for form <id>...`.
    """
    logger.info(
        f"[list_form_responses] Invoked. Email: '{user_google_email}', Form ID: {form_id}"
    )

    params = {"formId": form_id, "pageSize": page_size}
    if page_token:
        params["pageToken"] = page_token

    responses_result = await asyncio.to_thread(
        service.forms().responses().list(**params).execute
    )

    responses = responses_result.get("responses", [])
    next_page_token = responses_result.get("nextPageToken")

    if not responses:
        return f"No responses found for form {form_id} for {user_google_email}."

    response_details = []
    for i, response in enumerate(responses, 1):
        response_id = response.get("responseId", "Unknown")
        create_time = response.get("createTime", "Unknown")
        last_submitted_time = response.get("lastSubmittedTime", "Unknown")

        answers_count = len(response.get("answers", {}))
        response_details.append(
            f"  {i}. Response ID: {response_id} | Created: {create_time} | Last Submitted: {last_submitted_time} | Answers: {answers_count}"
        )

    pagination_info = (
        f"\nNext page token: {next_page_token}"
        if next_page_token
        else "\nNo more pages."
    )

    result = f"""Form Responses for {user_google_email}:
- Form ID: {form_id}
- Total responses returned: {len(responses)}
- Responses:
{chr(10).join(response_details)}{pagination_info}"""

    logger.info(
        f"Successfully retrieved {len(responses)} responses for {user_google_email}. Form ID: {form_id}"
    )
    return result


# Internal implementation function for testing
async def _batch_update_form_impl(
    service: Any,
    form_id: str,
    requests: List[Dict[str, Any]],
) -> str:
    """Internal implementation for batch_update_form.

    Applies batch updates to a Google Form using the Forms API batchUpdate method.

    Args:
        service: Google Forms API service client.
        form_id: The ID of the form to update.
        requests: List of update request dictionaries.

    Returns:
        Formatted string with batch update results.
    """
    body = {"requests": requests}

    result = await asyncio.to_thread(
        service.forms().batchUpdate(formId=form_id, body=body).execute
    )

    replies = result.get("replies", [])

    confirmation_message = f"""Batch Update Completed:
- Form ID: {form_id}
- URL: https://docs.google.com/forms/d/{form_id}/edit
- Requests Applied: {len(requests)}
- Replies Received: {len(replies)}"""

    if replies:
        confirmation_message += "\n\nUpdate Results:"
        for i, reply in enumerate(replies, 1):
            if "createItem" in reply:
                item_id = reply["createItem"].get("itemId", "Unknown")
                question_ids = reply["createItem"].get("questionId", [])
                question_info = (
                    f" (Question IDs: {', '.join(question_ids)})"
                    if question_ids
                    else ""
                )
                confirmation_message += (
                    f"\n  Request {i}: Created item {item_id}{question_info}"
                )
            else:
                confirmation_message += f"\n  Request {i}: Operation completed"

    return confirmation_message


@server.tool()
@handle_http_errors("batch_update_form", service_type="forms")
@require_google_service("forms", "forms")
async def batch_update_form(
    service,
    user_google_email: str,
    form_id: str,
    requests: List[Dict[str, Any]],
) -> str:
    """Apply a batch of Forms API edit requests in one atomic call.

    Primary way to modify a form after creation — add/update/delete
    questions, reorder items, update info, toggle quiz mode, etc. All
    requests apply atomically: partial failure rolls the whole batch
    back. Use get_form first to discover existing itemIds/questionIds.
    For publish settings use set_publish_settings. Requires the forms
    OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        form_id: Form ID from the edit URL.
        requests: List of Forms API request objects — each has exactly
            one key: `createItem` (with item body + location.index),
            `updateItem` (item + updateMask), `deleteItem` (location
            index), `moveItem` (originalLocation + newLocation),
            `updateFormInfo` (info + updateMask), or `updateSettings`
            (settings + updateMask). See
            https://developers.google.com/forms/api/reference/rest/v1/forms/batchUpdate
            for full schemas.

    Returns:
        Summary with request count, reply count, edit URL, and inline
        notes for createItem replies showing new itemIds/questionIds —
        capture these for follow-up edits.
    """
    logger.info(
        f"[batch_update_form] Invoked. Email: '{user_google_email}', "
        f"Form ID: '{form_id}', Requests: {len(requests)}"
    )

    result = await _batch_update_form_impl(service, form_id, requests)

    logger.info(f"Batch update completed successfully for {user_google_email}")
    return result
