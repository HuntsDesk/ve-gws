"""
Google Apps Script MCP Tools

This module provides MCP tools for interacting with Google Apps Script API.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


# Internal implementation functions for testing
async def _list_script_projects_impl(
    service: Any,
    user_google_email: str,
    page_size: int = 50,
    page_token: Optional[str] = None,
) -> str:
    """Internal implementation for list_script_projects.

    Uses Drive API to find Apps Script files since the Script API
    does not have a projects.list method.
    """
    logger.info(
        f"[list_script_projects] Email: {user_google_email}, PageSize: {page_size}"
    )

    # Search for Apps Script files using Drive API
    query = "mimeType='application/vnd.google-apps.script' and trashed=false"
    request_params = {
        "q": query,
        "pageSize": page_size,
        "fields": "nextPageToken, files(id, name, createdTime, modifiedTime)",
        "orderBy": "modifiedTime desc",
    }
    if page_token:
        request_params["pageToken"] = page_token

    response = await asyncio.to_thread(service.files().list(**request_params).execute)

    files = response.get("files", [])

    if not files:
        return "No Apps Script projects found."

    output = [f"Found {len(files)} Apps Script projects:"]
    for file in files:
        title = file.get("name", "Untitled")
        script_id = file.get("id", "Unknown ID")
        create_time = file.get("createdTime", "Unknown")
        update_time = file.get("modifiedTime", "Unknown")

        output.append(
            f"- {title} (ID: {script_id}) Created: {create_time} Modified: {update_time}"
        )

    if "nextPageToken" in response:
        output.append(f"\nNext page token: {response['nextPageToken']}")

    logger.info(
        f"[list_script_projects] Found {len(files)} projects for {user_google_email}"
    )
    return "\n".join(output)


@server.tool()
@handle_http_errors("list_script_projects", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_script_projects(
    service: Any,
    user_google_email: str,
    page_size: int = 50,
    page_token: Optional[str] = None,
) -> str:
    """Find Apps Script projects by scanning the user's Drive.

    Uses Drive API search (mimeType='application/vnd.google-apps.script')
    because the Apps Script API has no list endpoint of its own. For
    content of a specific script use get_script_project or
    get_script_content. Requires the drive.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        page_size: Max projects per page. Default 50; Drive cap 1000.
        page_token: Cursor from a prior response's pagination token.

    Returns:
        Formatted list with one line per project: title, scriptId,
        modified time, and web link. Trailing pagination token when more
        pages exist.
    """
    return await _list_script_projects_impl(
        service, user_google_email, page_size, page_token
    )


async def _get_script_project_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Internal implementation for get_script_project."""
    logger.info(f"[get_script_project] Email: {user_google_email}, ID: {script_id}")

    # Get project metadata and content concurrently (independent requests)
    project, content = await asyncio.gather(
        asyncio.to_thread(service.projects().get(scriptId=script_id).execute),
        asyncio.to_thread(service.projects().getContent(scriptId=script_id).execute),
    )

    title = project.get("title", "Untitled")
    project_script_id = project.get("scriptId", "Unknown")
    creator = project.get("creator", {}).get("email", "Unknown")
    create_time = project.get("createTime", "Unknown")
    update_time = project.get("updateTime", "Unknown")

    output = [
        f"Project: {title} (ID: {project_script_id})",
        f"Creator: {creator}",
        f"Created: {create_time}",
        f"Modified: {update_time}",
        "",
        "Files:",
    ]

    files = content.get("files", [])
    for i, file in enumerate(files, 1):
        file_name = file.get("name", "Untitled")
        file_type = file.get("type", "Unknown")
        source = file.get("source", "")

        output.append(f"{i}. {file_name} ({file_type})")
        if source:
            output.append(f"   {source[:200]}{'...' if len(source) > 200 else ''}")
            output.append("")

    logger.info(f"[get_script_project] Retrieved project {script_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("get_script_project", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def get_script_project(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Retrieve an Apps Script project's metadata and all file contents.

    Dumps every file in the project (gs/html/json) with a 200-char source
    preview. For the full content of one file use get_script_content. For
    writing use update_script_content. Requires the script.projects
    readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Apps Script project ID from list_script_projects or
            a script URL like script.google.com/d/<scriptId>/edit.

    Returns:
        Block with project title, scriptId, creator, create/modify
        timestamps, and a numbered files list showing name, type, and a
        200-char source preview.
    """
    return await _get_script_project_impl(service, user_google_email, script_id)


async def _get_script_content_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    file_name: str,
) -> str:
    """Internal implementation for get_script_content."""
    logger.info(
        f"[get_script_content] Email: {user_google_email}, ID: {script_id}, File: {file_name}"
    )

    # Must use getContent() to retrieve files, not get() which only returns metadata
    content = await asyncio.to_thread(
        service.projects().getContent(scriptId=script_id).execute
    )

    files = content.get("files", [])
    target_file = None

    for file in files:
        if file.get("name") == file_name:
            target_file = file
            break

    if not target_file:
        return f"File '{file_name}' not found in project {script_id}"

    source = target_file.get("source", "")
    file_type = target_file.get("type", "Unknown")

    output = [f"File: {file_name} ({file_type})", "", source]

    logger.info(f"[get_script_content] Retrieved file {file_name} from {script_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("get_script_content", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def get_script_content(
    service: Any,
    user_google_email: str,
    script_id: str,
    file_name: str,
) -> str:
    """Retrieve the full source of one file in an Apps Script project.

    Use this after get_script_project has surfaced the file names. For
    project-wide overview use get_script_project. Requires the
    script.projects readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Apps Script project ID.
        file_name: File name as it appears in the Apps Script editor
            (without extension — e.g. "Code", "appsscript",
            "helpers"). Matched exactly.

    Returns:
        Block with the file header (name, type) followed by the full
        source. Returns a "not found" message when file_name doesn't
        match any file in the project.
    """
    return await _get_script_content_impl(
        service, user_google_email, script_id, file_name
    )


async def _create_script_project_impl(
    service: Any,
    user_google_email: str,
    title: str,
    parent_id: Optional[str] = None,
) -> str:
    """Internal implementation for create_script_project."""
    logger.info(f"[create_script_project] Email: {user_google_email}, Title: {title}")

    request_body = {"title": title}

    if parent_id:
        request_body["parentId"] = parent_id

    project = await asyncio.to_thread(
        service.projects().create(body=request_body).execute
    )

    script_id = project.get("scriptId", "Unknown")
    edit_url = f"https://script.google.com/d/{script_id}/edit"

    output = [
        f"Created Apps Script project: {title}",
        f"Script ID: {script_id}",
        f"Edit URL: {edit_url}",
    ]

    logger.info(f"[create_script_project] Created project {script_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("create_script_project", service_type="script")
@require_google_service("script", "script_projects")
async def create_script_project(
    service: Any,
    user_google_email: str,
    title: str,
    parent_id: Optional[str] = None,
) -> str:
    """Create a new (standalone or container-bound) Apps Script project.

    Side effects: creates a new Apps Script project in Drive. Pass a
    parent_id of a Sheet/Doc/Form/Slides file ID to create a bound
    script; omit for a standalone script. To add files afterwards use
    update_script_content. Requires the script.projects OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        title: Display title for the new project.
        parent_id: Optional Drive ID of the container (Sheet/Doc/Form/
            Slides) the script is bound to. Omit for a standalone
            project.

    Returns:
        Confirmation with the project title, new scriptId, and editor
        URL (script.google.com/d/<id>/edit).
    """
    return await _create_script_project_impl(
        service, user_google_email, title, parent_id
    )


async def _update_script_content_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    files: List[Dict[str, str]],
) -> str:
    """Internal implementation for update_script_content."""
    logger.info(
        f"[update_script_content] Email: {user_google_email}, ID: {script_id}, Files: {len(files)}"
    )

    request_body = {"files": files}

    updated_content = await asyncio.to_thread(
        service.projects().updateContent(scriptId=script_id, body=request_body).execute
    )

    output = [f"Updated script project: {script_id}", "", "Modified files:"]

    for file in updated_content.get("files", []):
        file_name = file.get("name", "Untitled")
        file_type = file.get("type", "Unknown")
        output.append(f"- {file_name} ({file_type})")

    logger.info(f"[update_script_content] Updated {len(files)} files in {script_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("update_script_content", service_type="script")
@require_google_service("script", "script_projects")
async def update_script_content(
    service: Any,
    user_google_email: str,
    script_id: str,
    files: List[Dict[str, str]],
) -> str:
    """Write files into an Apps Script project (replacing the full set).

    Side effect: this is a full replace of the project's files — any
    file NOT included in `files` is deleted. Always fetch via
    get_script_project first, modify the set, and pass the complete
    list back. Requires the script.projects OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID.
        files: Complete list of file objects. Each dict needs:
            - "name" (str): file name without extension.
            - "type" (str): "SERVER_JS", "HTML", or "JSON" (the
              "JSON" type is only valid for the manifest file named
              "appsscript").
            - "source" (str): full text content.

    Returns:
        Confirmation listing the files after update (name + type per
        entry).
    """
    return await _update_script_content_impl(
        service, user_google_email, script_id, files
    )


async def _run_script_function_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    function_name: str,
    parameters: Optional[list[object]] = None,
    dev_mode: bool = False,
) -> str:
    """Internal implementation for run_script_function."""
    logger.info(
        f"[run_script_function] Email: {user_google_email}, ID: {script_id}, Function: {function_name}"
    )

    request_body = {"function": function_name, "devMode": dev_mode}

    if parameters:
        request_body["parameters"] = parameters

    try:
        response = await asyncio.to_thread(
            service.scripts().run(scriptId=script_id, body=request_body).execute
        )

        if "error" in response:
            error_details = response["error"]
            error_message = error_details.get("message", "Unknown error")
            return (
                f"Execution failed\nFunction: {function_name}\nError: {error_message}"
            )

        result = response.get("response", {}).get("result")
        output = [
            "Execution successful",
            f"Function: {function_name}",
            f"Result: {result}",
        ]

        logger.info(f"[run_script_function] Successfully executed {function_name}")
        return "\n".join(output)

    except Exception as e:
        logger.error(f"[run_script_function] Execution error: {str(e)}")
        return f"Execution failed\nFunction: {function_name}\nError: {str(e)}"


@server.tool()
@handle_http_errors("run_script_function", service_type="script")
@require_google_service("script", "script_projects")
async def run_script_function(
    service: Any,
    user_google_email: str,
    script_id: str,
    function_name: str,
    parameters: Optional[list[object]] = None,
    dev_mode: bool = False,
) -> str:
    """Execute a function inside an Apps Script project.

    Requires the project to have an API-executable deployment configured
    (Deploy > New deployment > Library/API executable). dev_mode=True
    runs the latest saved code without needing a fresh deployment —
    available only to the script's OWN Google account. For managing
    deployments themselves use manage_deployment. Requires the
    script.projects OAuth scope plus any scopes the target function
    needs.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID (must have an API-executable
            deployment).
        function_name: Name of the top-level function to invoke (e.g.
            "doWork"). Private/internal helpers cannot be called.
        parameters: Optional positional arguments to pass to the
            function. Must be JSON-serializable primitives or arrays.
        dev_mode: True runs the latest saved code (owner only); False
            (default) runs the deployed version.

    Returns:
        "Execution successful" with function name and result, or
        "Execution failed" with the error message.
    """
    return await _run_script_function_impl(
        service, user_google_email, script_id, function_name, parameters, dev_mode
    )


async def _create_deployment_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    description: str,
    version_description: Optional[str] = None,
) -> str:
    """Internal implementation for create_deployment.

    Creates a new version first, then creates a deployment using that version.
    """
    logger.info(
        f"[create_deployment] Email: {user_google_email}, ID: {script_id}, Desc: {description}"
    )

    # First, create a new version
    version_body = {"description": version_description or description}
    version = await asyncio.to_thread(
        service.projects()
        .versions()
        .create(scriptId=script_id, body=version_body)
        .execute
    )
    version_number = version.get("versionNumber")
    logger.info(f"[create_deployment] Created version {version_number}")

    # Now create the deployment with the version number
    deployment_body = {
        "versionNumber": version_number,
        "description": description,
    }

    deployment = await asyncio.to_thread(
        service.projects()
        .deployments()
        .create(scriptId=script_id, body=deployment_body)
        .execute
    )

    deployment_id = deployment.get("deploymentId", "Unknown")

    output = [
        f"Created deployment for script: {script_id}",
        f"Deployment ID: {deployment_id}",
        f"Version: {version_number}",
        f"Description: {description}",
    ]

    logger.info(f"[create_deployment] Created deployment {deployment_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("manage_deployment", service_type="script")
@require_google_service("script", "script_deployments")
async def manage_deployment(
    service: Any,
    user_google_email: str,
    action: str,
    script_id: str,
    deployment_id: Optional[str] = None,
    description: Optional[str] = None,
    version_description: Optional[str] = None,
) -> str:
    """Create, update, or delete an Apps Script deployment.

    Side effects: create first generates a new version then deploys it
    (two API calls atomically); delete is destructive. To list existing
    deployments use list_deployments. For executing code via the API use
    run_script_function. Requires the script.deployments OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        action: "create", "update", or "delete".
        script_id: Target project ID.
        deployment_id: Existing deployment ID. Required for update and
            delete (get from list_deployments).
        description: Deployment description shown in the Apps Script UI.
            Required for create and update.
        version_description: For create only — description attached to
            the auto-created version. Defaults to `description` when
            omitted.

    Returns:
        Confirmation with deployment details (ID, version, description)
        for create/update, or a deletion confirmation.
    """
    action = action.lower().strip()
    if action == "create":
        if description is None or description.strip() == "":
            raise ValueError("description is required for create action")
        return await _create_deployment_impl(
            service, user_google_email, script_id, description, version_description
        )
    elif action == "update":
        if not deployment_id:
            raise ValueError("deployment_id is required for update action")
        if description is None or description.strip() == "":
            raise ValueError("description is required for update action")
        return await _update_deployment_impl(
            service, user_google_email, script_id, deployment_id, description
        )
    elif action == "delete":
        if not deployment_id:
            raise ValueError("deployment_id is required for delete action")
        return await _delete_deployment_impl(
            service, user_google_email, script_id, deployment_id
        )
    else:
        raise ValueError(
            f"Invalid action '{action}'. Must be 'create', 'update', or 'delete'."
        )


async def _list_deployments_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Internal implementation for list_deployments."""
    logger.info(f"[list_deployments] Email: {user_google_email}, ID: {script_id}")

    response = await asyncio.to_thread(
        service.projects().deployments().list(scriptId=script_id).execute
    )

    deployments = response.get("deployments", [])

    if not deployments:
        return f"No deployments found for script: {script_id}"

    output = [f"Deployments for script: {script_id}", ""]

    for i, deployment in enumerate(deployments, 1):
        deployment_id = deployment.get("deploymentId", "Unknown")
        description = deployment.get("description", "No description")
        update_time = deployment.get("updateTime", "Unknown")

        output.append(f"{i}. {description} ({deployment_id})")
        output.append(f"   Updated: {update_time}")
        output.append("")

    logger.info(f"[list_deployments] Found {len(deployments)} deployments")
    return "\n".join(output)


@server.tool()
@handle_http_errors("list_deployments", is_read_only=True, service_type="script")
@require_google_service("script", "script_deployments_readonly")
async def list_deployments(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """List all deployments (versioned snapshots) of an Apps Script project.

    Deployments are how Apps Script exposes a script as a web app, API
    executable, add-on, or library. For managing deployments use
    manage_deployment; for code versions use list_versions. Requires the
    script.deployments.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID.

    Returns:
        Numbered list of deployments: description, deploymentId, and
        last update time per entry.
    """
    return await _list_deployments_impl(service, user_google_email, script_id)


async def _update_deployment_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    deployment_id: str,
    description: Optional[str] = None,
) -> str:
    """Internal implementation for update_deployment."""
    logger.info(
        f"[update_deployment] Email: {user_google_email}, Script: {script_id}, Deployment: {deployment_id}"
    )

    request_body = {}
    if description:
        request_body["description"] = description

    deployment = await asyncio.to_thread(
        service.projects()
        .deployments()
        .update(scriptId=script_id, deploymentId=deployment_id, body=request_body)
        .execute
    )

    output = [
        f"Updated deployment: {deployment_id}",
        f"Script: {script_id}",
        f"Description: {deployment.get('description', 'No description')}",
    ]

    logger.info(f"[update_deployment] Updated deployment {deployment_id}")
    return "\n".join(output)


async def _delete_deployment_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    deployment_id: str,
) -> str:
    """Internal implementation for delete_deployment."""
    logger.info(
        f"[delete_deployment] Email: {user_google_email}, Script: {script_id}, Deployment: {deployment_id}"
    )

    await asyncio.to_thread(
        service.projects()
        .deployments()
        .delete(scriptId=script_id, deploymentId=deployment_id)
        .execute
    )

    output = f"Deleted deployment: {deployment_id} from script: {script_id}"

    logger.info(f"[delete_deployment] Deleted deployment {deployment_id}")
    return output


async def _list_script_processes_impl(
    service: Any,
    user_google_email: str,
    page_size: int = 50,
    script_id: Optional[str] = None,
) -> str:
    """Internal implementation for list_script_processes."""
    logger.info(
        f"[list_script_processes] Email: {user_google_email}, PageSize: {page_size}"
    )

    request_params = {"pageSize": page_size}
    if script_id:
        request_params["scriptId"] = script_id

    response = await asyncio.to_thread(
        service.processes().list(**request_params).execute
    )

    processes = response.get("processes", [])

    if not processes:
        return "No recent script executions found."

    output = ["Recent script executions:", ""]

    for i, process in enumerate(processes, 1):
        function_name = process.get("functionName", "Unknown")
        process_status = process.get("processStatus", "Unknown")
        start_time = process.get("startTime", "Unknown")
        duration = process.get("duration", "Unknown")

        output.append(f"{i}. {function_name}")
        output.append(f"   Status: {process_status}")
        output.append(f"   Started: {start_time}")
        output.append(f"   Duration: {duration}")
        output.append("")

    logger.info(f"[list_script_processes] Found {len(processes)} processes")
    return "\n".join(output)


@server.tool()
@handle_http_errors("list_script_processes", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def list_script_processes(
    service: Any,
    user_google_email: str,
    page_size: int = 50,
    script_id: Optional[str] = None,
) -> str:
    """List recent Apps Script executions (across all scripts or one).

    Useful for debugging failed runs or auditing triggered executions.
    Reports function name, status, start time, and duration per process.
    Requires the script.processes.readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        page_size: Max processes to return. Default 50.
        script_id: Optional — scope to a single project's executions.
            Omit for all accessible scripts.

    Returns:
        Numbered list: function name, processStatus
        (COMPLETED/FAILED/TIMED_OUT/CANCELED/etc.), start time, and
        duration per process.
    """
    return await _list_script_processes_impl(
        service, user_google_email, page_size, script_id
    )


# ============================================================================
# Delete Script Project
# ============================================================================


async def _delete_script_project_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Internal implementation for delete_script_project."""
    logger.info(
        f"[delete_script_project] Email: {user_google_email}, ScriptID: {script_id}"
    )

    # Apps Script projects are stored as Drive files
    await asyncio.to_thread(service.files().delete(fileId=script_id).execute)

    logger.info(f"[delete_script_project] Deleted script {script_id}")
    return f"Deleted Apps Script project: {script_id}"


@server.tool()
@handle_http_errors("delete_script_project", is_read_only=False, service_type="drive")
@require_google_service("drive", "drive_full")
async def delete_script_project(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Permanently delete an Apps Script project (Drive-backed).

    Side effects: PERMANENTLY deletes the script project via the Drive
    API (Apps Script projects are stored as Drive files). No undo via
    API — Drive Trash may still allow restore for up to 30 days via the
    web UI. For disabling without deletion, remove deployments via
    manage_deployment instead. Requires the drive full OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target Apps Script project ID (same as the Drive
            file ID).

    Returns:
        Deletion confirmation with the deleted script ID.
    """
    return await _delete_script_project_impl(service, user_google_email, script_id)


# ============================================================================
# Version Management
# ============================================================================


async def _list_versions_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """Internal implementation for list_versions."""
    logger.info(f"[list_versions] Email: {user_google_email}, ScriptID: {script_id}")

    response = await asyncio.to_thread(
        service.projects().versions().list(scriptId=script_id).execute
    )

    versions = response.get("versions", [])

    if not versions:
        return f"No versions found for script: {script_id}"

    output = [f"Versions for script: {script_id}", ""]

    for version in versions:
        version_number = version.get("versionNumber", "Unknown")
        description = version.get("description", "No description")
        create_time = version.get("createTime", "Unknown")

        output.append(f"Version {version_number}: {description}")
        output.append(f"   Created: {create_time}")
        output.append("")

    logger.info(f"[list_versions] Found {len(versions)} versions")
    return "\n".join(output)


@server.tool()
@handle_http_errors("list_versions", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def list_versions(
    service: Any,
    user_google_email: str,
    script_id: str,
) -> str:
    """List all versions (immutable snapshots) of an Apps Script project.

    Versions are the snapshots that deployments reference. Created
    automatically when you deploy, or manually via create_version. For
    deployments use list_deployments. Requires the script.projects
    readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID.

    Returns:
        Formatted version list: versionNumber, description, createTime
        per entry.
    """
    return await _list_versions_impl(service, user_google_email, script_id)


async def _create_version_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    description: Optional[str] = None,
) -> str:
    """Internal implementation for create_version."""
    logger.info(f"[create_version] Email: {user_google_email}, ScriptID: {script_id}")

    request_body = {}
    if description:
        request_body["description"] = description

    version = await asyncio.to_thread(
        service.projects()
        .versions()
        .create(scriptId=script_id, body=request_body)
        .execute
    )

    version_number = version.get("versionNumber", "Unknown")
    create_time = version.get("createTime", "Unknown")

    output = [
        f"Created version {version_number} for script: {script_id}",
        f"Description: {description or 'No description'}",
        f"Created: {create_time}",
    ]

    logger.info(f"[create_version] Created version {version_number}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("create_version", is_read_only=False, service_type="script")
@require_google_service("script", "script_full")
async def create_version(
    service: Any,
    user_google_email: str,
    script_id: str,
    description: Optional[str] = None,
) -> str:
    """Create a new immutable version snapshot of an Apps Script project.

    Side effects: freezes the current code into a new versionNumber.
    Versions cannot be edited or deleted. A deployment can then be
    pinned to this version via manage_deployment. To list versions use
    list_versions. Requires the full script.projects OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID.
        description: Optional version note (e.g. release notes, ticket
            number).

    Returns:
        Confirmation with the new versionNumber, description, and
        createTime.
    """
    return await _create_version_impl(
        service, user_google_email, script_id, description
    )


async def _get_version_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    version_number: int,
) -> str:
    """Internal implementation for get_version."""
    logger.info(
        f"[get_version] Email: {user_google_email}, ScriptID: {script_id}, Version: {version_number}"
    )

    version = await asyncio.to_thread(
        service.projects()
        .versions()
        .get(scriptId=script_id, versionNumber=version_number)
        .execute
    )

    ver_num = version.get("versionNumber", "Unknown")
    description = version.get("description", "No description")
    create_time = version.get("createTime", "Unknown")

    output = [
        f"Version {ver_num} of script: {script_id}",
        f"Description: {description}",
        f"Created: {create_time}",
    ]

    logger.info(f"[get_version] Retrieved version {ver_num}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("get_version", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def get_version(
    service: Any,
    user_google_email: str,
    script_id: str,
    version_number: int,
) -> str:
    """
    Fetch metadata for a specific immutable version of a Google Apps Script
    project.

    A "version" in Apps Script is a snapshot of script code taken at a point
    in time — versions are referenced by deployments and cannot be edited
    after creation. Use this to inspect the description/create-time of one
    known version. For the full list of versions on a project, use
    `list_versions`. To create a new version from the current code, use
    `create_version`. To inspect deployments that point to versions, use
    `list_deployments`.

    Requires OAuth scope:
    `https://www.googleapis.com/auth/script.projects.readonly` (read-only)
    or the broader `script.projects` scope.

    Args:
        service: Injected Google API service client.
        user_google_email: User's email address. Required.
        script_id: The Apps Script project ID — the string after `/d/` in the
            `script.google.com/home/projects/<id>` URL, or the `scriptId`
            field returned by `create_script_project` / `list_script_projects`.
        version_number: The 1-indexed integer version number to retrieve
            (1 for the first version, 2 for the second, etc.). Must refer to
            an already-created version; requesting a non-existent number
            raises a `404 Not Found` handled by the error decorator.

    Returns:
        Multi-line string:
            Line 1: `Version <N> of script: <script_id>`
            Line 2: `Description: <description or "No description">`
            Line 3: `Created: <rfc3339 timestamp>`
    """
    return await _get_version_impl(
        service, user_google_email, script_id, version_number
    )


# ============================================================================
# Metrics
# ============================================================================


async def _get_script_metrics_impl(
    service: Any,
    user_google_email: str,
    script_id: str,
    metrics_granularity: str = "DAILY",
) -> str:
    """Internal implementation for get_script_metrics."""
    logger.info(
        f"[get_script_metrics] Email: {user_google_email}, ScriptID: {script_id}, Granularity: {metrics_granularity}"
    )

    request_params = {
        "scriptId": script_id,
        "metricsGranularity": metrics_granularity,
    }

    response = await asyncio.to_thread(
        service.projects().getMetrics(**request_params).execute
    )

    output = [
        f"Metrics for script: {script_id}",
        f"Granularity: {metrics_granularity}",
        "",
    ]

    # Active users
    active_users = response.get("activeUsers", [])
    if active_users:
        output.append("Active Users:")
        for metric in active_users:
            start_time = metric.get("startTime", "Unknown")
            end_time = metric.get("endTime", "Unknown")
            value = metric.get("value", "0")
            output.append(f"  {start_time} to {end_time}: {value} users")
        output.append("")

    # Total executions
    total_executions = response.get("totalExecutions", [])
    if total_executions:
        output.append("Total Executions:")
        for metric in total_executions:
            start_time = metric.get("startTime", "Unknown")
            end_time = metric.get("endTime", "Unknown")
            value = metric.get("value", "0")
            output.append(f"  {start_time} to {end_time}: {value} executions")
        output.append("")

    # Failed executions
    failed_executions = response.get("failedExecutions", [])
    if failed_executions:
        output.append("Failed Executions:")
        for metric in failed_executions:
            start_time = metric.get("startTime", "Unknown")
            end_time = metric.get("endTime", "Unknown")
            value = metric.get("value", "0")
            output.append(f"  {start_time} to {end_time}: {value} failures")
        output.append("")

    if not active_users and not total_executions and not failed_executions:
        output.append("No metrics data available for this script.")

    logger.info(f"[get_script_metrics] Retrieved metrics for {script_id}")
    return "\n".join(output)


@server.tool()
@handle_http_errors("get_script_metrics", is_read_only=True, service_type="script")
@require_google_service("script", "script_readonly")
async def get_script_metrics(
    service: Any,
    user_google_email: str,
    script_id: str,
    metrics_granularity: str = "DAILY",
) -> str:
    """Fetch execution analytics (users, runs, failures) for a script.

    Returns time-series metrics over the last 7 days (DAILY) or 8 weeks
    (WEEKLY). Useful for adoption audits and error monitoring. For
    per-execution details use list_script_processes. Requires the
    script.metrics readonly OAuth scope.

    Args:
        user_google_email: The user's Google email address (authenticated
            account).
        script_id: Target project ID.
        metrics_granularity: "DAILY" (last 7 days) or "WEEKLY" (last 8
            weeks). Default "DAILY".

    Returns:
        Formatted block with three sections — Active Users, Total
        Executions, Failed Executions — each showing start/end time and
        count per bucket. "No metrics data available" when the script
        has no usage in the window.
    """
    return await _get_script_metrics_impl(
        service, user_google_email, script_id, metrics_granularity
    )


# ============================================================================
# Trigger Code Generation
# ============================================================================


def _generate_trigger_code_impl(
    trigger_type: str,
    function_name: str,
    schedule: str = "",
) -> str:
    """Internal implementation for generate_trigger_code."""
    code_lines = []

    if trigger_type == "on_open":
        code_lines = [
            "// Simple trigger - just rename your function to 'onOpen'",
            "// This runs automatically when the document is opened",
            "function onOpen(e) {",
            f"  {function_name}();",
            "}",
        ]
    elif trigger_type == "on_edit":
        code_lines = [
            "// Simple trigger - just rename your function to 'onEdit'",
            "// This runs automatically when a user edits the spreadsheet",
            "function onEdit(e) {",
            f"  {function_name}();",
            "}",
        ]
    elif trigger_type == "time_minutes":
        interval = schedule or "5"
        code_lines = [
            "// Run this function ONCE to install the trigger",
            f"function createTimeTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            f"  // Create new trigger - runs every {interval} minutes",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .timeBased()",
            f"    .everyMinutes({interval})",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run every {interval} minutes');",
            "}",
        ]
    elif trigger_type == "time_hours":
        interval = schedule or "1"
        code_lines = [
            "// Run this function ONCE to install the trigger",
            f"function createTimeTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            f"  // Create new trigger - runs every {interval} hour(s)",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .timeBased()",
            f"    .everyHours({interval})",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run every {interval} hour(s)');",
            "}",
        ]
    elif trigger_type == "time_daily":
        hour = schedule or "9"
        code_lines = [
            "// Run this function ONCE to install the trigger",
            f"function createDailyTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            f"  // Create new trigger - runs daily at {hour}:00",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .timeBased()",
            f"    .atHour({hour})",
            "    .everyDays(1)",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run daily at {hour}:00');",
            "}",
        ]
    elif trigger_type == "time_weekly":
        day = schedule.upper() if schedule else "MONDAY"
        code_lines = [
            "// Run this function ONCE to install the trigger",
            f"function createWeeklyTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            f"  // Create new trigger - runs weekly on {day}",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .timeBased()",
            f"    .onWeekDay(ScriptApp.WeekDay.{day})",
            "    .atHour(9)",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run every {day} at 9:00');",
            "}",
        ]
    elif trigger_type == "on_form_submit":
        code_lines = [
            "// Run this function ONCE to install the trigger",
            "// This must be run from a script BOUND to the Google Form",
            f"function createFormSubmitTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            "  // Create new trigger - runs when form is submitted",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .forForm(FormApp.getActiveForm())",
            "    .onFormSubmit()",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run on form submit');",
            "}",
        ]
    elif trigger_type == "on_change":
        code_lines = [
            "// Run this function ONCE to install the trigger",
            "// This must be run from a script BOUND to a Google Sheet",
            f"function createChangeTrigger_{function_name}() {{",
            "  // Delete existing triggers for this function first",
            "  const triggers = ScriptApp.getProjectTriggers();",
            "  triggers.forEach(trigger => {",
            f"    if (trigger.getHandlerFunction() === '{function_name}') {{",
            "      ScriptApp.deleteTrigger(trigger);",
            "    }",
            "  });",
            "",
            "  // Create new trigger - runs when spreadsheet changes",
            f"  ScriptApp.newTrigger('{function_name}')",
            "    .forSpreadsheet(SpreadsheetApp.getActive())",
            "    .onChange()",
            "    .create();",
            "",
            f"  Logger.log('Trigger created: {function_name} will run on spreadsheet change');",
            "}",
        ]
    else:
        return (
            f"Unknown trigger type: {trigger_type}\n\n"
            "Valid types: time_minutes, time_hours, time_daily, time_weekly, "
            "on_open, on_edit, on_form_submit, on_change"
        )

    code = "\n".join(code_lines)

    instructions = []
    if trigger_type.startswith("on_"):
        if trigger_type in ("on_open", "on_edit"):
            instructions = [
                "SIMPLE TRIGGER",
                "=" * 50,
                "",
                "Add this code to your script. Simple triggers run automatically",
                "when the event occurs - no setup function needed.",
                "",
                "Note: Simple triggers have limitations:",
                "- Cannot access services that require authorization",
                "- Cannot run longer than 30 seconds",
                "- Cannot make external HTTP requests",
                "",
                "For more capabilities, use an installable trigger instead.",
                "",
                "CODE TO ADD:",
                "-" * 50,
            ]
        else:
            instructions = [
                "INSTALLABLE TRIGGER",
                "=" * 50,
                "",
                "1. Add this code to your script",
                f"2. Run the setup function once: createFormSubmitTrigger_{function_name}() or similar",
                "3. The trigger will then run automatically",
                "",
                "CODE TO ADD:",
                "-" * 50,
            ]
    else:
        instructions = [
            "INSTALLABLE TRIGGER",
            "=" * 50,
            "",
            "1. Add this code to your script using update_script_content",
            "2. Run the setup function ONCE (manually in Apps Script editor or via run_script_function)",
            "3. The trigger will then run automatically on schedule",
            "",
            "To check installed triggers: Apps Script editor > Triggers (clock icon)",
            "",
            "CODE TO ADD:",
            "-" * 50,
        ]

    return "\n".join(instructions) + "\n\n" + code


@server.tool()
async def generate_trigger_code(
    trigger_type: str,
    function_name: str,
    schedule: str = "",
) -> str:
    """
    Generates Apps Script code for creating triggers.

    The Apps Script API cannot create triggers directly - they must be created
    from within Apps Script itself. This tool generates the code you need.

    Args:
        trigger_type: Type of trigger. One of:
                      - "time_minutes" (run every N minutes: 1, 5, 10, 15, 30)
                      - "time_hours" (run every N hours: 1, 2, 4, 6, 8, 12)
                      - "time_daily" (run daily at a specific hour: 0-23)
                      - "time_weekly" (run weekly on a specific day)
                      - "on_open" (simple trigger - runs when document opens)
                      - "on_edit" (simple trigger - runs when user edits)
                      - "on_form_submit" (runs when form is submitted)
                      - "on_change" (runs when content changes)

        function_name: The function to run when trigger fires (e.g., "sendDailyReport")

        schedule: Schedule details (depends on trigger_type):
                  - For time_minutes: "1", "5", "10", "15", or "30"
                  - For time_hours: "1", "2", "4", "6", "8", or "12"
                  - For time_daily: hour as "0"-"23" (e.g., "9" for 9am)
                  - For time_weekly: "MONDAY", "TUESDAY", etc.
                  - For simple triggers (on_open, on_edit): not needed

    Returns:
        str: Apps Script code to create the trigger
    """
    return _generate_trigger_code_impl(trigger_type, function_name, schedule)
