# Contributing to ve-gws

Thanks for your interest. ve-gws is a fork of [`taylorwilsdon/google_workspace_mcp`](https://github.com/taylorwilsdon/google_workspace_mcp) that adds 28 authoring-focused tools. It's part of the [`ve-*` framework family](https://github.com/HuntsDesk/ve-kit).

## If you want to report a bug

Open an [issue](https://github.com/HuntsDesk/ve-gws/issues) with:
- **Is the bug ve-gws-specific or upstream?** Try reproducing against [taylorwilsdon/google_workspace_mcp](https://github.com/taylorwilsdon/google_workspace_mcp) — if the same bug happens there, file upstream instead. We periodically merge upstream fixes.
- Which tool (e.g., `apply_continuous_numbering`, `create_slides_shape`)
- Which Python version + OS
- What you expected vs what happened
- Reproducible steps + any error messages

## If you want to propose a change

**Small fixes** (typos, doc errors, obvious bugs in ve-gws-specific tools): PR welcome. Target `main`.

**Extending a ve-gws-specific tool** (the 28 tools in the Docs/Slides/Sheets/Drive categories — see [README.md](./README.md)): PR welcome. Include tests — ve-gws has an 820+ test suite and PRs without tests for touched code usually get bounced.

**Fixing upstream behavior**: please send that PR to [taylorwilsdon/google_workspace_mcp](https://github.com/taylorwilsdon/google_workspace_mcp) instead. We'll pick up the fix on the next upstream merge.

**Porting features from [blakesplay/apollo](https://github.com/blakesplay/apollo)**: apollo is TypeScript; ve-gws is Python. Ports are manual semantic translations. Open an issue first to discuss which feature + why, so the maintainer can weigh it vs the upstream-merge cadence.

## Development setup

```bash
git clone https://github.com/HuntsDesk/ve-gws.git
cd ve-gws
uv sync                          # installs all deps
uv run pytest tests/             # full suite (~820 tests, <5s)
uv run python main.py            # run the server locally
```

## Code style

- Follow existing patterns in the module you're editing (consistency > personal preference)
- `asyncio.to_thread` for blocking `googleapiclient` calls
- Decorator stack: `@server.tool()` + `@handle_http_errors(...)` + `@require_google_service(...)` for new tool functions
- Unit tests for pure-Python helpers; leave integration tests manual unless you have a test Google account configured

## Upstream tracking

ve-gws pulls from `taylorwilsdon/google_workspace_mcp` periodically. If your PR touches `gslides/`, `gdocs/`, `gsheets/`, `gdrive/`, or `core/tool_tiers.yaml`, expect occasional merge conflicts during upstream pulls — the maintainer resolves these in favor of keeping ve-gws's enhanced tools working.

## Code of conduct

Be kind. Assume good faith. Disagree about ideas, not people.

## Related

- [`HuntsDesk/ve-kit`](https://github.com/HuntsDesk/ve-kit) — productivity kit (companion repo)
- [`taylorwilsdon/google_workspace_mcp`](https://github.com/taylorwilsdon/google_workspace_mcp) — upstream fork parent
- [`blakesplay/apollo`](https://github.com/blakesplay/apollo) — source of many of ve-gws's enhanced tool ideas (TypeScript, ported manually)
- [`piotr-agier/google-drive-mcp`](https://github.com/piotr-agier/google-drive-mcp) — Apollo was originally based on this earlier Google Drive MCP; acknowledging the full attribution chain
