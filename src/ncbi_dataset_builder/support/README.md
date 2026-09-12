# `support`

This internal subpackage holds reusable infrastructure. Application users
normally reach it through `DatasetBuilder`, acquisition, or processors.

`CommandRunner(base_env=None)` adds optional environment values to every child
process. It resolves tools with `which(executable)`,
checks requirements with `require(*executables)`, executes argument lists with
`run(command, cwd=None, env=None, timeout=None, capture_output=True, text=True,
check=True, stdout=None, stderr=None)`, and obtains one-line versions through
`version(executable, *arguments)`. Acquisition and ATAC processing share it.

`ProgressReporter(enabled=True, use_bars=True, stream=None,
text_interval_seconds=5)` provides `message(message, level=...)`, the
`minimum_level(level)` context manager, `cache_summary(...)`,
`network_summary(...)`, `task(description, total=None, unit="items")`, and
`track(items, description, total=None, unit="items")`.
`cache_summary(description, cached, missing, unit="items")` reports local
reuse; `network_summary(description, cached, to_fetch,
unit="request chunks")` reports HTTP work. A `ProgressTask` supports
`update(amount=1)` and `close(status="complete")`. The builder passes one
reporter through clients and processors so nested work is coherent.

`unit_logging.py` attaches Python logging plus subprocess stdout/stderr to one
sample log. `util.py` contains atomic writes, SHA-256 streaming, safe identifier
normalization, timestamping, and heartbeat file locks. These functions are
implementation details and intentionally do not create another public API.
