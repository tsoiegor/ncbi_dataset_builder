# Support utilities

The `ncbi_dataset_builder.support` subpackage contains external-command,
progress, unit-log, hashing, atomic-write, locking, and filesystem helpers used
by the public layers.

Most users do not need these APIs. They are documented for custom providers,
custom processors, transport injection, and maintenance.

## Module map

| Module | Contents |
| --- | --- |
| `commands.py` | `CommandRunner` |
| `progress.py` | `ProgressReporter`, `ProgressTask`, and context-aware visibility |
| `unit_logging.py` | Per-unit log contexts and stdout/stderr routing |
| `util.py` | Units, hashes, atomic writes, JSON, locks, timestamps, and file validation |
| `__init__.py` | No re-export surface; import helpers from their modules |

# External commands

## `CommandRunner`

```python
CommandRunner(
    *,
    base_env=None,
)
```

| Argument | Meaning |
| --- | --- |
| `base_env: Mapping[str, str] | None` | Environment values added to every child process. Per-call `env` values override them. |

Commands are executed as argument sequences without a shell.

### Methods

| Method | Arguments and result |
| --- | --- |
| `which(executable)` | Return the resolved executable path or `None`. |
| `require(*executables)` | Raise `ExternalToolError` listing unavailable commands. |
| `version(executable, *arguments)` | Run a version command and return its first output line. |

#### `run(command, *, ...)`

```python
run(
    command,
    *,
    cwd=None,
    env=None,
    timeout=None,
    capture_output=True,
    text=True,
    check=True,
    stdout=None,
    stderr=None,
) -> subprocess.CompletedProcess
```

| Argument | Meaning |
| --- | --- |
| `command: Sequence[str | PathLike]` | Executable and arguments. Empty commands are rejected. |
| `cwd: Path | None` | Child working directory. |
| `env: Mapping[str, str] | None` | Additional/overriding environment values. |
| `timeout: float | None` | Process timeout in seconds. |
| `capture_output: bool` | Capture stdout/stderr when explicit streams are absent. |
| `text: bool` | Return decoded strings instead of bytes. |
| `check: bool` | Raise `ExternalToolError` on non-zero status. |
| `stdout`, `stderr` | Optional explicit subprocess streams. |

```python
from ncbi_dataset_builder.support.commands import CommandRunner

runner = CommandRunner(base_env={"OMP_NUM_THREADS": "4"})
runner.require("my-tool")

completed = runner.run(
    ["my-tool", "--input", str(input_path), "--threads", "4"],
    cwd=work_dir,
)
print(completed.stdout)
```

# Progress

## `ProgressReporter`

```python
ProgressReporter(
    *,
    enabled=True,
    use_bars=True,
    stream=None,
    text_interval_seconds=5.0,
)
```

| Argument | Meaning |
| --- | --- |
| `enabled: bool` | Enable direct console/bar display. Standard log records are still emitted. |
| `use_bars: bool` | Use optional tqdm bars when available. |
| `stream: TextIO | None` | Fallback text destination; defaults to stderr behavior. |
| `text_interval_seconds: float` | Minimum positive interval between fallback task updates. |

| Method | Arguments and result |
| --- | --- |
| `message(message, *, level=logging.INFO)` | Log and optionally display one event. |
| `minimum_level(level)` | Context manager that suppresses direct display below `level` while retaining logs. |
| `cache_summary(description, *, cached, missing, unit="items")` | Report reusable and missing counts. |
| `network_summary(description, *, cached, to_fetch, unit="request chunks")` | Report cache/network work. |
| `task(description, *, total=None, unit="items")` | Create `ProgressTask`. |
| `track(items, description, *, total=None, unit="items")` | Yield an iterable while advancing a task. |

`get_progress(progress)` returns the supplied reporter or a shared disabled
reporter. `progress_level_enabled(level)` checks the current context-local
display threshold.

## `ProgressTask`

```python
ProgressTask(
    reporter,
    description,
    *,
    total,
    unit,
)
```

| Argument | Meaning |
| --- | --- |
| `reporter` | Owning `ProgressReporter`. |
| `description` | Human-readable operation name. |
| `total: float | None` | Expected amount, or unknown. |
| `unit: str` | Label such as `"items"`, `"GB"`, or `"samples"`. |

`update(amount=1)` advances by a non-negative amount.
`close(status="complete")` emits the final state once. As a context manager,
the task closes with `"complete"` or `"failed"` according to the exception
state.

```python
with progress.task("Hash files", total=len(paths), unit="files") as task:
    for path in paths:
        hash_one(path)
        task.update()
```

# Unit logging

`install_unit_logging()` installs context-aware package logging and
stdout/stderr proxies once. `DatasetBuilder` calls it during construction.

## Public module functions

| Function | Arguments and behavior |
| --- | --- |
| `unit_log(path, *, phase, unit_id, fsync=True)` | Context manager that appends a timestamped phase to the durable unit log and yields the path. |
| `current_unit_log_path()` | Active path or `None` outside a unit context. |
| `current_unit_log_handle()` | Active text handle, used for direct subprocess stderr routing. |
| `write_unit_output(label, value)` | Append optional string/bytes command output under a label. |

Repeated staging, processing, error, and retry phases append to the same unit
file. `fsync=True` synchronizes it when the context exits.

## Private logging classes

| Class | Role |
| --- | --- |
| `_UnitLogSession(path, handle, fsync, lock)` | Active context state; `write(value)` appends/flushes and `close()` optionally fsyncs. |
| `_UnitLogHandler` | Logging handler whose `emit(record)` routes records to the active session. |
| `_ContextStream` | stdout/stderr proxy implementing `write`, `flush`, `isatty`, `fileno`, `encoding`, and `errors`. |

These underscore-prefixed classes are implementation details.

# Filesystem and serialization helpers

## Units and identifiers

| Function | Behavior |
| --- | --- |
| `bytes_to_gb(value)` | Convert bytes to decimal GB using 1,000,000,000 bytes per GB. |
| `gb_to_bytes(value)` | Convert decimal GB to integer bytes. |
| `sanitize_identifier(value)` | Replace unsafe filename characters and require a non-empty safe identifier. |
| `utc_timestamp()` | Return current UTC as an ISO-8601 string. |
| `existing_nonempty(path)` | True only for an existing regular file with size greater than zero. |

## Hashing

```python
sha256_file(
    path,
    chunk_size=8 * 1024 * 1024,
    *,
    progress=None,
) -> str
```

`path` is streamed in chunks; optional progress reports decimal GB.

## Atomic writes

| Function | Behavior |
| --- | --- |
| `atomic_write_bytes(path, data)` | Replace the file only when bytes changed; return whether replacement occurred. |
| `atomic_write_text(path, text)` | UTF-8 wrapper around atomic byte writing. |
| `atomic_write_json(path, value)` | Stable JSON serialization and atomic write; return whether changed. |
| `read_json(path)` | Read/decode UTF-8 JSON. |

Atomic writers create parent directories, use a same-directory temporary file,
flush and fsync it, then replace the destination.

## `exclusive_file_lock(...)`

```python
exclusive_file_lock(
    path,
    *,
    timeout_seconds=120.0,
    stale_after_seconds=24 * 60 * 60,
    heartbeat_seconds=None,
)
```

| Argument | Meaning |
| --- | --- |
| `path: Path` | Normal lock file, suitable for a shared filesystem. |
| `timeout_seconds: float` | Maximum acquisition wait. |
| `stale_after_seconds: float` | Age after which ownership may be reclaimed. |
| `heartbeat_seconds: float | None` | Optional interval that refreshes long-running ownership. |

The context records PID, host, creation time, and optional heartbeat. Local
ownership checks use process liveness where possible. Callers use narrow lock
paths for workspace config, queue coordination, downloads, genomes, state, and
publication.

# Export status

Only `ProgressReporter` and `ProgressTask` are exported from the package
root. Other helpers are available from their modules but should be treated as
advanced API unless their behavior is essential to a custom component.
