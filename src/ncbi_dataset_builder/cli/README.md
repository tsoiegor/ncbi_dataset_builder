# `cli`

The CLI is a thin mapping onto the Python API. It stores no independent
configuration and uses the same workspace execution records as
`DatasetBuilder`.

Install the project first (see the root [installation
guide](../../../README.md#installation)), then use either `ncbi-dataset` or
`python -m ncbi_dataset_builder`.

## Shared arguments

Every command requires `--workspace PATH`. `--email` and `--ncbi-api-key`
default to `NCBI_EMAIL` and `NCBI_API_KEY`; `--group-by` accepts `run`,
`experiment`, `sra_sample`, or `biosample`; and `--prefetch-max-size` accepts an
SRA Toolkit value such as `100G` or `u`.

Processing commands select exactly one catalog source: `--catalog PATH` loads
CSV while `--query EXPR` fetches NCBI (and may use `--refresh`). Their queue
flags are `--download-workers`, `--max-inflight-gb`,
`--processing-storage-multiplier`, `--keep-inputs`,
`--discard-failed-inputs`, and `--retry-failed`; these map directly to
[`QueuePolicy`](../execution/README.md).

## Commands

### `fetch-catalog`

```text
ncbi-dataset fetch-catalog --workspace PATH --query EXPR --output CSV
```

Fetches SRA RunInfo. Direct NCBI access requires an email. `--refresh` bypasses
the query cache.

### `build-local`

```text
ncbi-dataset build-local --workspace PATH (--catalog CSV | --query EXPR)
  --processor MODULE:CALLABLE --total-cpus N
  [--min-cpus-per-job N] [--max-cpus-per-job N]
  [--max-running-jobs N] [--reserve-free-gb GB]
```

Runs in the current process. The CPU total is required; minimum CPUs and
concurrency default to one, maximum CPUs defaults to the total, and filesystem
reserve defaults to zero. There is no local memory flag. A sample failure makes
the command return exit code 1.

### `submit-single-node`

```text
ncbi-dataset submit-single-node --workspace PATH (--catalog CSV | --query EXPR)
  --processor MODULE:CALLABLE --allocation-cpus N
  --allocation-memory-gb GB --allocation-time-limit TIME
  --memory-gb-per-job GB --quota-gb GB [resource and queue options]
```

Optional resource fields include `--min-cpus-per-job`,
`--max-cpus-per-job`, and `--max-running-jobs`. The command requests one Slurm
allocation and runs a streaming queue inside it.

### `submit-distributed`

```text
ncbi-dataset submit-distributed --workspace PATH (--catalog CSV | --query EXPR)
  --processor MODULE:CALLABLE --total-cpu-quota N --max-running-jobs N
  --cpus-per-node N --min-cpus-per-job N --max-cpus-per-job N
  --memory-gb-per-job GB --worker-time-limit TIME --quota-gb GB
```

Optional coordinator fields are `--coordinator-cpus` (default 1),
`--coordinator-memory-gb` (default 4), and `--coordinator-time-limit` (default
seven days). A coordinator submits one importable processor job per sample.

Both Slurm commands accept `--partition`, `--account`, `--qos`,
`--quota-reserve-gb`, `--quota-usage-root`, `--script-path`, and `--no-submit`.
Quota settings model writable user capacity, not shared-filesystem free space.

### `status` and `publish`

```text
ncbi-dataset status --workspace PATH [--execution-id ID]
ncbi-dataset publish --workspace PATH [--destination PATH]
  [--execution-id ID] [--mode auto|hardlink|copy] [--overwrite]
```

`status` selects the latest execution when no ID is supplied. `publish` exports
verified experiment outputs and refuses to replace a separate destination
unless `--overwrite` is present.

## Implementation entry point

`main(argv=None)` parses an optional argument list and returns a process exit
code. It constructs `BuilderConfig`, one execution-system object, and
`QueuePolicy`, then calls the corresponding `DatasetBuilder` method. It stores
no parallel CLI-only state. See the full [command-line
reference](../../../docs/CommandLineInterface.md) for runnable examples and
every default.
