# Workspace and runtime data

The `ncbi_dataset_builder.workspace` subpackage owns the visible directory
layout, stable workspace semantics, execution snapshots, and the public
artifact manifest.

A workspace is durable state. Use a persistent local or shared path, not a
temporary directory. The same layout is used by all three
[execution systems](../../../docs/ExecutionSystems.md).

## Module map

| Module | Contents |
| --- | --- |
| `store.py` | `WorkspaceConfig`, `WorkspaceStore`, lazy runtime paths, execution records, and root manifest |
| `__init__.py` | Public workspace export list |

## Directory layout

`WorkspaceStore` creates runtime directories lazily. The state store is the
only subsystem initialized with the builder itself.

| Path | Ownership and purpose |
| --- | --- |
| `manifest.json` | Public, portable artifact index |
| `output/<unit>/` | Processor-owned files; this root is configurable |
| `runtime/workspace.json` | Stable schema, grouping, output root, and genome policy |
| `runtime/catalogs/` | Cached and selected RunInfo tables |
| `runtime/metadata/` | Normalized metadata and local metadata inventory |
| `runtime/metadata_cache/` | Raw reusable NCBI responses |
| `runtime/fastq/` | Provider-owned bounded input |
| `runtime/genomes/` | Genome FASTAs, lockfile, and reusable indexes |
| `runtime/state/` | Atomic unit state, retry history, and locks |
| `runtime/executions/` | Immutable automatic execution snapshots |
| `runtime/slurm/` | Generated coordinator and distributed sample scripts |
| `runtime/logs/` | Per-unit and Slurm logs |

# `WorkspaceConfig`

Frozen serialized value:

```python
WorkspaceConfig(
    schema_version,
    created_at,
    group_by,
    output_dir,
    genome_policy={},
    directories={},
)
```

| Field | Meaning |
| --- | --- |
| `schema_version: int` | Workspace metadata schema. New workspaces use version 2. |
| `created_at: str` | UTC ISO-8601 initialization timestamp. |
| `group_by: str` | Processing-unit entity: run, experiment, SRA Sample, or BioSample. |
| `output_dir: str` | Absolute processor-owned output root. |
| `genome_policy: dict` | Serialized stable assembly-selection policy. |
| `directories: dict[str, str]` | Human-readable directory role map. |

| Method | Behavior |
| --- | --- |
| `to_dict()` | Serialize all fields. |
| `from_dict(value)` | Restore fields, with compatible defaults for optional mappings. |

# `WorkspaceStore`

```python
WorkspaceStore(root, output_dir=None)
```

`root: Path` is created immediately; runtime subdirectories are lazy.
`output_dir` defaults to `root / "output"`. Construction exposes:

| Attribute | Value |
| --- | --- |
| `root` | Normalized workspace root |
| `runtime` | `root / "runtime"` for new workspaces |
| `output` | Configured output root |
| `config_path` | `runtime / "workspace.json"` |
| `manifest_path` | `root / "manifest.json"` |
| `executions` | `runtime / "executions"` |
| `RUNTIME_DIRECTORIES` | Class-level runtime-directory role mapping |

## `configure(*, group_by, genome_policy) -> WorkspaceConfig`

| Argument | Meaning |
| --- | --- |
| `group_by: str` | Stable processing-unit level. |
| `genome_policy: dict` | Serialized selection policy. |

The method atomically creates `workspace.json` or validates the existing
configuration under a lock. If unit state exists, changing grouping, output
root, or genome policy raises instead of silently reinterpreting prior work.

## Execution-record methods

| Method | Arguments and result |
| --- | --- |
| `save_execution(execution)` | Atomically write `executions/<execution_id>.json`; return its path. |
| `load_execution(path_or_id)` | Load an explicit JSON path or resolve an ID below `executions/`; return `ExecutionRecord`. |
| `latest_execution()` | Load the execution JSON with the latest modification time. |
| `all_executions()` | Load every execution ordered by creation time and identifier. |

Execution records capture queue items, processor identity, catalog audit,
execution/queue configs, grouping, query provenance, and creation time.

## `sync_manifest(execution, states) -> Path`

`states` maps unit IDs to state dictionaries or `None`. The method writes a
root manifest containing:

- update time;
- latest execution ID;
- grouping;
- processor identity; and
- retained entries from earlier executions; and
- one refreshed entry per requested unit with its catalog identity, status,
  phase, attempts, output directory, execution and processor identity, declared
  artifacts, and relevant provenance.

A later subset execution therefore updates its units without deleting other
experiments already indexed in the workspace.

The returned path is `workspace/manifest.json`.

# Dataset shaping

There is no publication layer. A processor controls every file below its unit
output directory and declares durable artifacts by role. The manifest stores
portable artifact paths, sizes, and checksums. Independent scripts can use it
to consolidate BigWigs, descriptions, genomes, or any other desired layout.

# Resume and cleanup boundaries

| Mechanism | Owns |
| --- | --- |
| Unit state | Status, semantic fingerprint, declared output hashes/metadata, genome and FASTQ provenance |
| Queue cleanup | Provider-owned roots below `workspace/runtime/fastq/` |
| Processor retention | Everything below the assigned unit output directory |

Queue cleanup failure does not turn a validated success into failure; inspect
unit logs when input retention is unexpected.
