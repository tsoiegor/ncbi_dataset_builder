# Workspace and publication

The `ncbi_dataset_builder.workspace` subpackage owns the visible directory
layout, stable workspace semantics, execution snapshots, manifests, and compact
dataset publication.

A workspace is durable state. Use a persistent local or shared path, not a
temporary directory. The same layout is used by all three
[execution systems](../../../docs/ExecutionSystems.md).

## Module map

| Module | Contents |
| --- | --- |
| `store.py` | `WorkspaceConfig`, `WorkspaceStore`, directory creation, execution records, and root manifest |
| `publishing.py` | `PublishMode`, `DatasetExport`, and `DatasetPublisher` |
| `__init__.py` | Public workspace export list |

## Directory layout

`WorkspaceStore` creates every directory during construction.

| Path | Ownership and purpose |
| --- | --- |
| `workspace.json` | Stable schema, grouping, description profile, genome policy, and directory roles |
| `manifest.json` | Latest execution/unit view and optional in-place published-dataset manifest |
| `bigWig/` | In-place published experiment BigWigs |
| `descriptions/` | In-place published experiment descriptions |
| `genomes/` | Genome FASTAs, assembly lockfile, reusable indexes, and in-place published genomes |
| `catalogs/` | Cached and selected RunInfo tables |
| `metadata/` | Normalized metadata, cache index, and experiment descriptions |
| `metadata_cache/` | Raw reusable NCBI responses |
| `fastq/` | Provider-owned bounded input; queue cleanup is allowed only below this boundary |
| `work/` | Processor/download intermediates and publication staging |
| `outputs/` | Processor output roots by unit |
| `state/` | Atomic unit state and coordination locks |
| `executions/` | Immutable automatic execution snapshots |
| `slurm/` | Generated coordinator and distributed sample scripts |
| `logs/` | Per-unit and Slurm logs |

# `WorkspaceConfig`

Frozen serialized value:

```python
WorkspaceConfig(
    schema_version,
    created_at,
    group_by,
    description_profile,
    genome_policy={},
    directories={},
)
```

| Field | Meaning |
| --- | --- |
| `schema_version: int` | Workspace metadata schema. Current code writes version 1. |
| `created_at: str` | UTC ISO-8601 initialization timestamp. |
| `group_by: str` | Processing-unit entity: run, experiment, SRA Sample, or BioSample. |
| `description_profile: str` | Compact training or full description projection. |
| `genome_policy: dict` | Serialized stable assembly-selection policy. |
| `directories: dict[str, str]` | Human-readable directory role map. |

| Method | Behavior |
| --- | --- |
| `to_dict()` | Serialize all fields. |
| `from_dict(value)` | Restore fields, with compatible defaults for optional mappings. |

# `WorkspaceStore`

```python
WorkspaceStore(root)
```

`root: Path` is created along with all managed directories. Construction
exposes:

| Attribute | Value |
| --- | --- |
| `root` | Normalized workspace root |
| `config_path` | `root / "workspace.json"` |
| `manifest_path` | `root / "manifest.json"` |
| `executions` | `root / "executions"` |
| `DIRECTORIES` | Class-level directory role mapping |

## `configure(*, group_by, description_profile, genome_policy) -> WorkspaceConfig`

| Argument | Meaning |
| --- | --- |
| `group_by: str` | Stable processing-unit level. |
| `description_profile: str` | Stable description projection. |
| `genome_policy: dict` | Serialized selection policy. |

The method atomically creates `workspace.json` or validates the existing
configuration under a lock. If unit state exists, changing any of these three
semantic settings raises instead of silently reinterpreting prior work.

## Execution-record methods

| Method | Arguments and result |
| --- | --- |
| `save_execution(execution)` | Atomically write `executions/<execution_id>.json`; return its path. |
| `load_execution(path_or_id)` | Load an explicit JSON path or resolve an ID below `executions/`; return `ExecutionRecord`. |
| `latest_execution()` | Load the execution JSON with the latest modification time. |

Execution records capture queue items, processor identity, catalog audit,
execution/queue configs, grouping, query provenance, and creation time.

## `sync_manifest(execution, states) -> Path`

`states` maps unit IDs to state dictionaries or `None`. The method writes a
root manifest containing:

- update time;
- latest execution ID;
- grouping;
- processor identity; and
- one entry per requested unit with its serialized `ProcessingUnit`,
  fingerprint, status, and complete current state.

The returned path is `workspace/manifest.json`.

# Publication

## `PublishMode`

Literal value `"auto"`, `"hardlink"`, or `"copy"`:

| Mode | Behavior |
| --- | --- |
| `"auto"` | Try a hard link; fall back to a metadata-preserving copy. |
| `"hardlink"` | Require a hard link; propagate filesystem failure. |
| `"copy"` | Always copy. |

Genome FASTAs not already gzip-compressed are compressed during publication,
so their publish method is recorded as `"gzip"`.

## `DatasetExport`

```python
DatasetExport(
    destination,
    manifest,
    experiments,
    genomes,
)
```

| Field | Meaning |
| --- | --- |
| `destination: Path` | Published dataset root. |
| `manifest: Path` | Published provenance manifest. |
| `experiments: int` | Number of experiment records produced by this call. |
| `genomes: int` | Number of distinct species genome files produced by this call. |

## `DatasetPublisher`

```python
DatasetPublisher(
    workspace,
    *,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `workspace: Path` | Source builder workspace. |
| `progress: ProgressReporter | None` | Optional publication/hash reporting sink. |

### `publish(execution, destination=None, *, mode="auto", overwrite=False) -> DatasetExport`

| Argument | Meaning |
| --- | --- |
| `execution: ExecutionRecord` | Snapshot whose requested experiments are published. |
| `destination: Path | None` | Separate dataset root, or `None` for in-place workspace publication. |
| `mode: PublishMode` | File materialization policy. |
| `overwrite: bool` | Permit atomic replacement of an existing separate destination. |

The high-level equivalent is
`DatasetBuilder.publish_dataset(destination, execution_id=..., mode=..., overwrite=...)`.

## Publication requirements

Publication validates all of these before finalizing:

1. the execution is grouped by `"experiment"`;
2. each unit represents exactly one experiment matching its unit ID;
3. each experiment links exactly one SRA Sample;
4. unit state is `"succeeded"`;
5. declared processing outputs contain the `coverage` role;
6. normalized metadata and the experiment description exist;
7. persisted genome taxonomy matches the processing unit;
8. species filename collisions do not refer to different assemblies; and
9. published genome gzip data is readable and begins with a FASTA header.

The publisher reads the experiment-keyed description written during metadata
enrichment; publication does not reshape sample-level metadata.

## Published layout

```text
model-dataset/
├── manifest.json
├── bigWig/
│   └── <experiment>.bw
├── descriptions/
│   └── <experiment>.json
└── genomes/
    └── <scientific_name>.fasta.gz
```

The manifest records run accessions, SRA Sample, species, taxonomy ID, assembly,
relative file paths, checksums, sizes, publish methods, and genome provenance.

## Atomic behavior

### Separate destination

The complete dataset is built in a sibling staging directory. If
`overwrite=True`, an existing target is first moved to a unique backup only
after staging succeeds; the staged dataset then replaces it atomically. On
failure, staging is removed and a displaced target is restored.

### In-place publication

Files are staged below `workspace/work/publishing/<execution_id>/`, then
moved into the workspace’s published directories. The root manifest keeps
previous experiment/genome records and marks which experiments were requested
by the latest execution.

## Example

```python
from pathlib import Path

# Select the latest execution and publish to a separate compact directory.
export = builder.publish_dataset(
    Path("/data/model-dataset"),
    mode="auto",       # Prefer hard links on the same filesystem.
    overwrite=False,   # Refuse to replace an existing dataset.
)

print(export.manifest)
print(export.experiments, export.genomes)
```

If the destination already exists, inspect it before rerunning with
`overwrite=True`.

# Resume and cleanup boundaries

| Mechanism | Owns |
| --- | --- |
| Unit state | Status, semantic fingerprint, declared output hashes/metadata, genome and FASTQ provenance |
| Queue cleanup | Provider-owned roots below `workspace/fastq/` |
| Processor retention | Processor-created work/output/cache artifacts |
| Publication | Only its staging directory, separate destination replacement, and in-place published files |

Publication never changes a successful processing state. Queue cleanup failure
also does not turn a validated success into failure; inspect unit logs when
input retention is unexpected.

# Internal helpers

Publication’s path removal, file linking/copying, genome compression,
description projection, and BigWig selection helpers are private. Use
`publish()` or `DatasetBuilder.publish_dataset()` so validation and atomic
replacement stay intact.
