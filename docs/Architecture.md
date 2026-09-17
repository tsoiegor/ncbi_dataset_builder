# Architecture, state, and restart behavior

The package is built around one durable workspace and one independently
restartable state record per processing unit. An execution record is created
automatically from the catalog and configuration. Users do not manually build
an orchestration graph.

## End-to-end flow

```text
NCBI query or RunInfo CSV
          |
          v
      RunCatalog -- immutable filters/transforms --> ProcessingUnit values
          |                                             |
          |                                             v
          |                                     execution snapshot
          |                                             |
          v                              +--------------+--------------+
 normalized metadata                    |              |              |
                                        v              v              v
                                  genome resolve   input stage   state claim
                                        \              |              /
                                         \             v             /
                                          +------ processor --------+
                                                     |
                                                     v
                                         validate and checksum outputs
                                                     |
                                                     v
                                         per-unit state and manifest
                                                     |
                                                     v
                                  downstream manifest-driven reshaping
```

## Main concepts

| Concept | Meaning | Where it is stored |
| --- | --- | --- |
| Run catalog | One row per SRA run plus an immutable audit trail | Memory and optional `runtime/catalogs/` CSV cache |
| Processing unit | Group of runs processed together | Execution record and manifest |
| Execution record | Immutable snapshot of requested units, resource config, queue config, and provenance | `runtime/executions/<execution-id>.json` |
| Unit fingerprint | Hash of semantic work identity | Execution item and unit state |
| Unit state | Current claim, phase, resources, result, or error for one unit | `runtime/state/units/<unit-id>.json` |
| Workspace manifest | Public cumulative unit and processor-artifact index | `manifest.json` |
| Provider input | Downloaded/staged data controlled by queue cleanup | `runtime/fastq/` |
| Processor output | Processor-owned artifacts and intermediates | `output/<unit-id>/` |

## Workspace layout

| Path | Owner | Purpose | May default cleanup remove it? |
| --- | --- | --- | --- |
| `manifest.json` | Workspace | Public artifact index | No |
| `output/<id>/` | Processor | All processor-owned files | Replaced only when that unit rebuilds |
| `runtime/workspace.json` | Workspace | Stable grouping, output root, genome policy, and schema | No |
| `runtime/catalogs/` | Builder | Cached RunInfo query CSVs | No |
| `runtime/metadata/` | Metadata layer | Normalized records and cache index | No |
| `runtime/metadata_cache/` | Entrez clients | Reusable raw NCBI responses | No |
| `runtime/fastq/` | FASTQ provider | SRA archives, FASTQs, and provider manifests | Yes, only declared unit roots |
| `runtime/genomes/` | Genome manager | Downloaded genomes, lockfile, and indexes | No |
| `runtime/state/` | State store | Unit state, locks, and retry history | No |
| `runtime/executions/` | Workspace | Immutable automatic execution snapshots | No |
| `runtime/slurm/` | Slurm executor | Coordinator and sample scripts | No |
| `runtime/logs/` | Logging layer/Slurm | Per-unit and scheduler logs | No |

## How processing units are formed

`RunCatalog.processing_units(by=...)` supports:

| `group_by` | Unit identifier | Typical use | Restriction |
| --- | --- | --- | --- |
| `"run"` | SRA run accession | Treat each run independently | Replicates/runs are not merged |
| `"experiment"` | SRA experiment accession | Default; combine runs from one experiment | Usually best for assay-level processing |
| `"sra_sample"` | SRA Sample accession | Combine experiments/runs linked to one SRA sample | Requires the `SRA Sample` column |
| `"biosample"` | BioSample accession | Broad biological-sample grouping | Requires the `BioSample` column |

Every unit must resolve to at most one taxonomy ID and one scientific name.
Contradictory species data is rejected rather than silently selected.

## Execution-record contents

| Field | Meaning |
| --- | --- |
| `execution_id` | UTC-like timestamp plus content hash |
| `created_at` | Creation timestamp |
| `query` | Optional source query recorded as provenance |
| `group_by` | Unit grouping used for this snapshot |
| `items` | Ordered queue items, resources, pins, and fingerprints |
| `processor_identity` | Import reference or derived callable/config/source identity |
| `execution_type` | Local, single-node Slurm, or distributed Slurm |
| `execution_config` | Serialized execution and storage values |
| `queue_config` | Serialized `QueuePolicy` |
| `catalog_audit` | Immutable catalog operations preceding execution |
| `metadata` | Provider identity and future provenance extensions |

Execution records are written before local work begins or Slurm is submitted.
This makes the exact request inspectable and reproducible.

## What enters the unit fingerprint

| Input | Included? |
| --- | --- |
| Grouping level | Yes |
| Unit identifier | Yes |
| Sorted run, experiment, SRA Sample, and BioSample accessions | Yes |
| Taxonomy ID | Yes |
| Exact genome pin | Yes |
| Processor identity/config/source hash where available | Yes |
| FASTQ provider identity/config or URL mapping | Yes |
| CPU count and concurrency | No |
| Queue polling interval | No |
| Slurm partition/time limit | No |

Resource tuning can therefore reuse biologically identical successes.
Changing a semantic input archives prior state and creates fresh work.

## Stable workspace configuration

The first execution writes `runtime/workspace.json`. Three settings are treated as
stable semantics:

| Setting | Why stable |
| --- | --- |
| `group_by` | Changes which runs belong to one unit |
| `output_dir` | Changes ownership and location of processor artifacts |
| `genome_policy` | Changes reference-selection semantics |

If unit state already exists, changing one of these values raises instead of
mixing incompatible meanings in one workspace. Start a new workspace when the
scientific semantics must change.

## Unit state lifecycle

| Status/phase | Meaning |
| --- | --- |
| No file / pending | Unit has not been claimed for this workspace state |
| `downloading` / `resolving-genome` | Genome selection/download is in progress |
| `downloading` / `downloading-sra` or `downloading-input` | Provider input is being staged |
| `ready` / `ready` | Input and genome are durable and waiting for processing capacity |
| `submitted` / `queued` | Distributed Slurm job ID and resources are durably recorded |
| `running` / `starting` | A distributed worker has started but has not entered the processor yet |
| `running` / `processing` | Processor is running |
| `succeeded` / `completed` | Result and validation metadata are durable |
| `failed` / `failed` | Bounded traceback tail is stored |

State writes and execution writes are atomic. Unit updates are protected by
per-unit file locks and a unique claim token. Once a retry or replacement owns
a newer claim, the superseded worker cannot publish a phase, resource update,
success, or failure over it. Local/single-node coordination also uses a
workspace queue coordinator lock.

## Success validation and reuse

A matching success is reusable only when:

1. its fingerprint matches the requested work;
2. at least one processor output is declared;
3. every declared output exists and is non-empty;
4. unchanged size and modification time match stored facts, or a stored SHA-256
   matches a newly computed checksum; and
5. the persisted genome FASTA exists and is non-empty.

If a matching success has invalid outputs, the builder force-reclaims it and
resets its processor-owned unit output directory before processing.

## Retry behavior

| Existing state | Default behavior | With `retry_failed=True` |
| --- | --- | --- |
| Matching valid success | Reuse as `skipped` | Still reuse |
| Matching success with invalid output | Rebuild | Rebuild |
| Matching failure | Return/leave skipped failure state | Reclaim and retry |
| Different fingerprint | Archive old state and run new work | Same |
| Active matching claim | Local caller receives skipped `UnitAlreadyRunning`; distributed resume checks its recorded Slurm job before either reattaching or reclaiming it | Same relevant mode behavior |

Retry changes whether matching failures are reclaimed. It does not bypass
fingerprints or make invalid outputs acceptable.

## Reset and deletion boundaries

When a unit must be rebuilt after a changed fingerprint, failed processing, or
invalid success, the processor phase may remove:

```text
workspace/output/<unit-id>/
```

The builder does not reset arbitrary paths supplied by the user.

After processing, queue cleanup is separately constrained to provider-declared
roots strictly below:

```text
workspace/runtime/fastq/
```

See [Storage](Storage.md#cleanup-matrix) for the cleanup decision table.

## Logs

| Log | Location | Contents |
| --- | --- | --- |
| Unit log | `runtime/logs/<sanitized-species>/<unit-id>.log` | Download, processing, warnings, errors, and cleanup diagnostics |
| Single-node coordinator log | `runtime/logs/slurm/<job-id>.coordinator.log` | Whole allocation/coordinator output |
| Distributed coordinator log | `runtime/logs/slurm/<job-id>.coordinator.log` | Admissions, scheduler queries, and worker coordination |
| Distributed worker log | `runtime/logs/slurm/sample-<item-index>.<job-id>.log` | Batch stdout/stderr for one sample job |

Unit logs append across attempts. `QueuePolicy.fsync_logs=True` synchronizes
phase-boundary records for durability.

## Distributed submission safety

For each sample the coordinator:

1. writes a sample script;
2. submits it in Slurm hold state;
3. records job ID, CPUs, memory, fingerprint, execution ID, item, and log path;
4. releases the held job; and
5. cancels it if durable submission bookkeeping fails after `sbatch`.

The coordinator reads the active Slurm queue once and filters it to recorded job
IDs, so a purged historical ID does not make `squeue` fail. When `squeue` itself
fails, new admission pauses. If a job disappears for more than 30 seconds
without writing terminal unit state, its old claim is invalidated and the unit
is put at the front of the queue. Persisted prepared input is reused; otherwise
provider staging resumes from its cache.

## Dataset shaping

The core does not impose a BigWig, description, or genome folder structure.
Each processor owns its unit output directory and declares durable artifacts.
`manifest.json` retains units from every execution and records each unit's
latest portable paths, roles, sizes, checksums, execution ID, and processor
identity. A small downstream script can therefore create any consolidated
dataset layout without losing older experiments when a later run selects only
a subset.

## Operational checklist

Before a long run:

- inspect the catalog and unit grouping;
- verify workspace path and available storage;
- preflight acquisition, genome, and processor commands;
- use a small representative catalog;
- inspect generated Slurm scripts with `submit=False`;
- verify processor imports in the compute environment;
- measure peak memory and storage;
- confirm per-unit and Slurm logs appear where expected; and
- only then increase concurrency.

## Related pages

- [Choosing an execution system](ExecutionSystems.md)
- [Storage and queue policy](Storage.md)
- [Catalogs](Catalogs.md)
- [Writing a processor](Processors.md)
- [Workspace API reference](../src/ncbi_dataset_builder/workspace/README.md)
- [Execution state API](../src/ncbi_dataset_builder/execution/README.md)
