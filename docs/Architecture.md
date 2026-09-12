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
                                  optional compact dataset publication
```

## Main concepts

| Concept | Meaning | Where it is stored |
| --- | --- | --- |
| Run catalog | One row per SRA run plus an immutable audit trail | Memory and optional `catalogs/` CSV cache |
| Processing unit | Group of runs processed together | Execution record and manifest |
| Execution record | Immutable snapshot of requested units, resource config, queue config, and provenance | `executions/<execution-id>.json` |
| Unit fingerprint | Hash of semantic work identity | Execution item and unit state |
| Unit state | Current claim, phase, resources, result, or error for one unit | `state/units/<unit-id>.json` |
| Workspace manifest | Latest execution summary and optional published dataset section | `manifest.json` |
| Provider input | Downloaded/staged data controlled by queue cleanup | `fastq/` |
| Processor work | Recoverable or removable processor-created intermediates | `work/units/<unit-id>/` |
| Processor output | Declared final files | `outputs/<unit-id>/` |

## Workspace layout

| Path | Owner | Purpose | May default cleanup remove it? |
| --- | --- | --- | --- |
| `workspace.json` | Workspace | Stable grouping, description profile, genome policy, schema, and directory roles | No |
| `manifest.json` | Workspace | Latest execution/unit summary and publication manifest | No |
| `catalogs/` | Builder | Cached RunInfo query CSVs | No |
| `metadata/` | Metadata layer | Normalized records and sample descriptions | No |
| `metadata_cache/` | Entrez clients | Reusable raw NCBI responses | No |
| `fastq/` | FASTQ provider | SRA archives, converted/merged FASTQs, and provider manifests | Yes, only declared unit roots |
| `work/genome_cache/` | Genome manager | Downloaded genomes, lockfile, and indexes | No |
| `work/units/<id>/` | Processor | Unit-local intermediates | No; processor retention may remove its own files |
| `outputs/<id>/` | Processor | Final declared artifacts | No |
| `state/units/` | State store | Current atomic per-unit JSON state | No |
| `state/history/` | State store | Archived prior state after semantic changes/repairs | No |
| `state/locks/` | State store | Unit update locks | No |
| `executions/` | Workspace | Immutable automatic execution snapshots | No |
| `slurm/` | Slurm executor | Coordinator and distributed sample scripts | No |
| `logs/` | Logging layer/Slurm | Per-unit and scheduler logs | No |
| `bigWig/`, `descriptions/`, `genomes/` | Publisher | Optional in-place compact dataset | No |

## How processing units are formed

`RunCatalog.processing_units(by=...)` supports:

| `group_by` | Unit identifier | Typical use | Restriction |
| --- | --- | --- | --- |
| `"run"` | SRA run accession | Treat each run independently | Replicates/runs are not merged |
| `"experiment"` | SRA experiment accession | Default; combine runs from one experiment | Required for compact publication |
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

The first execution writes `workspace.json`. Three settings are treated as
stable semantics:

| Setting | Why stable |
| --- | --- |
| `group_by` | Changes which runs belong to one unit |
| `description_profile` | Changes published/normalized description meaning |
| `genome_policy` | Changes reference-selection semantics |

If unit state already exists, changing one of these values raises instead of
mixing incompatible meanings in one workspace. Start a new workspace when the
scientific semantics must change.

## Unit state lifecycle

| Status/phase | Meaning |
| --- | --- |
| No file / pending | Unit has not been claimed for this workspace state |
| `submitted` / `queued` | Distributed Slurm job ID and resources are durably recorded |
| `running` / `staging` | Unit is claimed; genome and provider input are being prepared |
| `running` / `ready` | Input and genome are ready for processing |
| `running` / `processing` | Processor is running |
| `succeeded` / `completed` | Result and validation metadata are durable |
| `failed` / `failed` | Bounded traceback tail is stored |

State writes and execution writes are atomic. Unit updates are protected by
per-unit file locks. Local/single-node coordination also uses a workspace queue
coordinator lock.

## Success validation and reuse

A matching success is reusable only when:

1. its fingerprint matches the requested work;
2. at least one processor output is declared;
3. every declared output exists and is non-empty;
4. unchanged size and modification time match stored facts, or a stored SHA-256
   matches a newly computed checksum; and
5. the persisted genome FASTA exists and is non-empty.

If a matching success has invalid outputs, the builder force-reclaims it and
resets its package-owned unit work/output directories before processing.

## Retry behavior

| Existing state | Default behavior | With `retry_failed=True` |
| --- | --- | --- |
| Matching valid success | Reuse as `skipped` | Still reuse |
| Matching success with invalid output | Rebuild | Rebuild |
| Matching failure | Return/leave skipped failure state | Reclaim and retry |
| Different fingerprint | Archive old state and run new work | Same |
| Active matching claim | Local caller receives skipped `UnitAlreadyRunning`; distributed worker may reclaim its submitted/running claim | Same relevant mode behavior |

Retry changes whether matching failures are reclaimed. It does not bypass
fingerprints or make invalid outputs acceptable.

## Reset and deletion boundaries

When a unit must be rebuilt after a changed fingerprint, failed processing, or
invalid success, the processor phase may remove:

```text
workspace/work/units/<unit-id>/
workspace/outputs/<unit-id>/
```

The builder does not reset arbitrary paths supplied by the user.

After processing, queue cleanup is separately constrained to provider-declared
roots strictly below:

```text
workspace/fastq/
```

See [Storage](Storage.md#cleanup-matrix) for the cleanup decision table.

## Logs

| Log | Location | Contents |
| --- | --- | --- |
| Unit log | `logs/<sanitized-species>/<unit-id>.log` | Download, processing, warnings, errors, and cleanup diagnostics |
| Single-node coordinator log | `logs/slurm/<job-id>.coordinator.log` | Whole allocation/coordinator output |
| Distributed coordinator log | `logs/slurm/<job-id>.coordinator.log` | Admissions, scheduler queries, and worker coordination |
| Distributed worker log | `logs/slurm/sample-<item-index>.<job-id>.log` | Batch stdout/stderr for one sample job |

Unit logs append across attempts. `QueuePolicy.fsync_logs=True` synchronizes
phase-boundary records for durability.

## Distributed submission safety

For each sample the coordinator:

1. writes a sample script;
2. submits it in Slurm hold state;
3. records job ID, CPUs, memory, fingerprint, execution ID, item, and log path;
4. releases the held job; and
5. cancels it if durable submission bookkeeping fails after `sbatch`.

The coordinator uses `squeue` to observe active jobs. When `squeue` fails, new
admission pauses. If a job disappears for more than 30 seconds without writing
terminal unit state, the coordinator marks that unit failed.

## Publication

`publish_dataset()` creates a compact dataset only when:

| Requirement | Reason |
| --- | --- |
| Execution is grouped by experiment | Publication keys are experiment IDs |
| Each unit has exactly one matching experiment | Prevents ambiguous output ownership |
| Each experiment links exactly one SRA Sample | Description identity must be unambiguous |
| Unit state is successful | Failed or pending data is not publishable |
| Exactly one BigWig is declared | Compact manifest expects one coverage track per experiment |
| Normalized metadata and sample description exist | Required for published descriptions |
| Genome taxonomy matches the unit | Prevents cross-species publication |

Publication can use hard links or copies. `auto` attempts a hard link and
falls back to copying. An external existing destination requires
`overwrite=True`; replacement is staged and swapped atomically.

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
