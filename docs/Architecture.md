# Architecture

The library has one durable workspace and one queue item per selected sample.
An execution snapshot records what was requested automatically; users do not
create, save, or submit a separate orchestration object.

```text
NCBI query or RunInfo CSV
          |
          v
      RunCatalog -- filter/transform --> ProcessingUnit per sample
          |                                  |
          |                                  v
          |                         execution sample queue
          |                         /        |        \
          v                        v         v         v
 normalized metadata          download    genome   processor
          |                        \         |         /
          |                         \        v        /
          +-----------------------> workspace state
                                             |
                                             v
                                publish BigWig/descriptions/genomes
```

The streaming scheduler admits samples according to CPU, Slurm memory where
applicable, the execution system's storage policy, `max_running_jobs`, and the
queue's optional in-flight estimate. Downloads can overlap processing. A sample
is restartable independently, and successful outputs are checked before they
are reused.

## Workspace layout

```text
workspace/
  workspace.json      stable grouping, description, and genome semantics
  manifest.json       latest execution and sample state
  catalogs/           query cache and selected run tables
  metadata/           normalized records and sample descriptions
  metadata_cache/     raw reusable NCBI responses
  fastq/              provider-owned inputs eligible for queue cleanup
  work/                processor intermediates and genome cache
  outputs/             declared processor outputs by sample
  state/units/         atomic, restart-safe sample state
  state/history/       archived state after semantic input changes
  executions/          automatic immutable execution snapshots
  slurm/               generated scheduler scripts
  logs/                sample and scheduler logs
  bigWig/              optional in-place published BigWigs
  descriptions/        optional in-place published descriptions
  genomes/             optional in-place published genomes
```

Changing CPU counts or queue concurrency does not change biological identity.
Changing runs, grouping, genome pin, or processor identity changes the sample
fingerprint and causes a rebuild while preserving prior state in history.

