# Command-line interface

Installation creates `ncbi-dataset`; `python -m ncbi_dataset_builder` is
equivalent. Run `ncbi-dataset COMMAND --help` for argparse's generated view.

## Shared builder options

| Option | Values and purpose |
|---|---|
| `--workspace PATH` | Required durable workspace root. |
| `--email ADDRESS` | NCBI contact email; defaults to `NCBI_EMAIL`. Required for direct NCBI metadata/catalog access. |
| `--ncbi-api-key KEY` | Optional NCBI API key; defaults to `NCBI_API_KEY`. |
| `--group-by LEVEL` | `run`, `experiment`, `sra_sample`, or `biosample`; default `experiment`. |
| `--prefetch-max-size VALUE` | SRA Toolkit maximum archive size, for example `100G` or `u`; default `u`. |

Commands that process a catalog accept exactly one of `--catalog PATH` and
`--query EXPR`. `--refresh` bypasses the matching NCBI catalog cache.

## Queue options

`build-local`, `submit-single-node`, and `submit-distributed` accept:

| Option | Default | Meaning |
|---|---:|---|
| `--download-workers N` | `2` | Simultaneous sample downloads. |
| `--max-inflight-gb GB` | unset | Optional estimated storage window for downloading, ready, and processing samples. |
| `--processing-storage-multiplier X` | `1.0` | Estimated peak processor storage divided by raw input size; must be at least one. |
| `--keep-inputs` | false | Never remove provider-owned input after success. |
| `--discard-failed-inputs` | false | Allow cleanup after a failed processor instead of retaining inputs. |
| `--retry-failed` | false | Retry matching failed sample state. |

## `fetch-catalog`

Fetches a complete NCBI SRA RunInfo table and writes CSV.

```bash
ncbi-dataset fetch-catalog \
  --workspace /data/ncbi-workspace \
  --email researcher@example.org \
  --ncbi-api-key 0123456789abcdef0123456789abcdef01234567 \
  --query '"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]' \
  --output runinfo.csv
```

Required command fields are `--query` and `--output`. Add `--refresh` to bypass
the existing query cache. The key above is fake.

## `build-local`

Runs the streaming queue in the current process. Local execution has CPU and
free-storage settings but no memory setting.

```bash
ncbi-dataset build-local \
  --workspace /data/ncbi-workspace \
  --catalog runinfo.csv \
  --processor my_processors:process_sample \
  --total-cpus 100 \
  --min-cpus-per-job 4 \
  --max-cpus-per-job 20 \
  --max-running-jobs 10 \
  --reserve-free-gb 500 \
  --download-workers 6 \
  --max-inflight-gb 1200 \
  --processing-storage-multiplier 2
```

`--processor` is an importable `module:callable`. `--total-cpus` is required;
`--min-cpus-per-job` and `--max-running-jobs` default to one;
`--max-cpus-per-job` defaults to the total; and `--reserve-free-gb` defaults to
zero. Exit status is `1` if any sample failed, otherwise `0`.

## Shared Slurm and quota options

Both Slurm commands accept optional `--partition`, `--account`, and `--qos`,
plus these fields:

| Option | Purpose |
|---|---|
| `--quota-gb GB` | Required user/project storage quota. |
| `--quota-reserve-gb GB` | Capacity to leave unused; default zero. |
| `--quota-usage-root PATH` | Root whose current file size counts against quota; workspace when omitted. |
| `--script-path PATH` | Override generated coordinator script destination. |
| `--no-submit` | Write the script and execution snapshot without calling `sbatch`. |

Slurm storage is quota-based and never inferred from global filesystem free
space.

## `submit-single-node`

Requests one allocation and streams multiple samples inside it.

```bash
ncbi-dataset submit-single-node \
  --workspace /scratch/project-owner/ncbi-workspace \
  --catalog runinfo.csv \
  --processor ncbi_dataset_builder.processing.atac:process_atac \
  --allocation-cpus 128 \
  --allocation-memory-gb 1000 \
  --allocation-time-limit 2-00:00:00 \
  --min-cpus-per-job 8 \
  --max-cpus-per-job 32 \
  --memory-gb-per-job 100 \
  --max-running-jobs 8 \
  --quota-gb 5000 \
  --quota-reserve-gb 250 \
  --quota-usage-root /scratch/project-owner \
  --partition amd_1Tb,amd_2Tb
```

Allocation CPUs, allocation memory, allocation time, and per-job memory are
required. Per-job CPU defaults match the Python API: minimum one and maximum
the allocation. `max_running_jobs` defaults to one.

## `submit-distributed`

Submits a small coordinator that creates independent Slurm jobs for ready
samples.

```bash
ncbi-dataset submit-distributed \
  --workspace /scratch/project-owner/ncbi-workspace \
  --query '"ATAC-seq"[Strategy] AND "Mus musculus"[Organism]' \
  --email researcher@example.org \
  --processor ncbi_dataset_builder.processing.atac:process_atac \
  --total-cpu-quota 500 \
  --max-running-jobs 50 \
  --cpus-per-node 128 \
  --min-cpus-per-job 8 \
  --max-cpus-per-job 64 \
  --memory-gb-per-job 100 \
  --worker-time-limit 3-00:00:00 \
  --coordinator-cpus 1 \
  --coordinator-memory-gb 4 \
  --coordinator-time-limit 7-00:00:00 \
  --quota-gb 5000 \
  --quota-reserve-gb 250 \
  --quota-usage-root /scratch/project-owner \
  --partition amd_256M,amd_1Tb,amd_2Tb
```

The first seven worker resource fields shown above are required. Coordinator
fields default to 1 CPU, 4 GB, and seven days. `total_cpu_quota` includes the
coordinator.

## `status`

```bash
ncbi-dataset status --workspace /data/ncbi-workspace
ncbi-dataset status --workspace /data/ncbi-workspace --execution-id execution-20260912-abc123
```

Without `--execution-id`, the newest execution snapshot is selected. Output is
a Python mapping with counts and per-sample state records.

## `publish`

Publishes verified experiment-level BigWigs, descriptions, and genomes:

```bash
ncbi-dataset publish \
  --workspace /data/ncbi-workspace \
  --destination /data/model-dataset \
  --mode auto \
  --overwrite
```

`--destination` defaults to in-place workspace publication. `--execution-id`
selects a snapshot; otherwise the latest is used. `--mode` is `auto`,
`hardlink`, or `copy`. `--overwrite` is required to replace a separate existing
destination.

