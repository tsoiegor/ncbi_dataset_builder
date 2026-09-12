# Installation

Install only the components required by the workflow you will run. The Python
package itself requires Python 3.10 or newer and Polars. SRA acquisition,
reference-genome acquisition, ATAC processing, and Slurm execution each add
external commands.

The project is not published to PyPI or Conda. Install it from this checkout or
from a wheel built from this checkout.

## Choose the required components

| Workflow | Python package | External commands |
| --- | --- | --- |
| Load an existing RunInfo CSV and use custom local files/tools | Base package | Only commands required by your provider and processor |
| Fetch SRA archives and convert to FASTQ | Base package | `prefetch`, `vdb-validate`, `fasterq-dump`; optional `pigz` |
| Resolve and download NCBI genomes | Base package | NCBI `datasets` |
| Run the built-in ATAC processor | Base package | `fastp`, `bowtie2`, `bowtie2-build`, `samtools`, `bamCoverage` |
| Submit Slurm jobs | Base package | `sbatch`; distributed mode also needs `scontrol`, `scancel`, and `squeue` |
| Show tqdm progress bars | `progress` extra | No additional command |
| Run tests and lint | `dev` extra | No additional command |

## Recommended Conda environment

```bash
# Create and activate one environment visible wherever processing will run.
conda create -n ncbi-builder python=3.12 -y
conda activate ncbi-builder

# Install acquisition, genome, and built-in ATAC dependencies.
conda install -c conda-forge -c bioconda \
  sra-tools ncbi-datasets-cli pigz fastp bowtie2 samtools deeptools -y

# Enter this checkout and install the runtime package plus progress bars.
cd /path/to/multispeciesATACseq
python -m pip install -e ".[progress]"
```

Use `".[dev,progress]"` for a development environment with pytest,
pytest-cov, and Ruff.

## Python installation parameters

| Choice | Meaning | Starting recommendation | Restriction |
| --- | --- | --- | --- |
| Python version | Interpreter used by the package and embedded in generated Slurm scripts | Python 3.11 or 3.12 | Must be 3.10 or newer |
| Base install | Installs the package and Polars | Always | Required |
| `progress` extra | Installs tqdm | Use for interactive local runs | Text progress remains available without it |
| `dev` extra | Installs test and lint tooling | Use only for development | Not required on compute nodes |
| Editable install | Imports code directly from the checkout | Convenient during development | Every Slurm node must see the checkout at the same path |
| Wheel install | Copies a fixed package version into the environment | Prefer for stable cluster deployments | Install the same wheel in submission and compute environments |

## External command details

| Command | Used by | Required behavior |
| --- | --- | --- |
| `prefetch` | `SraToolkitProvider` staging | Downloads each SRA run archive |
| `vdb-validate` | `SraToolkitProvider` staging | Validates a staged SRA archive before it is accepted |
| `fasterq-dump` | `SraToolkitProvider` materialization | Converts a valid archive with `--split-3` |
| `pigz` | FASTQ compression | Optional parallel compressor; Python gzip is used when absent |
| `datasets` | `GenomeManager` | Summarizes assemblies and downloads selected genome packages |
| `fastp` | `AtacSeqProcessor` | Cleans paired and/or single FASTQ and writes JSON/HTML reports |
| `bowtie2-build` | `AtacSeqProcessor` | Creates a reusable six-file Bowtie2 index |
| `bowtie2` | `AtacSeqProcessor` | Aligns cleaned reads |
| `samtools` | `AtacSeqProcessor` | Converts, sorts, merges/copies, and CSI-indexes BAM files |
| `bamCoverage` | `AtacSeqProcessor` | Produces BigWig coverage |
| `sbatch` | Both Slurm modes | Submits generated coordinator and, in distributed mode, worker scripts |
| `scontrol` | Distributed Slurm | Releases a worker after its job ID and resources are persisted |
| `scancel` | Distributed Slurm | Cancels a held worker when durable submission fails |
| `squeue` | Distributed Slurm | Reports active worker states; failed queries pause new admissions |

## Verify the environment

Run these checks in the same environment that will execute the work:

```bash
# Confirm the package and CLI.
python -c "import ncbi_dataset_builder as n; print(n.__version__)"
ncbi-dataset --help

# Confirm SRA acquisition tools and versions.
python -c "from ncbi_dataset_builder import SraToolkitProvider; print(SraToolkitProvider().preflight())"

# Confirm NCBI genome acquisition.
python -c "from ncbi_dataset_builder import GenomeManager; from pathlib import Path; print(GenomeManager(Path('/tmp/genome-check')).preflight())"

# Confirm all built-in ATAC tools.
python -c "from ncbi_dataset_builder import AtacSeqProcessor; print(AtacSeqProcessor().preflight())"
```

The genome command creates the supplied root. Replace `/tmp/genome-check`
with a writable temporary path. A preflight failure is expected to name the
missing command.

For Slurm, also test the compute-node environment rather than only the login
node:

```bash
# Replace the partition and environment activation with site-specific values.
srun --partition=compute --cpus-per-task=1 --mem=2G --time=00:05:00 \
  bash -lc 'conda activate ncbi-builder && python -c "import ncbi_dataset_builder; print(ncbi_dataset_builder.__version__)"'
```

## Slurm environment checklist

| Check | Why it matters |
| --- | --- |
| The workspace has the same absolute path on login and compute nodes | Generated scripts contain resolved absolute workspace and execution-record paths |
| The submitting Python executable exists on compute nodes | Its absolute path is embedded in coordinator and worker commands |
| The processor module imports in that interpreter | Slurm accepts an import reference, not a notebook-local callable |
| External bioinformatics commands are on batch-job `PATH` | Interactive shell initialization may differ from a batch shell |
| `NCBI_API_KEY` is exported to jobs when needed | Generated scripts pass email explicitly but workers read the API key from their environment |
| Workspace storage is writable from every participating node | State, scripts, logs, inputs, work, and outputs are shared |
| `sbatch` is available on the submission node | `submit_slurm(submit=True)` calls it immediately |
| `squeue`, `scontrol`, and `scancel` are available in distributed jobs | The distributed coordinator uses them directly |

## Common installation failures

| Symptom | Likely cause | Action |
| --- | --- | --- |
| Polars reports inconsistent expression classes | `polars` and `polars-lts-cpu` were mixed, or a notebook retained old imports | Restart the kernel; if needed, remove both distributions and install exactly one |
| Import works on login node but Slurm job fails | Different interpreter, missing editable checkout, or batch `PATH` | Inspect the generated script and test its exact Python path inside `srun` |
| `datasets` is missing | NCBI Datasets CLI was not installed | Install `ncbi-datasets-cli` and rerun `GenomeManager.preflight()` |
| ATAC preflight fails at `bamCoverage` | deepTools is missing from the active environment | Install deepTools in the compute environment |
| SRA conversion is slow during compression | `pigz` is absent | Install `pigz` or accept single-process Python gzip |
| Tool exists interactively but not in Slurm | Shell modules/Conda activation are not applied in batch | Make the submitting interpreter and tool `PATH` available to the batch environment |

## Restrictions to understand before the first Slurm run

- A local editable checkout is not enough unless compute nodes see that same
  checkout path.
- Generated scripts do not add a Conda activation command. They invoke the
  exact Python executable that submitted the job.
- The standard Slurm workers reconstruct the default SRA provider and genome
  manager. A custom FASTQ provider or custom genome-manager object attached to
  the submitting `DatasetBuilder` is not serialized to workers.
- Non-default ATAC configuration must be exposed as an importable configured
  callable; the default reference
  `ncbi_dataset_builder.processing.atac:process_atac` uses default settings.
- Installing tools does not select safe CPU, memory, storage, or concurrency
  values. Continue with [Choosing an execution system](ExecutionSystems.md).

## Next step

Read [Choosing an execution system](ExecutionSystems.md), then use the full
[local](LocalExecution.md),
[single-node Slurm](SlurmSingleNodeExecution.md), or
[distributed Slurm](SlurmDistributedExecution.md) setup worksheet.
