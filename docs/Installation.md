# Installation

The package requires Python 3.10 or newer and Polars 1.x. It has not yet been
published to PyPI or Conda, so install from this checkout or a wheel built from
it.

## Conda-managed environment

Conda can install Python and the executables needed by the built-in ATAC-seq
processor:

```bash
conda create -n ncbi-builder python=3.12 -y
conda activate ncbi-builder
conda install -c conda-forge -c bioconda \
  sra-tools fastp bowtie2 samtools deeptools -y
git clone https://github.com/tsoiegor/ncbi_dataset_builder.git
cd ncbi_dataset_builder
python -m pip install -e ".[dev,progress]"`
```

The `dev` extra installs pytest, pytest-cov, and Ruff; `progress` installs tqdm. A basic
runtime install needs only Polars.

## External tools

Only workflows that use the built-in components need these programs:

| Component | Executables |
|---|---|
| SRA FASTQ provider | `prefetch`, `fasterq-dump`; `fastq-dump` fallback; optional `pigz` |
| NCBI genome manager | NCBI `datasets` CLI plus archive/decompression support |
| ATAC-seq processor | `fastp`, `bowtie2`, `bowtie2-build`, `samtools`, `bamCoverage` |
| Slurm execution | `sbatch`, `scontrol`, `scancel`, `squeue`, and `sacct` where available |

Custom providers/processors only need their own dependencies. Python methods
such as `SraToolkitProvider.preflight()`, `GenomeManager.preflight()`, and
`AtacSeqProcessor.preflight()` report missing tools before expensive work.

## Verification

```bash
python -c "import ncbi_dataset_builder as n; print(n.__version__)"
ncbi-dataset --help
python -c "from ncbi_dataset_builder.processing.atac import AtacSeqProcessor; print(AtacSeqProcessor().preflight())"
```

The final command is expected to fail with an actionable message if ATAC tools
are not installed. Restart notebook kernels after installing or upgrading
Polars so an old module is not mixed with newly installed files.

