# Tests

The test suite checks public behavior at subsystem boundaries. It uses
temporary paths and fake transports/runners for most cases; it does not submit
real Slurm jobs or download complete public datasets.

## Test map

| File | Coverage |
| --- | --- |
| `test_api.py` | Builder configuration, execution creation, resume fingerprints, and high-level methods |
| `test_atac.py` | ATAC commands, outputs, retention, caching, and strict mixed-layout behavior |
| `test_catalog.py` | Run catalog construction, transformations, deduplication, grouping, and conflicts |
| `test_descriptions.py` | Compact description policy and experiment/sample projection |
| `test_distributed_scheduler.py` | Coordinator-side staging and ready-only distributed CPU allocation |
| `test_execution_config.py` | Validation and serialization for all resource/storage/queue configs |
| `test_fastq.py` | SRA/GEO staging, conversion, manifests, checksums, retries, and layouts |
| `test_genomes.py` | Candidate parsing, policy ranking, custom references, caches, and locks |
| `test_metadata.py` | Entrez/SRA/BioSample parsing, relationships, caching, saving, and enrichment |
| `test_progress.py` | Text/bar progress and visibility behavior |
| `test_state_and_workspace.py` | Workspace semantics, unit state, stale claims, and manifests |
| `test_docstrings.py` | Public-module/class/function docstring coverage |

## Run the suite

```bash
# Install development dependencies once.
python -m pip install -e ".[dev,progress]"

# Run all tests configured by pyproject.toml.
pytest

# Lint the source and tests.
ruff check src tests examples
```

On a system where the default pytest temporary directory is not writable, use
a project-local base:

```powershell
# Keep both pytest temp files and cache behavior inside a writable project path.
pytest -p no:cacheprovider --basetemp .pytest-tmp
```

## Focused runs

```bash
# Strict mixed-layout and ATAC retention behavior.
pytest tests/test_atac.py

# Resource and three-system validation.
pytest tests/test_execution_config.py

# Workspace resume and atomic state.
pytest tests/test_state_and_workspace.py
```

A focused passing test is evidence only for that area. Before release, run the
complete test and Ruff commands in the project’s intended environment.

## External-tool boundary

Unit tests mock command execution where practical. Real operational validation
still needs:

- NCBI network access and credentials for live Entrez calls;
- SRA Toolkit for real SRA materialization;
- NCBI Datasets CLI for genome download;
- fastp, Bowtie2, samtools, and deepTools for ATAC processing; and
- an actual Slurm installation for submission and scheduler behavior.
