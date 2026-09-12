# Tests

The suite is organized by the public boundary it verifies:

- catalog transformations, grouping, and conflict detection;
- normalized metadata, biological descriptions, FASTQ, and genome acquisition;
- system-specific resource validation and Slurm script generation;
- local sample streaming, restart behavior, durable state, and publication;
- ATAC commands, mixed-layout protection, and every intermediate-retention
  switch;
- public source docstring coverage.

Run from the project root:

```bash
python -m pytest -q -p no:cacheprovider
ruff check src/ncbi_dataset_builder tests examples
```

Windows PowerShell with an existing environment:

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m pytest -q -p no:cacheprovider --basetemp "$PWD\pytest-run"
ruff check src/ncbi_dataset_builder tests examples
```

An editable `.[dev]` installation makes the explicit `PYTHONPATH` unnecessary.

Tests use fake transports, providers, genomes, command runners, and scheduler
responses. They do not need NCBI network access or Slurm.
