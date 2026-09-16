"""Minimal custom processor for local or Slurm execution."""

from __future__ import annotations

import hashlib

from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingContext, ProcessingResult


def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    context: ProcessingContext,
) -> ProcessingResult:
    """Hash sample input and write a reproducible text result.

    Args:
        fastq: Validated local FASTQ paths and acquisition provenance.
        genome: Selected local genome reference.
        context: Pipeline identity, directories, log, and assigned CPUs.

    Returns:
        A successful result declaring the non-empty text output.
    """

    fastq.validate()
    genome.validate()
    digest = hashlib.sha256()
    inputs = (*fastq.read1, *fastq.read2, *fastq.single)
    for path in inputs:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    context.output_dir.mkdir(parents=True, exist_ok=True)
    output = context.output_dir / f"{context.unit_id}.txt"
    output.write_text(
        f"sample={context.unit_id}\n"
        f"assembly={genome.accession}\n"
        f"threads={context.threads}\n"
        f"fastq_sha256={digest.hexdigest()}\n",
        encoding="utf-8",
    )
    return ProcessingResult(
        success=True,
        outputs={"summary": output},
        metrics={"input_files": len(inputs), "threads": context.threads},
    )
