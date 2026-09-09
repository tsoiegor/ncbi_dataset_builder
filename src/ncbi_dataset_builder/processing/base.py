from __future__ import annotations

import importlib
from typing import Protocol

from ..models import FastqSet, GenomeRef, ProcessingResult


class Processor(Protocol):
    """Protocol for assay processors that consume FASTQ, genome, and thread count."""

    def __call__(self, fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
        """Process *fastq* against *genome* using at most *threads* threads."""

        ...


def load_processor(reference: str) -> Processor:
    """Load processor *reference* in ``package.module:callable`` form."""

    if ":" not in reference:
        raise ValueError("Processor reference must have the form 'package.module:callable'")
    module_name, attribute_name = reference.split(":", 1)
    module = importlib.import_module(module_name)
    processor = getattr(module, attribute_name)
    if not callable(processor):
        raise TypeError(f"Processor is not callable: {reference}")
    return processor
