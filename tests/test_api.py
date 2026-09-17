import json
import logging
from io import StringIO

import pytest

import ncbi_dataset_builder.api as api_module
from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    GenomeSelectionPolicy,
    LocalExecution,
    ProgressReporter,
    QueuePolicy,
    QuotaStorage,
    RunCatalog,
    SlurmSingleNodeExecution,
    SraToolkitProvider,
)
from ncbi_dataset_builder.cli.main import _print_status
from ncbi_dataset_builder.metadata import MetadataBundle
from ncbi_dataset_builder.models import FastqLayout, FastqSet, GenomeRef, ProcessingResult


class FakeFastqProvider:
    def fetch(self, unit, destination, *, threads):
        root = destination / unit.unit_id
        root.mkdir(parents=True, exist_ok=True)
        read = root / "reads.fastq.gz"
        read.write_bytes(b"reads")
        return FastqSet(
            layout=FastqLayout.SINGLE,
            run_accessions=unit.run_accessions,
            single=(read,),
            provider_metadata={"download_threads": threads},
        )


class FakeGenomeManager:
    def __init__(self, root):
        self.root = root

    def resolve(self, *, taxid, scientific_name, pin=None):
        self.root.mkdir(parents=True, exist_ok=True)
        fasta = self.root / f"{taxid}.fna"
        fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
        return GenomeRef(taxid, scientific_name or "unknown", pin or "GCF_TEST", fasta, "sha")


def processor(fastq, genome, context):
    context.output_dir.mkdir(parents=True, exist_ok=True)
    output = context.output_dir / f"{context.unit_id}.bw"
    output.write_bytes(f"{genome.accession}:{context.threads}".encode())
    return ProcessingResult(
        True,
        outputs={"coverage": output},
        metrics={"cpus": context.threads},
    )


def description_processor(fastq, genome, context):
    del fastq, genome
    description = context.output_dir / f"{context.unit_id}.json"
    value = json.loads(description.read_text(encoding="utf-8"))
    assert value["ID"] == context.unit_id
    artifact = context.output_dir / f"{context.unit_id}.txt"
    artifact.write_text("processed", encoding="utf-8")
    return ProcessingResult(
        True,
        outputs={"description": description, "artifact": artifact},
    )


description_processor.description_profile = "training"


def catalog(*experiment_ids):
    return RunCatalog.from_records(
        [
            {
                "Run": f"SRR{index}",
                "Experiment": experiment,
                "SRA Sample": f"SRS{index}",
                "BioSample": f"SAMN{index}",
                "TaxID": 9606,
                "ScientificName": "Homo sapiens",
                "size_MB": 1,
            }
            for index, experiment in enumerate(experiment_ids, 1)
        ]
    )


def builder(tmp_path):
    return DatasetBuilder(
        BuilderConfig(tmp_path, show_progress=False),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )


def test_local_sample_queue_builds_and_resumes(tmp_path):
    instance = builder(tmp_path)
    execution = LocalExecution(
        total_cpus=8,
        min_cpus_per_job=2,
        max_cpus_per_job=4,
        max_running_jobs=2,
        storage=FilesystemStorage(reserve_free_gb=0),
    )
    queue = QueuePolicy(download_workers=2, cleanup="after_success")
    first = instance.build(catalog("SRX1", "SRX2"), processor, execution=execution, queue=queue)
    assert first.succeeded == 2
    assert first.failed == 0
    assert not (tmp_path / "runtime" / "fastq" / "SRX1").exists()
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 3
    assert manifest["output_root"] == "output"
    assert manifest["units"]["SRX1"]["artifacts"]["coverage"]["path"] == "SRX1/SRX1.bw"
    second = instance.build(catalog("SRX1", "SRX2"), processor, execution=execution, queue=queue)
    assert second.skipped == 2
    assert instance.status()["counts"]["succeeded"] == 2
    assert [item["experiment_id"] for item in instance.status()["experiments"]] == [
        "SRX1",
        "SRX2",
    ]
    assert list((tmp_path / "runtime" / "executions").glob("execution-*.json"))
    assert not any((tmp_path / name).exists() for name in ("bigWig", "descriptions", "genomes"))


def test_workspace_is_lazy_and_custom_output_root_is_used(tmp_path):
    custom_output = tmp_path / "dataset"
    instance = DatasetBuilder(
        BuilderConfig(tmp_path, output_dir=custom_output, show_progress=False),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )
    assert {path.name for path in tmp_path.iterdir()} == {"runtime"}

    instance.build(catalog("SRX1"), processor)

    assert (custom_output / "SRX1" / "SRX1.bw").is_file()
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["output_root"] == "dataset"
    reopened = DatasetBuilder(
        BuilderConfig(tmp_path, show_progress=False),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
    )
    assert reopened.workspace.output == custom_output.resolve()


def test_processing_result_rejects_artifact_outside_output_dir(tmp_path):
    outside = tmp_path / "outside.bw"
    outside.write_bytes(b"track")
    result = ProcessingResult(True, outputs={"coverage": outside})

    with pytest.raises(ValueError, match="outside its output directory"):
        result.validate(output_dir=tmp_path / "output" / "SRX1")


def test_changed_processor_identity_rebuilds_sample(tmp_path):
    instance = builder(tmp_path)
    instance.build(catalog("SRX1"), processor, processor_id="processor-v1")
    rebuilt = instance.build(catalog("SRX1"), processor, processor_id="processor-v2")
    assert rebuilt.succeeded == 1
    assert list((tmp_path / "runtime" / "state" / "history" / "SRX1").glob("*.json"))


def test_catalog_loads_from_csv_without_ncbi_credentials(tmp_path):
    csv = tmp_path / "runs.csv"
    csv.write_text(
        "Run,Experiment,TaxID,ScientificName,size_MB\nSRR1,SRX1,9606,Homo sapiens,1.5\n",
        encoding="utf-8",
    )
    loaded = DatasetBuilder.load_runs(csv)
    assert loaded.processing_units()[0].total_size_gb == 0.0015


def test_fetch_runs_uses_ncbi_client_and_catalog_cache(tmp_path):
    class FakeSra:
        def __init__(self):
            self.calls = 0

        def fetch_runinfo(self, query, *, refresh):
            self.calls += 1
            assert query == '"ATAC-seq"[Strategy]'
            return catalog("SRX1")

    instance = builder(tmp_path)
    instance.sra = FakeSra()
    first = instance.fetch_runs('"ATAC-seq"[Strategy]')
    second = instance.fetch_runs('"ATAC-seq"[Strategy]')
    assert first.frame.equals(second.frame)
    assert instance.sra.calls == 1
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8")) if (tmp_path / "manifest.json").exists() else None
    assert manifest is None


def test_enrich_metadata_fetches_only_missing_experiments(tmp_path):
    class IncrementalSra:
        def __init__(self):
            self.calls = []

        def fetch_packages(self, accessions, **kwargs):
            self.calls.append((list(accessions), kwargs))
            result = MetadataBundle()
            for accession in accessions:
                suffix = accession.removeprefix("SRX")
                sample = f"SRS{suffix}"
                biosample = f"SAMN{suffix}"
                result.packages.append(
                    {
                        "accession": accession,
                        "experiment_accession": accession,
                        "sra_sample_accession": sample,
                        "run_accessions": [f"SRR{suffix}"],
                    }
                )
                result.experiments.append(
                    {"accession": accession, "library": {"strategy": "ATAC-seq"}}
                )
                result.sra_samples.append(
                    {
                        "accession": sample,
                        "biosample": biosample,
                        "organism": "Homo sapiens",
                    }
                )
                result.runs.append({"accession": f"SRR{suffix}"})
            return result

    class IncrementalBioSample:
        def __init__(self):
            self.calls = []

        def fetch(self, accessions, **kwargs):
            self.calls.append((list(accessions), kwargs))
            return [{"accession": accession} for accession in accessions]

    instance = builder(tmp_path)
    instance.sra = IncrementalSra()
    instance.biosample = IncrementalBioSample()

    instance.enrich_metadata(catalog("SRX1"))
    scoped = instance.enrich_metadata(catalog("SRX1", "SRX2"))

    assert [call[0] for call in instance.sra.calls] == [["SRX1"], ["SRX2"]]
    assert [row["accession"] for row in scoped.experiments] == ["SRX1", "SRX2"]
    index = json.loads(
        (tmp_path / "runtime" / "metadata" / "metadata_index.json").read_text(
            encoding="utf-8"
        )
    )
    assert set(index["experiments"]) == {"SRX1", "SRX2"}
    assert not (tmp_path / "runtime" / "metadata" / "experiment_descriptions").exists()


def test_processor_can_request_training_description_in_experiment_output(tmp_path):
    instance = builder(tmp_path)
    MetadataBundle(
        packages=[
            {
                "accession": "SRX1",
                "experiment_accession": "SRX1",
                "sra_sample_accession": "SRS1",
                "run_accessions": ["SRR1"],
            }
        ],
        experiments=[{"accession": "SRX1", "library": {"strategy": "ATAC-seq"}}],
        sra_samples=[
            {
                "accession": "SRS1",
                "biosample": "SAMN1",
                "organism": "Homo sapiens",
            }
        ],
        biosamples=[{"accession": "SAMN1"}],
    ).save(tmp_path / "runtime" / "metadata")

    report = instance.build(catalog("SRX1"), description_processor)

    assert report.succeeded == 1
    description = tmp_path / "output" / "SRX1" / "SRX1.json"
    assert json.loads(description.read_text(encoding="utf-8"))["Strategy"] == "ATAC-seq"
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["units"]["SRX1"]["artifacts"]["description"]["path"] == (
        "SRX1/SRX1.json"
    )


def test_changed_description_content_invalidates_successful_unit(tmp_path):
    instance = builder(tmp_path)

    def save_metadata(strategy):
        MetadataBundle(
            packages=[
                {
                    "accession": "SRX1",
                    "experiment_accession": "SRX1",
                    "sra_sample_accession": "SRS1",
                    "run_accessions": ["SRR1"],
                }
            ],
            experiments=[{"accession": "SRX1", "library": {"strategy": strategy}}],
            sra_samples=[
                {
                    "accession": "SRS1",
                    "biosample": "SAMN1",
                    "organism": "Homo sapiens",
                }
            ],
            biosamples=[{"accession": "SAMN1"}],
        ).save(tmp_path / "runtime" / "metadata")

    save_metadata("ATAC-seq")
    first = instance.build(catalog("SRX1"), description_processor)
    save_metadata("DNase-Hypersensitivity")
    second = instance.build(catalog("SRX1"), description_processor)

    assert first.succeeded == 1
    assert second.succeeded == 1
    description = json.loads(
        (tmp_path / "output" / "SRX1" / "SRX1.json").read_text(encoding="utf-8")
    )
    assert description["Strategy"] == "DNase-Hypersensitivity"


def test_description_checksums_use_one_aggregate_progress_task(
    tmp_path,
    monkeypatch,
    caplog,
):
    MetadataBundle(
        packages=[
            {
                "accession": experiment,
                "experiment_accession": experiment,
                "sra_sample_accession": sample,
                "run_accessions": [run],
            }
            for experiment, sample, run in (
                ("SRX1", "SRS1", "SRR1"),
                ("SRX2", "SRS2", "SRR2"),
            )
        ],
        experiments=[
            {"accession": "SRX1", "library": {"strategy": "ATAC-seq"}},
            {"accession": "SRX2", "library": {"strategy": "ATAC-seq"}},
        ],
        sra_samples=[
            {"accession": "SRS1", "biosample": "SAMN1", "organism": "Homo sapiens"},
            {"accession": "SRS2", "biosample": "SAMN2", "organism": "Homo sapiens"},
        ],
        biosamples=[{"accession": "SAMN1"}, {"accession": "SAMN2"}],
    ).save(tmp_path / "runtime" / "metadata")
    stream = StringIO()
    instance = DatasetBuilder(
        BuilderConfig(tmp_path, progress_bars=False),
        fastq_provider=FakeFastqProvider(),
        genome_manager=FakeGenomeManager(tmp_path / "genome-cache"),
        progress=ProgressReporter(use_bars=False, stream=stream),
    )
    original_sha256_file = api_module.sha256_file
    metadata_checksums = 0

    def tracked_sha256_file(path, *args, **kwargs):
        nonlocal metadata_checksums
        if path.name == "metadata.json":
            metadata_checksums += 1
        return original_sha256_file(path, *args, **kwargs)

    monkeypatch.setattr(api_module, "sha256_file", tracked_sha256_file)

    with caplog.at_level(logging.INFO, logger="ncbi_dataset_builder"):
        instance._create_execution(
            catalog("SRX1", "SRX2"),
            description_processor,
            execution=LocalExecution(storage=FilesystemStorage(reserve_free_gb=0)),
            queue=QueuePolicy(),
            group_by=None,
            genome_pins=None,
            query=None,
            processor_id="description-processor-v1",
        )

    output = stream.getvalue()
    assert metadata_checksums == 1
    assert output.count("Checksum experiment descriptions: started") == 1
    assert "Checksum experiment descriptions: complete (2/2 descriptions" in output
    assert "Checksum metadata.json" not in output
    assert not any(
        "Checksum metadata.json" in record.getMessage() for record in caplog.records
    )


def test_manifest_and_default_status_retain_prior_executions(tmp_path):
    instance = builder(tmp_path)
    instance.build(catalog("SRX1", "SRX2"), processor)
    latest = instance.build(catalog("SRX2"), processor)

    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert set(manifest["units"]) == {"SRX1", "SRX2"}
    assert {item["experiment_id"] for item in instance.status()["experiments"]} == {
        "SRX1",
        "SRX2",
    }
    selected = instance.status(latest.execution_id)
    assert [item["experiment_id"] for item in selected["experiments"]] == ["SRX2"]
    assert selected["scope"] == "execution"


def test_slurm_worker_reconstructs_builder_and_sra_provider_configuration(tmp_path):
    config = BuilderConfig(
        tmp_path,
        output_dir=tmp_path / "dataset",
        genome_policy=GenomeSelectionPolicy(allow_atypical=True, prefer_refseq=False),
        prefetch_max_size="7G",
        show_progress=False,
        progress_bars=False,
    )
    provider = SraToolkitProvider(
        retries=6,
        prefetch_max_size="9G",
        prefetch_reset_after_failures=2,
        prefetch_retry_max_delay_seconds=17,
    )
    instance = DatasetBuilder(config, fastq_provider=provider)
    execution = SlurmSingleNodeExecution(
        allocation_cpus=4,
        allocation_memory_gb=8,
        allocation_time_limit="01:00:00",
        storage=QuotaStorage(quota_gb=100),
        memory_gb_per_job=4,
    )
    record = instance._create_execution(
        catalog("SRX1"),
        processor,
        execution=execution,
        queue=QueuePolicy(),
        group_by=None,
        genome_pins=None,
        query=None,
        processor_id="processor-v1",
    )

    restored = DatasetBuilder.from_execution_record(
        workspace=tmp_path,
        record=record,
        email=None,
        ncbi_api_key=None,
    )

    assert restored.config.output_dir == (tmp_path / "dataset").resolve()
    assert restored.config.genome_policy == config.genome_policy
    assert restored.config.prefetch_max_size == "7G"
    assert restored.config.show_progress is False
    assert isinstance(restored.fastq_provider, SraToolkitProvider)
    assert restored.fastq_provider.retries == 6
    assert restored.fastq_provider.prefetch_max_size == "9G"
    assert restored.fastq_provider.prefetch_reset_after_failures == 2
    assert restored.fastq_provider.prefetch_retry_max_delay_seconds == 17


def test_status_formatter_lists_every_experiment_and_inventory(tmp_path, capsys):
    instance = builder(tmp_path)
    instance.build(catalog("SRX1", "SRX2"), processor)

    _print_status(instance.status())

    output = capsys.readouterr().out
    assert "Experiments: succeeded=2" in output
    assert "All experiments" in output
    assert "SRX1" in output and "SRX2" in output
    assert "Metadata" in output and "Genomes" in output
