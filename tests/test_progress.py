import logging
from io import StringIO

from ncbi_dataset_builder.progress import ProgressReporter


def test_text_progress_reports_cache_counts_and_completion():
    stream = StringIO()
    reporter = ProgressReporter(
        use_bars=False,
        stream=stream,
        text_interval_seconds=60,
    )

    reporter.cache_summary("Metadata", cached=12, missing=3, unit="samples")
    reporter.network_summary("SRA XML", cached=4, to_fetch=2)
    assert list(reporter.track(range(3), "Parse records", unit="records")) == [0, 1, 2]

    output = stream.getvalue()
    assert "12 samples loaded from cache; 3 samples require work" in output
    assert "4 request batches loaded from raw cache; 2 request batches will be fetched" in output
    assert "Parse records: started (0/3 records" in output
    assert "Parse records: complete (3/3 records" in output


def test_disabled_progress_has_no_direct_output():
    stream = StringIO()
    reporter = ProgressReporter(enabled=False, use_bars=False, stream=stream)

    reporter.message("hidden")
    list(reporter.track(range(2), "Hidden work"))

    assert stream.getvalue() == ""


def test_disabled_progress_still_emits_logging_events(caplog):
    """Disabled display retains task start and completion in package logs."""

    reporter = ProgressReporter(enabled=False, use_bars=False)

    with caplog.at_level(logging.INFO, logger="ncbi_dataset_builder"):
        list(reporter.track(range(2), "Logged work", unit="items"))

    messages = [record.getMessage() for record in caplog.records]
    assert any("Logged work: started" in message for message in messages)
    assert any("Logged work: complete (2/2 items" in message for message in messages)


def test_minimum_level_hides_console_info_but_retains_log_records(caplog):
    """A task scope hides console INFO while retaining it for unit log routing."""

    outer_stream = StringIO()
    inner_stream = StringIO()
    outer = ProgressReporter(use_bars=False, stream=outer_stream)
    inner = ProgressReporter(use_bars=False, stream=inner_stream)

    with (
        caplog.at_level(logging.INFO, logger="ncbi_dataset_builder"),
        outer.minimum_level(logging.WARNING),
    ):
        inner.message("hidden task phase")
        list(inner.track(range(2), "hidden task progress"))
        inner.message("visible task warning", level=logging.WARNING)

    assert outer_stream.getvalue() == ""
    assert inner_stream.getvalue().strip() == "visible task warning"
    messages = [record.getMessage() for record in caplog.records]
    assert "visible task warning" in messages
    assert any("hidden task phase" in message for message in messages)
