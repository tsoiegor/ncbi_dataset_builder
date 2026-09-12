# NCBI metadata

The `ncbi_dataset_builder.metadata` subpackage retrieves NCBI records,
preserves accession relationships, normalizes XML into JSON-compatible entity
collections, and derives compact biological descriptions without replacing the
full source metadata.

`DatasetBuilder.fetch_metadata()` and `enrich_metadata()` are the usual
entry points. Use the classes here for specialized NCBI queries, parsing,
caching, or description policies.

## Module map

| Module | Contents |
| --- | --- |
| `core.py` | `MetadataBundle`, Entrez/SRA/BioSample clients, XML normalization, and composition functions |
| `descriptions.py` | `DescriptionPolicy` and compact training-field projection |
| `http.py` | Throttled/retrying HTTP support types |
| `__init__.py` | Public metadata exports |

## Metadata hierarchy

The bundle remains run- and relationship-aware:

| Entity | Typical stable accession | Stored collection |
| --- | --- | --- |
| Study / BioProject | `SRP...` / `PRJNA...` | `studies` |
| Experiment | `SRX...` | `experiments` |
| SRA Sample | `SRS...` | `sra_samples` |
| BioSample | `SAMN...` | `biosamples` |
| Run | `SRR...` | `runs` |
| Submission | `SRA...` | `submissions` |
| Cross-entity package links | several | `packages` |

RunInfo’s human-readable `SampleName` is not a replacement for an SRA Sample
or BioSample accession.

# NCBI clients

## `EntrezClient`

```python
EntrezClient(
    *,
    email,
    api_key=None,
    tool="ncbi_dataset_builder",
    cache_dir=None,
    http=None,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `email: str` | Required NCBI contact email. Empty values are rejected. |
| `api_key: str | None` | Optional key; default request rate is higher when present. |
| `tool: str` | Tool name sent to NCBI. |
| `cache_dir: Path | None` | Optional raw-response cache. The builder uses `workspace/metadata_cache/`. |
| `http: HttpClient | None` | Injectable transport; a configured default is created otherwise. |
| `progress: ProgressReporter | None` | Optional cache/network event reporter. |

### Raw requests

#### `get(endpoint, params, *, refresh=False, read_cache=True) -> bytes`

| Argument | Meaning |
| --- | --- |
| `endpoint: str` | E-utilities endpoint name such as `"esearch.fcgi"`. |
| `params: dict` | Request parameters. Identity fields are added by the client. |
| `refresh: bool` | Bypass and replace a cached response. |
| `read_cache: bool` | Allow a cache hit. False still permits writing a successful new response. |

The cache key is derived from endpoint and request parameters. Only non-empty
successful responses are published.

| Method | Result |
| --- | --- |
| `is_cached(endpoint, params)` | Whether a non-empty matching raw response exists. |
| `statistics()` | Thread-safe cumulative cache-hit and network-request counts. |

### Search helpers

| Method | Arguments and behavior |
| --- | --- |
| `search_history(database, query, *, refresh=False)` | Open an Entrez history and return count, query key, and WebEnv data. History responses are not read from cache because tokens expire. |
| `search_ids(database, query, *, limit=100000, refresh=False)` | Return numeric UIDs and reject result counts above `limit`. |
| `resolve_uids(database, identifiers, *, field="Accession", request_chunk_size=50, refresh=False)` | Resolve accession-like identifiers to UIDs using bounded request chunks. |

`request_chunk_size` controls HTTP query size. It has no relationship to a
processing unit or scheduler concurrency.

## `SraClient`

```python
SraClient(entrez)
```

`entrez: EntrezClient` is reused for every SRA request.

| Method | Arguments and result |
| --- | --- |
| `fetch_runinfo(query, *, page_size=5000, refresh=False)` | Fetch every RunInfo page for an Entrez SRA expression and return `RunCatalog`. |
| `fetch_runinfo_ids(ids, *, request_chunk_size=200, refresh=False)` | Fetch RunInfo for explicit SRA accessions or UIDs in bounded chunks. |
| `parse_packages(payload, *, include_raw=False, progress=None)` | Static parser from SRA Experiment Package XML bytes to `MetadataBundle`. |
| `fetch_packages(accessions, *, request_chunk_size=100, include_raw=False, refresh=False)` | Resolve/fetch package XML and merge normalized records. |

`include_raw=True` stores complete parsed package trees in
`raw_sra_packages`; normalized collections are always produced.

## `BioSampleClient`

```python
BioSampleClient(entrez)
```

| Method | Arguments and result |
| --- | --- |
| `parse(payload, *, include_raw=False, progress=None)` | Static parser from BioSample XML bytes to normalized record dictionaries. |
| `fetch(accessions, *, request_chunk_size=100, include_raw=False, refresh=False)` | Resolve/fetch BioSamples and return normalized records. |

Records retain stable identifiers, owner, attributes, links, dates, status,
and optional raw XML trees.

# `MetadataBundle`

```python
MetadataBundle(
    packages=[],
    runs=[],
    experiments=[],
    sra_samples=[],
    studies=[],
    submissions=[],
    biosamples=[],
    raw_sra_packages=[],
)
```

Each default is an independent list.

| Field | Meaning |
| --- | --- |
| `packages` | Relationships among experiments, samples, studies, submissions, and runs. |
| `runs` | Normalized SRA run records. |
| `experiments` | Normalized experiment and library records. |
| `sra_samples` | Normalized SRA Sample records. |
| `studies` | Normalized SRA Study records. |
| `submissions` | Normalized submission records. |
| `biosamples` | Linked BioSample records. |
| `raw_sra_packages` | Optional complete parsed package trees. |

### Serialization and loading

| Method | Behavior |
| --- | --- |
| `to_dict()` | Return all collections in one JSON-compatible mapping. |
| `from_dict(value)` | Restore a bundle from a mapping. Missing collections become empty. |
| `load(path, *, progress=None)` | Read `path` as JSON, or read `path/metadata.json` when a directory is supplied. |

### `subset_experiments(accessions) -> MetadataBundle`

Return a new bundle containing the selected experiment accessions and every
linked run, SRA Sample, study, submission, BioSample, package, and optional raw
package record. Unknown selections simply contribute no linked records.

### Description methods

| Method | Arguments and result |
| --- | --- |
| `descriptions_by_sample(*, profile="training", policy=None, progress=None)` | Build description dictionaries keyed by SRA Sample accession. |
| `save_sample_descriptions(directory, *, profile="training", policy=None, progress=None)` | Atomically write one `<sample>.json` file per description. |
| `save(directory, *, description_profile="training", policy=None, progress=None)` | Write `metadata.json`, collection NDJSON files, and `sample_descriptions/`. |

`profile="training"` uses the compact biological projection.
`profile="full"` retains the normalized relationship-rich sample
description. Other values raise.

### `attach_to_runs(catalog) -> RunCatalog`

Join normalized metadata columns onto the source
[`RunCatalog`](../catalog/README.md) using entity accessions. The result is a
new catalog with an audit event; the bundle and original catalog are unchanged.

# Description projection

## `DescriptionPolicy`

```python
DescriptionPolicy(
    extra_attributes={},
)
```

| Argument | Meaning |
| --- | --- |
| `extra_attributes: Mapping[str, str]` | Exact normalized source attribute name to desired output label. |

Built-in rules retain relevant biological sample properties and experiment
attributes while filtering null/placeholder values. Protocols and abstracts
are not summarized or truncated.

### `select_attribute(name, source)`

Normalize `name` case-insensitively, inspect its source, and return
`(output_name_or_none, rationale)`.

`source` distinguishes `"experiment"`, `"sra_sample"`, and
`"biosample"`. Experiment attributes remain available because they can carry
assay conditions. Exact `extra_attributes` aliases extend the selection.

```python
from ncbi_dataset_builder import DescriptionPolicy

policy = DescriptionPolicy(
    extra_attributes={
        # Source names are normalized before exact lookup.
        "unknown mechanism": "Mechanism",
    }
)

bundle = builder.enrich_metadata(
    catalog,
    description_profile="training",
    description_policy=policy,
)
```

## Projection functions

### `training_fields_by_experiment(bundle, *, policy=None) -> dict`

Return compact library, assay, and study fields keyed by Experiment accession.
Repeated package relations must describe each experiment consistently or the
function raises `ValueError`.

The publisher uses this mapping when a sample-level description contains
multiple experiment entries.

### `training_descriptions(bundle, *, policy=None, progress=None) -> dict`

Module-level implementation function that builds compact descriptions by SRA
Sample. Identical repeated experiments collapse; fields shared across
experiments appear once at sample level; varying fields remain under
`Experiments`.

# Composition functions

## `fetch_metadata_for_accessions(...)`

```python
fetch_metadata_for_accessions(
    accessions,
    *,
    sra,
    biosample,
    include_raw=False,
    refresh=False,
) -> MetadataBundle
```

Fetch SRA packages, discover linked BioSample accessions, fetch those records,
and return one merged bundle.

## `fetch_metadata_for_catalog(...)`

```python
fetch_metadata_for_catalog(
    catalog,
    *,
    sra,
    biosample,
    include_raw=False,
    refresh=False,
) -> MetadataBundle
```

Fetch package metadata for accessions present in a `RunCatalog`, attach
linked BioSamples, and scope the result to catalog experiments.

## Other public helpers

| Function | Behavior |
| --- | --- |
| `xml_to_dict(element)` | Recursively convert an ElementTree node without losing repeated children or attributes. Module-only, not exported by `metadata.__init__`. |
| `sanitize_presentation_markup(value)` | Recursively strip presentation HTML and decode entities in strings, mappings, lists, and tuples without summarizing content. |
| `normalized_name(name)` | Normalize an attribute name for policy matching. Module-only. |

# HTTP support

The types below live in `metadata.http`. They support client injection and
tests; ordinary users normally configure `EntrezClient` instead.

## `HttpResponse`

```python
HttpResponse(
    url,
    status,
    headers,
    body,
)
```

| Field/property/method | Meaning |
| --- | --- |
| `url: str` | Final URL after redirects. |
| `status: int` | HTTP status code. |
| `headers: dict[str, str]` | Response headers. |
| `body: bytes` | Unmodified response bytes. |
| `text` | UTF-8 decoding with replacement for invalid bytes. |
| `json()` | Parse the body as JSON. |

## `RequestThrottle`

`RequestThrottle(requests_per_second)` requires a positive rate.
`wait()` serializes callers and blocks until the next request slot.

## `HttpClient`

```python
HttpClient(
    *,
    user_agent,
    requests_per_second=3.0,
    retries=5,
    timeout_seconds=60.0,
)
```

| Argument | Meaning |
| --- | --- |
| `user_agent: str` | Required request identity. |
| `requests_per_second: float` | Positive shared throttle rate. |
| `retries: int` | Retries after the initial attempt. |
| `timeout_seconds: float` | Per-request timeout. |

`request(url, *, params=None, headers=None) -> HttpResponse` performs a GET,
combines query parameters and headers, throttles callers, and retries network
failures plus status 408, 425, 429, 500, 502, 503, and 504 with bounded
backoff.

Those status codes are exposed as the class constant `RETRYABLE_STATUS`.
Change retry count and timeout through constructor arguments; treat the status
set as implementation policy rather than mutable user configuration.

## Internal parser class

`_MarkupTextExtractor` is a private `HTMLParser` used by
`sanitize_presentation_markup()`. Its `handle_data(data)` method collects
visible fragments. It is not compatibility API.

# Example

```python
from pathlib import Path

from ncbi_dataset_builder import BioSampleClient, EntrezClient, SraClient
from ncbi_dataset_builder.metadata import fetch_metadata_for_accessions

entrez = EntrezClient(
    email="researcher@example.org",
    cache_dir=Path("/data/ncbi-workspace/metadata_cache"),
)
sra = SraClient(entrez)
biosample = BioSampleClient(entrez)

bundle = fetch_metadata_for_accessions(
    ["SRX123456"],
    sra=sra,
    biosample=biosample,
    include_raw=False,   # Normalized records are still preserved.
    refresh=False,       # Reuse valid raw responses.
)

bundle.save(
    Path("/data/ncbi-workspace/metadata"),
    description_profile="training",
)
```
