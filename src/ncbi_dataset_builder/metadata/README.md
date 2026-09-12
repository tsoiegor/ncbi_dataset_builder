# `metadata`

This subpackage retrieves NCBI records, preserves their relationships, and
persists a normalized `MetadataBundle`. `DatasetBuilder.fetch_runs()`,
`fetch_metadata()`, and `enrich_metadata()` provide the usual public entry
points. Direct client use is useful for specialized queries.

## `EntrezClient`

Construct as `EntrezClient(email=..., api_key=None,
tool="ncbi_dataset_builder", cache_dir=None, http=None, progress=None)`. `email`
is required; a key raises the default request rate; `tool` identifies the
client; `cache_dir` enables raw response reuse; and `http`/`progress` accept the
transport and reporter described below.

- `get(endpoint, params, refresh=False, read_cache=True)` returns raw
  E-utilities response bytes. `refresh=True` fetches/replaces the cache;
  `read_cache=False` bypasses reads while still allowing a successful response
  to be written.
- `is_cached(endpoint, params)` checks that cache without a request.
- `statistics()` returns cache-hit and network-request counters.
- `search_history(database, query, refresh=False)` opens a short-lived Entrez
  history search and returns its count/token information.
- `search_ids(database, query, limit=100000, refresh=False)` returns numeric
  UIDs and protects against unexpectedly large result sets.
- `resolve_uids(database, identifiers, field="Accession",
  request_chunk_size=50, refresh=False)` resolves accession strings in bounded
  HTTP request chunks. This is transport chunking, not a processing unit.

## `SraClient`

`SraClient(entrez)` reuses the linked `EntrezClient` for all requests.

- `fetch_runinfo(query, page_size=5000, refresh=False)` returns a
  [`RunCatalog`](../catalog/README.md) for an Entrez SRA query.
- `fetch_runinfo_ids(ids, request_chunk_size=200, refresh=False)` fetches
  RunInfo for explicit accessions or UIDs.
- `parse_packages(payload, include_raw=False, progress=None)` parses SRA package
  XML bytes into a `MetadataBundle`.
- `fetch_packages(accessions, request_chunk_size=100, include_raw=False,
  refresh=False)` resolves and fetches SRA packages.

## `BioSampleClient`

`BioSampleClient(entrez)` provides `parse(payload, include_raw=False,
progress=None)` and `fetch(accessions, request_chunk_size=100,
include_raw=False, refresh=False)`. Records retain identifiers, owner,
attributes, links, dates, status, and optional raw XML trees.

## `MetadataBundle`

`MetadataBundle(packages=[], runs=[], experiments=[], sra_samples=[],
studies=[], submissions=[], biosamples=[], raw_sra_packages=[])` stores each
normalized entity collection and optional complete SRA package trees. Defaults
are independent empty lists.

- `to_dict()` / `from_dict(value)` serialize or restore it.
- `load(path, progress=None)` reads either a metadata directory or JSON path.
- `subset_experiments(accessions)` retains only linked records for the selected
  experiments.
- `descriptions_by_sample(profile="training", policy=None, progress=None)`
  creates compact or full descriptions. `DescriptionPolicy` controls biological
  attribute selection and extra attribute mappings.
- `save_sample_descriptions(directory, profile="training", policy=None,
  progress=None)` writes per-sample JSON.
- `save(directory, description_profile="training", policy=None,
  progress=None)` writes the normalized JSON/JSONL bundle and descriptions.
- `attach_to_runs(catalog)` returns a `RunCatalog` enriched with normalized
  relationships.

`DescriptionPolicy(extra_attributes={})` selects compact biological fields.
`extra_attributes` maps a normalized source attribute such as
`"unknown mechanism"` to an output label such as `"Mechanism"`.
`select_attribute(name, source)` returns the selected output name (or `None`)
and the reason; `source` is `experiment`, `sra_sample`, or `biosample`.

`training_fields_by_experiment(bundle, policy=None, progress=None)` returns the
assay-specific compact fields keyed by experiment and is used by publication
when one SRA Sample contains multiple experiments.
`sanitize_presentation_markup(value)` recursively strips presentation HTML and
decodes entities in strings, lists, tuples, and mappings without summarizing
biological text.

`fetch_metadata_for_accessions(accessions, sra=..., biosample=...,
include_raw=False, refresh=False)` and `fetch_metadata_for_catalog(catalog,
sra=..., biosample=..., include_raw=False, refresh=False)` are the composition
functions used by `DatasetBuilder`.

## HTTP support classes

`HttpResponse(url, status, headers, body)` stores unmodified response data. Its
`text` property decodes UTF-8 with replacement, and `json()` parses the body.

`RequestThrottle(requests_per_second)` serializes callers to a positive rate;
`wait()` blocks until the next slot. `HttpClient(user_agent=...,
requests_per_second=3, retries=5, timeout_seconds=60)` owns one throttle and
performs bounded retry/backoff. `request(url, params=None, headers=None)` sends
a GET and returns `HttpResponse`. These classes are implementation details
unless callers inject a custom transport for testing or site policy.
