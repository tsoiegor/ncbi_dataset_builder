import json

from ncbi_dataset_builder.metadata import (
    BioSampleClient,
    EntrezClient,
    SraClient,
    fetch_metadata_for_accessions,
    sanitize_legacy_metadata,
    sanitize_presentation_markup,
)

SRA_XML = b"""<EXPERIMENT_PACKAGE_SET><EXPERIMENT_PACKAGE>
  <EXPERIMENT alias="GSM3756614" accession="SRX5809925">
    <IDENTIFIERS><PRIMARY_ID>SRX5809925</PRIMARY_ID></IDENTIFIERS>
    <TITLE>GSM3756614: wt_sphere_atac_rep1; Danio rerio; ATAC-seq</TITLE>
    <STUDY_REF accession="SRP197260" refname="GSE130944"/>
    <DESIGN><DESIGN_DESCRIPTION>Open chromatin</DESIGN_DESCRIPTION>
      <SAMPLE_DESCRIPTOR accession="SRS4739189"/>
      <LIBRARY_DESCRIPTOR>
        <LIBRARY_STRATEGY>ATAC-seq</LIBRARY_STRATEGY>
        <LIBRARY_SOURCE>GENOMIC</LIBRARY_SOURCE>
        <LIBRARY_SELECTION>other</LIBRARY_SELECTION>
        <LIBRARY_LAYOUT><PAIRED nominal_length="0"/></LIBRARY_LAYOUT>
        <LIBRARY_CONSTRUCTION_PROTOCOL>ATAC protocol &amp; cleanup</LIBRARY_CONSTRUCTION_PROTOCOL>
      </LIBRARY_DESCRIPTOR>
    </DESIGN>
    <PLATFORM><ILLUMINA><INSTRUMENT_MODEL>Illumina HiSeq 2500</INSTRUMENT_MODEL></ILLUMINA></PLATFORM>
    <EXPERIMENT_ATTRIBUTES><EXPERIMENT_ATTRIBUTE><TAG>GEO Accession</TAG><VALUE>GSM3756614</VALUE></EXPERIMENT_ATTRIBUTE></EXPERIMENT_ATTRIBUTES>
  </EXPERIMENT>
  <SUBMISSION alias="GEO: GSE130944" broker_name="GEO" center_name="GEO" accession="SRA884593">
    <IDENTIFIERS><PRIMARY_ID>SRA884593</PRIMARY_ID></IDENTIFIERS>
  </SUBMISSION>
  <Organization type="center"><Name abbr="GEO">NCBI</Name><Contact email="geo-group@ncbi.nlm.nih.gov"><Name><First>Geo</First><Last>Curators</Last></Name></Contact></Organization>
  <STUDY center_name="GEO" alias="GSE130944" accession="SRP197260">
    <IDENTIFIERS><PRIMARY_ID>SRP197260</PRIMARY_ID><EXTERNAL_ID namespace="BioProject">PRJNA542075</EXTERNAL_ID><EXTERNAL_ID namespace="GEO">GSE130944</EXTERNAL_ID></IDENTIFIERS>
    <DESCRIPTOR><STUDY_TITLE>A study</STUDY_TITLE><STUDY_TYPE existing_study_type="Other"/><STUDY_ABSTRACT>Text &amp; design</STUDY_ABSTRACT></DESCRIPTOR>
    <STUDY_LINKS><STUDY_LINK><XREF_LINK><DB>pubmed</DB><ID>31940339</ID></XREF_LINK></STUDY_LINK></STUDY_LINKS>
  </STUDY>
  <SAMPLE alias="GSM3756614" accession="SRS4739189">
    <IDENTIFIERS><PRIMARY_ID>SRS4739189</PRIMARY_ID><EXTERNAL_ID namespace="BioSample">SAMN11608754</EXTERNAL_ID><EXTERNAL_ID namespace="GEO">GSM3756614</EXTERNAL_ID></IDENTIFIERS>
    <TITLE>wt_sphere_atac_rep1</TITLE><SAMPLE_NAME><TAXON_ID>7955</TAXON_ID><SCIENTIFIC_NAME>Danio rerio</SCIENTIFIC_NAME></SAMPLE_NAME>
    <SAMPLE_ATTRIBUTES><SAMPLE_ATTRIBUTE><TAG>tissue</TAG><VALUE>whole embryo</VALUE></SAMPLE_ATTRIBUTE></SAMPLE_ATTRIBUTES>
  </SAMPLE>
  <Pool><Member accession="SRS4739189" spots="30" bases="300"><IDENTIFIERS><EXTERNAL_ID namespace="BioSample">SAMN11608754</EXTERNAL_ID></IDENTIFIERS></Member></Pool>
  <RUN_SET runs="2" spots="30" bases="300" bytes="123">
    <RUN alias="lane1" accession="SRR9032674" total_spots="10" total_bases="100" size="41" published="2020-01-08" is_public="true">
      <IDENTIFIERS><PRIMARY_ID>SRR9032674</PRIMARY_ID></IDENTIFIERS><EXPERIMENT_REF accession="SRX5809925"/>
      <SRAFiles><SRAFile filename="SRR9032674" size="41" semantic_name="SRA Normalized"><Alternatives url="https://example.test/SRR9032674" org="AWS"/></SRAFile></SRAFiles>
      <Statistics nreads="2" nspots="10"/>
    </RUN>
    <RUN alias="lane2" accession="SRR9032675" total_spots="20" total_bases="200" size="82" published="2020-01-08"><EXPERIMENT_REF accession="SRX5809925"/></RUN>
  </RUN_SET>
</EXPERIMENT_PACKAGE></EXPERIMENT_PACKAGE_SET>"""


BIOSAMPLE_XML = b"""<BioSampleSet><BioSample access="public" publication_date="2020-01-09" last_update="2020-01-10" submission_date="2019-05-09" accession="SAMN11608754" id="11608754">
  <Ids><Id db="BioSample" is_primary="1">SAMN11608754</Id><Id db="SRA">SRS4739189</Id><Id db="GEO">GSM3756614</Id></Ids>
  <Description><Title>wt_sphere_atac_rep1</Title><Organism taxonomy_id="7955" taxonomy_name="Danio rerio"/></Description>
  <Owner><Name>A lab</Name><Contacts><Contact email="owner@example.test"><Name><First>A</First><Last>Person</Last></Name></Contact></Contacts></Owner>
  <Models><Model>Generic</Model></Models><Package display_name="Generic">Generic.1.0</Package>
  <Attributes><Attribute attribute_name="tissue" harmonized_name="tissue">whole embryo</Attribute><Attribute attribute_name="source name" harmonized_name="tissue">embryo</Attribute></Attributes>
  <Links><Link type="entrez" target="bioproject" label="PRJNA542075">542075</Link></Links><Status status="live" when="2020-01-08"/>
</BioSample></BioSampleSet>"""


class FakeEntrez:
    def __init__(self):
        self.resolutions = []
        self.fetches = []

    def resolve_uids(self, database, identifiers, **kwargs):
        self.resolutions.append((database, list(identifiers), kwargs))
        return ["7807635"] if database == "sra" else ["11608754"]

    def get(self, endpoint, params):
        assert endpoint == "efetch.fcgi"
        self.fetches.append(params)
        if params["db"] == "sra":
            assert params["id"] == "7807635"
            return SRA_XML
        assert params["id"] == "11608754"
        return BIOSAMPLE_XML


class RecordingResolver:
    def __init__(self):
        self.queries = []

    def search_ids(self, database, query):
        self.queries.append((database, query))
        return ["7807635"] if "SRS4739189" in query else ["7807636"]


def test_accessions_are_resolved_to_numeric_uids_before_efetch():
    resolver = RecordingResolver()
    result = EntrezClient.resolve_uids(
        resolver,
        "sra",
        ["SRS4739189", "123", "SRR9032674", "SRS4739189"],
        batch_size=1,
    )
    assert result == ["123", "7807635", "7807636"]
    assert resolver.queries == [
        ("sra", '"SRS4739189"[Accession]'),
        ("sra", '"SRR9032674"[Accession]'),
    ]


def test_structured_sra_xml_preserves_page_entities_and_resolves_accession():
    entrez = FakeEntrez()
    bundle = SraClient(entrez).fetch_packages(["SRS4739189"], include_raw=True)

    assert entrez.resolutions[0][0:2] == ("sra", ["SRS4739189"])
    assert bundle.packages[0]["run_accessions"] == ["SRR9032674", "SRR9032675"]
    assert bundle.packages[0]["totals"] == {
        "runs": 2,
        "spots": 30,
        "bases": 300,
        "bytes": 123,
    }
    assert bundle.studies[0]["accession"] == "SRP197260"
    assert bundle.studies[0]["bioproject"] == "PRJNA542075"
    assert bundle.studies[0]["geo"] == "GSE130944"
    assert bundle.studies[0]["abstract"] == "Text & design"
    assert bundle.studies[0]["pubmed_ids"] == ["31940339"]
    assert bundle.experiments[0]["accession"] == "SRX5809925"
    assert bundle.experiments[0]["library"]["strategy"] == "ATAC-seq"
    assert bundle.experiments[0]["library"]["layout"] == "PAIRED"
    assert bundle.experiments[0]["library"]["construction_protocol"] == "ATAC protocol & cleanup"
    assert bundle.experiments[0]["platform"]["instrument_model"] == "Illumina HiSeq 2500"
    assert bundle.experiments[0]["attributes"]["GEO Accession"] == "GSM3756614"
    assert bundle.sra_samples[0]["biosample"] == "SAMN11608754"
    assert bundle.submissions[0]["organization"]["name"] == "NCBI"
    assert bundle.runs[0]["size_bytes"] == 41
    assert bundle.runs[0]["files"][0]["alternatives"][0]["org"] == "AWS"
    assert bundle.raw_sra_packages


def test_biosample_xml_keeps_identifiers_dates_owner_and_repeated_attributes():
    record = BioSampleClient(FakeEntrez()).fetch(["SAMN11608754"], include_raw=True)[0]
    assert record["title"] == "wt_sphere_atac_rep1"
    assert record["organism"] == "Danio rerio"
    assert record["taxid"] == "7955"
    assert record["sra_sample"] == "SRS4739189"
    assert record["geo"] == "GSM3756614"
    assert record["publication_date"] == "2020-01-09"
    assert record["owner"]["contacts"][0]["email"] == "owner@example.test"
    assert record["package"]["name"] == "Generic.1.0"
    assert record["attributes"]["tissue"] == ["whole embryo", "embryo"]
    assert record["status"]["status"] == "live"
    assert "raw" in record


def test_combined_sample_description_and_persistence(tmp_path):
    entrez = FakeEntrez()
    bundle = fetch_metadata_for_accessions(
        ["SRS4739189"],
        sra=SraClient(entrez),
        biosample=BioSampleClient(entrez),
        include_raw=True,
    )
    description = bundle.descriptions_by_sample(profile="full")["SRS4739189"]
    assert description["experiments"][0]["library"]["strategy"] == "ATAC-seq"
    assert description["studies"][0]["accession"] == "SRP197260"
    assert description["biosample"]["accession"] == "SAMN11608754"
    assert [run["accession"] for run in description["runs"]] == [
        "SRR9032674",
        "SRR9032675",
    ]

    bundle.save(tmp_path, description_profile="full")
    saved = json.loads(
        (tmp_path / "sample_descriptions" / "SRS4739189.json").read_text(encoding="utf-8")
    )
    assert saved["submissions"][0]["accession"] == "SRA884593"
    assert (tmp_path / "packages.ndjson").stat().st_size > 0
    assert (tmp_path / "experiments.ndjson").stat().st_size > 0
    assert (tmp_path / "biosamples.ndjson").stat().st_size > 0


def test_legacy_markup_sanitizer_removes_tags_decodes_entities_and_preserves_unicode(
    tmp_path,
):
    original = {
        "External Id": '<span class="highlight" style="background-color:">SAMEA6806937</span>',
        "disease": "Huntington’s Disease",
        "protocol": "CUT&amp;amp;Tag",
        "nested": ["A &lt; B"],
    }
    assert sanitize_presentation_markup(original) == {
        "External Id": "SAMEA6806937",
        "disease": "Huntington’s Disease",
        "protocol": "CUT&Tag",
        "nested": ["A < B"],
    }

    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(original), encoding="utf-8")
    changed = sanitize_legacy_metadata([tmp_path])
    assert changed == [path]
    assert json.loads(path.read_text(encoding="utf-8"))["External Id"] == "SAMEA6806937"
    assert sanitize_legacy_metadata([path]) == []
