from __future__ import annotations

import io
import re
import tarfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

from ..catalog import RunCatalog
from ..errors import MetadataError
from ..metadata import EntrezClient, SraClient
from ..metadata.core import _tag, _text


@dataclass(frozen=True)
class GeoSupplementaryFile:
    """Describe one GEO supplementary file.

    Args:
        geo_accession: Parent GSE or GSM accession.
        url: Download URL declared by GEO.
        filename: File name derived from the URL.
    """

    geo_accession: str
    url: str
    filename: str


class GeoClient:
    """Use configured Entrez and SRA clients to resolve GEO sequencing data."""

    def __init__(self, entrez: EntrezClient, sra: SraClient) -> None:
        """Store *entrez* for GEO links and *sra* for linked RunInfo retrieval."""

        self.entrez = entrez
        self.sra = sra

    def resolve_to_sra(self, accessions: list[str]) -> RunCatalog:
        """Resolve GSE or GSM *accessions* to a deduplicated SRA run catalog."""

        sra_ids: list[str] = []
        for accession in self.entrez.progress.track(
            accessions, "Resolve GEO accessions", unit="accessions"
        ):
            gds_ids = self.entrez.search_ids("gds", f"{accession}[ACCN]", limit=100)
            if not gds_ids:
                raise MetadataError(f"GEO accession was not found: {accession}")
            payload = self.entrez.get(
                "elink.fcgi",
                {"dbfrom": "gds", "db": "sra", "id": gds_ids, "retmode": "xml"},
            )
            root = ET.fromstring(payload)
            for link_set_db in (node for node in root.iter() if _tag(node) == "LinkSetDb"):
                db_to = next((node for node in link_set_db if _tag(node) == "DbTo"), None)
                if _text(db_to) != "sra":
                    continue
                for link in (node for node in link_set_db.iter() if _tag(node) == "Link"):
                    identifier = next((node for node in link if _tag(node) == "Id"), None)
                    if _text(identifier):
                        sra_ids.append(_text(identifier) or "")
        unique_ids = list(dict.fromkeys(sra_ids))
        if not unique_ids:
            raise MetadataError(f"No SRA records are linked to GEO accessions: {accessions!r}")
        return self.sra.fetch_runinfo_ids(unique_ids)

    @staticmethod
    def _series_stem(accession: str) -> str:
        """Return the GEO FTP bucket stem for GSE *accession*."""

        match = re.fullmatch(r"GSE(\d+)", accession.upper())
        if not match:
            raise ValueError("GEO MINiML discovery requires a GSE accession")
        digits = match.group(1)
        return f"GSE{digits[:-3]}nnn" if len(digits) > 3 else "GSEnnn"

    def discover_supplementary(self, series_accession: str) -> list[GeoSupplementaryFile]:
        """Return files declared in the MINiML archive for *series_accession*."""

        accession = series_accession.upper()
        stem = self._series_stem(accession)
        url = (
            f"https://ftp.ncbi.nlm.nih.gov/geo/series/{stem}/{accession}/"
            f"miniml/{accession}_family.xml.tgz"
        )
        payload = self.entrez.http.request(url).body
        self.entrez.progress.message(f"Downloaded GEO MINiML archive for {accession}")
        try:
            with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
                xml_members = [
                    member for member in archive.getmembers() if member.name.endswith(".xml")
                ]
                if not xml_members:
                    raise MetadataError(f"GEO MINiML archive contains no XML: {url}")
                handle = archive.extractfile(xml_members[0])
                if handle is None:
                    raise MetadataError(f"Cannot read GEO MINiML XML: {url}")
                root = ET.fromstring(handle.read())
        except (tarfile.TarError, ET.ParseError) as exc:
            raise MetadataError(f"Malformed GEO MINiML archive: {url}") from exc

        files: list[GeoSupplementaryFile] = []
        for container in root.iter():
            if _tag(container) not in {"Series", "Sample"}:
                continue
            parent_accession = container.attrib.get("iid") or accession
            for node in container:
                if _tag(node) != "Supplementary-Data":
                    continue
                file_url = _text(node)
                if not file_url:
                    continue
                files.append(
                    GeoSupplementaryFile(
                        geo_accession=parent_accession,
                        url=file_url,
                        filename=Path(file_url).name,
                    )
                )
        return list({item.url: item for item in files}.values())
