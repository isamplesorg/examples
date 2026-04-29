"""Helpers for joining iSamples vocabulary URIs to human-readable labels.

The wide and narrow parquets currently store SKOS concept references as URIs
in `IdentifiedConcept.label` (e.g. `https://w3id.org/isample/vocabulary/
material/1.0/earthmaterial`). This module wraps the canonical
`vocab_labels.parquet` lookup published at `data.isamples.org` so notebooks
can render `Natural Solid Material` instead of the raw URI.

See https://github.com/isamplesorg/isamplesorg.github.io/issues/148 for
background and the build script that produces the artifact.
"""

from __future__ import annotations

import duckdb

VOCAB_LABELS_URL = "https://data.isamples.org/vocab_labels.parquet"


def load_vocab_labels(url: str = VOCAB_LABELS_URL, lang: str = "en") -> dict[str, str]:
    """Return a {uri: pref_label} dict for the requested language.

    The artifact is ~60KB; one HTTP fetch is fine for any notebook session.
    """
    con = duckdb.connect()
    rows = con.sql(
        f"SELECT uri, pref_label FROM read_parquet('{url}') WHERE lang = ?",
        params=[lang],
    ).fetchall()
    return {uri: label for uri, label in rows}


def pretty_label(uri: str | None, labels: dict[str, str]) -> str:
    """Return the SKOS prefLabel for `uri`, falling back to the URI tail."""
    if uri is None:
        return ""
    if uri in labels:
        return labels[uri]
    s = str(uri)
    if s.startswith(("http://", "https://")):
        tail = s.rstrip("/").rsplit("/", 1)[-1]
        return tail or s
    return s
