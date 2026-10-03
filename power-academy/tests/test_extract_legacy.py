import stat

import docx
import pytest

from academy import extract as ex
from academy.models import SourceEntry

FAKE = """#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    --outdir) out="$2"; shift;;
    --convert-to) ext="$2"; shift;;
    -*) ;;
    *) f="$1";;
  esac
  shift
done
[ -n "$FAKE_FAIL" ] && exit 3
b=$(basename "$f"); cp "$FAKE_SRC" "$out/${b%.*}.$ext"
"""


def _entry(id_, path, type_, cls="library"):
    return SourceEntry(id=id_, path=str(path), title=id_, type=type_,
                       source_class=cls, cleared=cls == "practice",
                       license_risk="high" if cls == "library" else "low")


@pytest.fixture
def fake_soffice(tmp_path, monkeypatch):
    src = tmp_path / "converted_src.docx"
    d = docx.Document()
    d.add_paragraph("delta hedging of peakers " * 20)
    d.save(src)
    script = tmp_path / "fake_soffice"
    script.write_text(FAKE)
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("FAKE_SRC", str(src))
    monkeypatch.setattr(ex, "find_soffice", lambda: str(script))
    return script


def test_legacy_doc_converted_into_cache_not_beside_source(tmp_path, fake_soffice):
    old = tmp_path / "src" / "old.doc"
    old.parent.mkdir()
    old.write_bytes(b"legacy")
    cache = tmp_path / "cache"
    r = ex.extract_source(_entry("old", old, "doc"), cache, convert_dir=cache / "_converted")
    assert r["status"] == "ok"
    assert "delta hedging" in (cache / "old.txt").read_text(encoding="utf-8")
    assert list(old.parent.iterdir()) == [old]          # nothing written next to the source
    assert any((cache / "_converted").rglob("old.docx"))


def test_legacy_without_soffice_is_unsupported(tmp_path, monkeypatch):
    monkeypatch.setattr(ex, "find_soffice", lambda: None)
    old = tmp_path / "old.ppt"
    old.write_bytes(b"legacy")
    cache = tmp_path / "cache"
    r = ex.extract_source(_entry("old", old, "ppt"), cache, convert_dir=cache / "_converted")
    assert r["status"] == "unsupported"


def test_conversion_failure_is_failed_not_crash(tmp_path, fake_soffice, monkeypatch):
    monkeypatch.setenv("FAKE_FAIL", "1")
    old = tmp_path / "old.doc"
    old.write_bytes(b"legacy")
    cache = tmp_path / "cache"
    r = ex.extract_source(_entry("old", old, "doc"), cache, convert_dir=cache / "_converted")
    assert r["status"] == "failed" and "conversion" in r["error"].lower()


def test_folder_converts_legacy_files(tmp_path, fake_soffice):
    f = tmp_path / "deal"
    f.mkdir()
    (f / "old.doc").write_bytes(b"legacy")
    cache = tmp_path / "cache"
    r = ex.extract_source(_entry("deal", f, "folder", "practice"), cache,
                          convert_dir=cache / "_converted")
    assert r["status"] == "ok"
    assert "delta hedging" in (cache / "deal.txt").read_text(encoding="utf-8")
