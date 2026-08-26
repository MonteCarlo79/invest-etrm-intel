"""Tests for services/exchange_reports/watcher.py"""
import datetime as dt
from unittest.mock import MagicMock, patch

from services.exchange_reports.watcher import (
    _list_tree,
    _within_days,
    scan_exchange_reports_onedrive,
)

_PATCH_INGEST = "services.exchange_reports.ingestor.ingest_report"
_PATCH_KNOWN = "services.exchange_reports.watcher._existing_filenames"


def _item(name, folder=False, days_ago=1, item_id=None):
    ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_ago)).isoformat()
    it = {"id": item_id or name, "name": name, "size": 1000, "lastModifiedDateTime": ts}
    if folder:
        it["folder"] = {}
    return it


def _fake_onedrive(structure):
    """structure: {path: [items]}"""
    od = MagicMock()
    od.list_items.side_effect = lambda path: structure.get(path, [])
    od.read_file.side_effect = lambda item_id: b"%PDF-fake"
    return od


class TestListTree:
    def test_recurses_into_province_dirs(self):
        od = _fake_onedrive({
            "root": [_item("新疆月报", folder=True), _item("全国月报", folder=True)],
            "root/新疆月报": [_item("a.pdf")],
            "root/全国月报": [_item("b.pdf"), _item("各省披露月报-2026-07", folder=True)],
            "root/全国月报/各省披露月报-2026-07": [_item("c.pdf")],
        })
        names = [it["name"] for it in _list_tree(od, "root")]
        assert names == ["a.pdf", "b.pdf", "c.pdf"]

    def test_list_failure_is_swallowed(self):
        od = _fake_onedrive({"root": [_item("x月报", folder=True)]})
        od.list_items.side_effect = lambda path: (_ for _ in ()).throw(RuntimeError("net"))
        assert _list_tree(od, "root") == []


class TestWithinDays:
    def test_recent_included(self):
        assert _within_days(_item("a.pdf", days_ago=5), 45) is True

    def test_old_excluded(self):
        assert _within_days(_item("a.pdf", days_ago=90), 45) is False

    def test_no_limit(self):
        assert _within_days(_item("a.pdf", days_ago=999), None) is True

    def test_bad_date_treated_as_new(self):
        it = _item("a.pdf")
        it["lastModifiedDateTime"] = "garbage"
        assert _within_days(it, 45) is True


class TestScan:
    def _setup(self, files_by_path, known=()):
        od = _fake_onedrive(files_by_path)
        return od

    def test_skips_known_and_old_files(self):
        od = self._setup({
            "root": [
                _item("known.pdf", days_ago=1),
                _item("old.pdf", days_ago=200),
                _item("new.pdf", days_ago=2),
            ],
        })
        with patch(_PATCH_KNOWN, return_value={"known.pdf"}), \
             patch(_PATCH_INGEST, return_value={"status": "ingested", "province": "新疆", "report_month": "2026-06-01"}) as ing:
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["scanned"] == 3
        assert s["candidates"] == 1
        assert s["ingested"] == 1
        ing.assert_called_once()

    def test_new_file_ingested_and_notified(self):
        od = self._setup({"root": [_item("new.pdf", days_ago=1)]})
        feishu = MagicMock()
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, return_value={"status": "ingested", "province": "蒙西", "report_month": "2026-06-01"}):
            s = scan_exchange_reports_onedrive(od, "pg", "key", feishu=feishu, owner_open_id="ou_x", root="root")
        assert s["ingested"] == 1
        feishu.send_text.assert_called_once()
        assert "蒙西" in feishu.send_text.call_args.kwargs["text"]

    def test_no_notification_without_ingested(self):
        od = self._setup({"root": [_item("dup.pdf", days_ago=1)]})
        feishu = MagicMock()
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, return_value={"status": "duplicate"}):
            s = scan_exchange_reports_onedrive(od, "pg", "key", feishu=feishu, owner_open_id="ou_x", root="root")
        assert s["duplicate"] == 1
        feishu.send_text.assert_not_called()

    def test_failure_does_not_stop_batch(self):
        od = self._setup({"root": [_item("bad.pdf", days_ago=1), _item("good.pdf", days_ago=1)]})
        calls = {"n": 0}
        def flaky(**kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("parse boom")
            return {"status": "ingested", "province": "山东", "report_month": "2026-06-01"}
        with patch(_PATCH_KNOWN, return_value=set()), patch(_PATCH_INGEST, side_effect=flaky):
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["failed"] == 1 and s["ingested"] == 1

    def test_no_onedrive_returns_error(self):
        s = scan_exchange_reports_onedrive(None, "pg", "key")
        assert s["error"] == "onedrive_not_configured"

    def test_dry_run_downloads_nothing(self):
        od = self._setup({"root": [_item("new.pdf", days_ago=1)]})
        with patch(_PATCH_KNOWN, return_value=set()):
            s = scan_exchange_reports_onedrive(od, "pg", "key", dry_run=True, root="root")
        od.read_file.assert_not_called()
        assert s["results"][0]["status"] == "dry_run"


class TestNonMonthlyReroute:
    def test_annual_report_rerouted_to_kb(self):
        od = _fake_onedrive({"root": [_item("广东电力现货市场2025年年报.pdf", days_ago=1)]})
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, side_effect=ValueError("Cannot infer report_month from filename: 'x'")), \
             patch("services.knowledge_pool.knowledge_docs.register_and_ingest",
                   return_value=(9001, True, "annual_report")) as kb:
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["kb_ingested"] == 1 and s["failed"] == 0
        assert s["results"][0]["kb_doc_id"] == 9001

    def test_monthly_failure_without_month_also_rerouted(self):
        od = _fake_onedrive({"root": [_item("2025年江苏电力市场运营情况通报.pdf", days_ago=1)]})
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, side_effect=ValueError("Cannot infer report_month from filename: 'x'")), \
             patch("services.knowledge_pool.knowledge_docs.register_and_ingest",
                   return_value=(9002, True, "monthly_report")):
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["kb_ingested"] == 1 and s["failed"] == 0

    def test_unrelated_failure_stays_failed(self):
        od = _fake_onedrive({"root": [_item("新疆2026年6月月报.pdf", days_ago=1)]})
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, side_effect=RuntimeError("connection reset")):
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["kb_ingested"] == 0 and s["failed"] == 1

    def test_kb_reroute_failure_stays_failed(self):
        od = _fake_onedrive({"root": [_item("2025年年报.pdf", days_ago=1)]})
        with patch(_PATCH_KNOWN, return_value=set()), \
             patch(_PATCH_INGEST, side_effect=ValueError("Cannot infer report_month from filename: 'x'")), \
             patch("services.knowledge_pool.knowledge_docs.register_and_ingest",
                   side_effect=RuntimeError("db down")):
            s = scan_exchange_reports_onedrive(od, "pg", "key", root="root")
        assert s["kb_ingested"] == 0 and s["failed"] == 1
