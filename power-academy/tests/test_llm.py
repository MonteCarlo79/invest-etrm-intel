import pytest

from academy.llm import call_json, parse_json
from tests.fakes import FakeClient


def test_parse_json_handles_fences_and_prose():
    assert parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert parse_json('Here you go: {"a": {"b": 2}} done') == {"a": {"b": 2}}


def test_parse_json_rejects_no_object():
    with pytest.raises(ValueError):
        parse_json("no json here")


def test_call_json_retries_once_then_succeeds():
    c = FakeClient(["garbage", '{"ok": true}'])
    assert call_json(c, "m", "sys", "user") == {"ok": True}
    assert len(c.calls) == 2


def test_call_json_raises_after_retries():
    c = FakeClient(["garbage", "still garbage"])
    with pytest.raises(ValueError, match="unparseable"):
        call_json(c, "m", "sys", "user")
