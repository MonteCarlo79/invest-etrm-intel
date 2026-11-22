"""_api_ask against the ask-modo v2 run API (no network).

Regression: Modo retired POST /ask-modo/api/v1/chats/completion (now 405,
Allow: GET).  The SPA starts a run with POST v2/chats/completion and reads the
answer from GET v2/chats/<chat_id>/runs/<run_id>/stream.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.gb_knowledge import modo_ai

V2 = "https://admin.modo.energy/ask-modo/api/v2"


class _Resp:
    def __init__(self, status=200, payload=None, lines=()):
        self.status_code = status
        self._payload = payload or {}
        self._lines = list(lines)

    def json(self):
        return self._payload

    def iter_lines(self, decode_unicode=True):
        return iter(self._lines)


class _Http:
    def __init__(self, start, stream=None):
        self._start, self._stream = start, stream
        self.calls = []

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw))
        return self._start

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw))
        return self._stream


SSE = [
    "event: set_chat_id", 'data: "c1"', "",
    "event: tool_call", 'data: {"tool_name": "search", "status": "start"}', "",
    "event: thinking_message", 'data: {"content": "pondering"}', "",
    "event: message_chunk", 'data: {"content": "GB BESS revenues "}', "",
    "event: message_chunk", 'data: {"content": "rose in November."}', "",
    'data: {"content": "untyped frame must not leak into the answer"}', "",
]


def test_ask_starts_v2_run_then_reads_stream():
    http = _Http(_Resp(200, {"chat_id": "c1", "run_id": "r1"}), _Resp(200, lines=SSE))
    answer = modo_ai._api_ask(http, "tok", "What happened?")
    assert answer == "GB BESS revenues rose in November."
    (m1, u1, kw1), (m2, u2, kw2) = http.calls
    assert (m1, u1) == ("POST", f"{V2}/chats/completion")
    assert kw1["json"] == {"message": "What happened?"}
    assert kw1["headers"]["Authorization"] == "Bearer tok"
    assert (m2, u2) == ("GET", f"{V2}/chats/c1/runs/r1/stream")
    assert kw2["headers"]["Authorization"] == "Bearer tok"


def test_ask_accepts_camelcase_run_ids():
    http = _Http(_Resp(200, {"chatId": "c9", "runId": "r9"}), _Resp(200, lines=SSE))
    modo_ai._api_ask(http, "tok", "q")
    assert http.calls[1][1] == f"{V2}/chats/c9/runs/r9/stream"


def test_ask_quota_exhausted_raises():
    with pytest.raises(modo_ai._QuotaExceeded):
        modo_ai._api_ask(_Http(_Resp(402)), "tok", "q")


def test_ask_http_error_raises_with_status():
    with pytest.raises(RuntimeError, match="405"):
        modo_ai._api_ask(_Http(_Resp(405)), "tok", "q")


def test_ask_missing_run_ids_raises():
    with pytest.raises(RuntimeError, match="run_id"):
        modo_ai._api_ask(_Http(_Resp(200, {"detail": "nope"})), "tok", "q")


def test_ask_stream_error_event_raises():
    lines = ["event: error", 'data: {"detail": "model unavailable"}', ""]
    http = _Http(_Resp(200, {"chat_id": "c", "run_id": "r"}), _Resp(200, lines=lines))
    with pytest.raises(RuntimeError, match="model unavailable"):
        modo_ai._api_ask(http, "tok", "q")
