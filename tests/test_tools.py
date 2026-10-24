from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from muse2api.app import create_app
from muse2api.core.tools import inject, parse_reply
from muse2api.drivers.mock import MockDriver

TOOLS = [{"type": "function", "function": {
    "name": "read_file", "description": "Read a file",
    "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}}]


class ScriptedDriver(MockDriver):
    def __init__(self, reply: str) -> None:
        super().__init__(delay=0)
        self.reply = reply
        self.prompts: list[str] = []

    async def chat_stream(self, account, req):
        self.prompts.append(req.prompt)
        yield self.reply


def _client(settings, reply):
    driver = ScriptedDriver(reply)
    return TestClient(create_app(settings, driver)), driver


CALL = '<tool_calls>[{"name": "read_file", "arguments": {"path": "a.py"}}]</tool_calls>'


def test_parse_tagged_call():
    parsed = parse_reply("Let me look.\n" + CALL, TOOLS)
    assert parsed.content == "Let me look."
    assert parsed.tool_calls[0]["function"]["name"] == "read_file"
    assert json.loads(parsed.tool_calls[0]["function"]["arguments"]) == {"path": "a.py"}


@pytest.mark.parametrize("text", [
    '```json\n[{"name": "read_file", "arguments": {"path": "a.py"}}]\n```',
    '{"tool_calls": [{"name": "read_file", "arguments": {"path": "a.py"}}]}',
    '<tool_calls>[{"name": "read_file", "arguments": {"path": "a.py"}}]',
])
def test_parse_lenient_formats(text):
    assert parse_reply(text, TOOLS).tool_calls[0]["function"]["name"] == "read_file"


def test_parse_ignores_unknown_tool_and_plain_text():
    bad = '<tool_calls>[{"name": "rm_rf", "arguments": {}}]</tool_calls>'
    assert parse_reply(bad, TOOLS).tool_calls == []
    assert parse_reply("Just an answer", TOOLS).content == "Just an answer"


def test_inject_renders_history():
    msgs = [
        {"role": "user", "content": "open a.py"},
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "read_file", "arguments": '{"path": "a.py"}'}}]},
        {"role": "tool", "tool_call_id": "c1", "content": "print(1)"},
    ]
    out = inject(msgs, TOOLS, None)
    assert out[0]["role"] == "system" and "read_file" in out[0]["content"]
    assert "<tool_calls>" in out[2]["content"]
    assert out[3]["content"].startswith("[Tool result: read_file]")


def test_chat_returns_tool_calls(settings, auth):
    with _client(settings, CALL)[0] as c:
        r = c.post("/v1/chat/completions", headers=auth, json={
            "model": "muse-chat", "tools": TOOLS,
            "messages": [{"role": "user", "content": "open a.py"}]})
    choice = r.json()["choices"][0]
    assert choice["finish_reason"] == "tool_calls"
    assert choice["message"]["content"] is None
    assert choice["message"]["tool_calls"][0]["function"]["name"] == "read_file"


def test_chat_stream_tool_calls(settings, auth):
    with _client(settings, CALL)[0] as c:
        r = c.post("/v1/chat/completions", headers=auth, json={
            "model": "muse-chat", "tools": TOOLS, "stream": True,
            "messages": [{"role": "user", "content": "open a.py"}]})
    events = [json.loads(line[6:]) for line in r.text.splitlines()
              if line.startswith("data: ") and line != "data: [DONE]"]
    assert events[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "read_file"
    assert events[-1]["choices"][0]["finish_reason"] == "tool_calls"
    assert r.text.rstrip().endswith("[DONE]")


def test_chat_with_tools_plain_answer_and_tool_choice_none(settings, auth):
    client, driver = _client(settings, "Hello there")
    with client as c:
        body = {"model": "muse-chat", "tools": TOOLS,
                "messages": [{"role": "user", "content": "hi"}]}
        r = c.post("/v1/chat/completions", headers=auth, json=body)
        assert r.json()["choices"][0]["message"]["content"] == "Hello there"
        assert r.json()["choices"][0]["finish_reason"] == "stop"
        c.post("/v1/chat/completions", headers=auth, json={**body, "tool_choice": "none"})
    assert "Available tools" in driver.prompts[0]
    assert "Available tools" not in driver.prompts[1]


def test_required_args_missing_is_malformed():
    tools = [{"type": "function", "function": {"name": "read_file", "parameters": {
        "type": "object", "properties": {"path": {}}, "required": ["path"]}}}]
    parsed = parse_reply('<tool_calls>[{"name": "read_file", "arguments": {}}]</tool_calls>', tools)
    assert parsed.malformed and not parsed.tool_calls


def test_malformed_reply_is_retried_once(settings, auth):
    class Flaky(ScriptedDriver):
        async def chat_stream(self, account, req):
            self.prompts.append(req.prompt)
            yield "<tool_calls>[oops" if len(self.prompts) == 1 else CALL

    driver = Flaky("")
    with TestClient(create_app(settings, driver)) as c:
        r = c.post("/v1/chat/completions", headers=auth, json={
            "model": "muse-chat", "tools": TOOLS,
            "messages": [{"role": "user", "content": "open a.py"}]})
    assert r.json()["choices"][0]["finish_reason"] == "tool_calls"
    assert len(driver.prompts) == 2 and "not valid" in driver.prompts[1]


def test_messages_text(client, auth):
    r = client.post("/v1/messages", headers=auth, json={
        "model": "claude-sonnet-4-5", "max_tokens": 50, "system": "Be brief.",
        "messages": [{"role": "user", "content": "hello"}]})
    body = r.json()
    assert body["type"] == "message" and body["stop_reason"] == "end_turn"
    assert body["content"][0]["type"] == "text" and "hello" in body["content"][0]["text"]


def test_messages_stream_text(client, auth):
    r = client.post("/v1/messages", headers=auth, json={
        "model": "claude-sonnet-4-5", "max_tokens": 50, "stream": True,
        "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]})
    names = [line[7:] for line in r.text.splitlines() if line.startswith("event: ")]
    assert names[0] == "message_start" and names[-1] == "message_stop"
    assert "content_block_delta" in names and "message_delta" in names


ANTHROPIC_TOOLS = [{"name": "read_file", "description": "Read a file",
                    "input_schema": {"type": "object", "properties": {"path": {"type": "string"}},
                                     "required": ["path"]}}]


def test_messages_tool_use(settings, auth):
    client, _ = _client(settings, CALL)
    with client as c:
        r = c.post("/v1/messages", headers=auth, json={
            "model": "claude-sonnet-4-5", "max_tokens": 50, "tools": ANTHROPIC_TOOLS,
            "messages": [{"role": "user", "content": "open a.py"}]})
    body = r.json()
    assert body["stop_reason"] == "tool_use"
    block = body["content"][-1]
    assert block["type"] == "tool_use" and block["name"] == "read_file"
    assert block["input"] == {"path": "a.py"} and block["id"].startswith("toolu_")


def test_messages_stream_tool_use_and_tool_result_history(settings, auth):
    client, driver = _client(settings, "It prints 1.")
    history = [
        {"role": "user", "content": "open a.py"},
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "toolu_1", "name": "read_file",
             "input": {"path": "a.py"}}]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "print(1)"}]},
    ]
    with client as c:
        r = c.post("/v1/messages", headers=auth, json={
            "model": "claude-sonnet-4-5", "max_tokens": 50, "tools": ANTHROPIC_TOOLS,
            "messages": history})
        assert r.json()["stop_reason"] == "end_turn"
    assert "[Tool result: read_file]" in driver.prompts[0] and "print(1)" in driver.prompts[0]

    client, _ = _client(settings, CALL)
    with client as c:
        r = c.post("/v1/messages", headers=auth, json={
            "model": "claude-sonnet-4-5", "max_tokens": 50, "tools": ANTHROPIC_TOOLS,
            "stream": True, "messages": [{"role": "user", "content": "open a.py"}]})
    assert "input_json_delta" in r.text and '"stop_reason": "tool_use"' in r.text


def test_messages_count_tokens(client, auth):
    r = client.post("/v1/messages/count_tokens", headers=auth, json={
        "model": "claude-sonnet-4-5", "messages": [{"role": "user", "content": "hello"}]})
    assert r.json()["input_tokens"] >= 1


RESP_TOOLS = [
    {"type": "function", "name": "read_file", "description": "Read a file",
     "parameters": {"type": "object", "properties": {"path": {"type": "string"}},
                    "required": ["path"]}},
    {"type": "custom", "name": "apply_patch", "description": "Apply a patch",
     "format": {"type": "grammar", "definition": "patch grammar"}},
]


def test_responses_function_call(settings, auth):
    client, _ = _client(settings, CALL)
    with client as c:
        r = c.post("/v1/responses", headers=auth, json={
            "model": "gpt-5", "input": "open a.py", "tools": RESP_TOOLS})
    item = r.json()["output"][-1]
    assert item["type"] == "function_call" and item["name"] == "read_file"
    assert json.loads(item["arguments"]) == {"path": "a.py"} and item["call_id"].startswith("call_")


def test_responses_custom_tool_call(settings, auth):
    reply = '<tool_calls>[{"name": "apply_patch", "arguments": {"input": "*** Begin"}}]</tool_calls>'
    client, _ = _client(settings, reply)
    with client as c:
        r = c.post("/v1/responses", headers=auth, json={
            "model": "gpt-5", "input": "patch", "tools": RESP_TOOLS})
    item = r.json()["output"][-1]
    assert item["type"] == "custom_tool_call" and item["input"] == "*** Begin"


def test_responses_stream_and_history(settings, auth):
    client, driver = _client(settings, CALL)
    with client as c:
        r = c.post("/v1/responses", headers=auth, json={
            "model": "gpt-5", "input": "open a.py", "tools": RESP_TOOLS, "stream": True})
        types = [json.loads(x[6:])["type"] for x in r.text.splitlines() if x.startswith("data: ")]
        assert "response.function_call_arguments.delta" in types
        assert types[0] == "response.created" and types[-1] == "response.completed"
    client, driver = _client(settings, "It prints 1.")
    with client as c:
        r = c.post("/v1/responses", headers=auth, json={"model": "gpt-5", "tools": RESP_TOOLS,
            "input": [
                {"role": "user", "content": "open a.py"},
                {"type": "reasoning", "summary": []},
                {"type": "function_call", "call_id": "call_1", "name": "read_file",
                 "arguments": '{"path": "a.py"}'},
                {"type": "function_call_output", "call_id": "call_1", "output": "print(1)"}]})
    assert r.json()["output"][0]["type"] == "message"
    assert "[Tool result: read_file]" in driver.prompts[0]


def test_announced_action_is_nudged(settings, auth):
    class Lazy(ScriptedDriver):
        async def chat_stream(self, account, req):
            self.prompts.append(req.prompt)
            yield "Tôi sẽ bắt đầu khám phá cấu trúc dự án." if len(self.prompts) == 1 else CALL

    driver = Lazy("")
    with TestClient(create_app(settings, driver)) as c:
        r = c.post("/v1/chat/completions", headers=auth, json={
            "model": "muse-chat", "tools": TOOLS,
            "messages": [{"role": "user", "content": "phân tích dự án này"}]})
    assert r.json()["choices"][0]["finish_reason"] == "tool_calls"
    assert "did not call a tool" in driver.prompts[1]


def test_final_answer_is_not_nudged(settings, auth):
    client, driver = _client(settings, "The file prints 1.")
    with client as c:
        r = c.post("/v1/chat/completions", headers=auth, json={
            "model": "muse-chat", "tools": TOOLS,
            "messages": [{"role": "user", "content": "what does it print?"}]})
    assert r.json()["choices"][0]["message"]["content"] == "The file prints 1."
    assert len(driver.prompts) == 1
