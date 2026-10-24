"""Anthropic Messages API (``/v1/messages``) for Claude-style clients.

Messages, system prompt and tools are converted to the OpenAI shapes used by the chat
route, so text, images and (emulated) tool use share one code path. Replies are
converted back to ``text`` / ``tool_use`` content blocks, with the standard streaming
events (``message_start`` ... ``message_stop``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from ...core.media import load_image_ref
from ...core.models import resolve_model
from ...core.prompt import flatten_messages, message_turns
from ...core.tools import FINAL_INSTRUCTION, active_tools, inject
from ...drivers.base import ChatRequest, InputImage
from ...errors import InvalidRequest, Muse2APIError
from ...services.container import Services
from ..deps import get_services, require_api_key
from .chat import collect_tool_reply

log = logging.getLogger(__name__)
router = APIRouter(tags=["anthropic"], dependencies=[Depends(require_api_key)])


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _event(name: str, payload: dict) -> bytes:
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


class MessagesRequest(BaseModel):
    model: str | None = None
    messages: list[dict[str, Any]] = Field(min_length=1)
    system: str | list[dict[str, Any]] | None = None
    max_tokens: int | None = None
    stream: bool = False
    tools: list[dict[str, Any]] | None = None
    tool_choice: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    model_config = ConfigDict(extra="allow")


def _blocks(content: Any) -> list[dict[str, Any]]:
    if content is None:
        return []
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return [b for b in content if isinstance(b, dict)]


def _image_url(block: dict[str, Any]) -> str | None:
    src = block.get("source") or {}
    if src.get("type") == "base64" and src.get("data"):
        return f"data:{src.get('media_type', 'image/png')};base64,{src['data']}"
    return src.get("url")


def _result_text(block: dict[str, Any]) -> str:
    text = "\n".join(b.get("text", "") for b in _blocks(block.get("content"))
                     if b.get("type") == "text")
    return f"Error: {text}" if block.get("is_error") else text


def to_openai_messages(system: Any, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    sys_text = system if isinstance(system, str) else "\n".join(
        b.get("text", "") for b in _blocks(system) if b.get("type") == "text")
    if sys_text and sys_text.strip():
        out.append({"role": "system", "content": sys_text})
    for msg in messages:
        role = msg.get("role", "user")
        blocks = _blocks(msg.get("content"))
        if role == "assistant":
            text = "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text")
            calls = [{"id": b.get("id", ""), "type": "function",
                      "function": {"name": b.get("name", ""),
                                   "arguments": json.dumps(b.get("input") or {},
                                                           ensure_ascii=False)}}
                     for b in blocks if b.get("type") == "tool_use"]
            item: dict[str, Any] = {"role": "assistant", "content": text or None}
            if calls:
                item["tool_calls"] = calls
            out.append(item)
            continue
        parts: list[dict[str, Any]] = []
        for b in blocks:
            kind = b.get("type")
            if kind == "tool_result":
                out.append({"role": "tool", "tool_call_id": b.get("tool_use_id", ""),
                            "content": _result_text(b)})
            elif kind == "text":
                parts.append({"type": "text", "text": b.get("text", "")})
            elif kind == "image" and (url := _image_url(b)):
                parts.append({"type": "image_url", "image_url": {"url": url}})
        if parts:
            out.append({"role": "user", "content": parts})
    return out


def to_openai_tools(tools: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    # Server-side tools (web_search, text_editor, ...) have no input_schema; skip them.
    return [{"type": "function", "function": {
        "name": t["name"], "description": t.get("description", ""),
        "parameters": t["input_schema"]}}
        for t in tools or [] if t.get("name") and isinstance(t.get("input_schema"), dict)]


def to_openai_choice(choice: dict[str, Any] | None) -> Any:
    kind = (choice or {}).get("type")
    if kind == "any":
        return "required"
    if kind == "tool" and choice.get("name"):
        return {"type": "function", "function": {"name": choice["name"]}}
    if kind == "none":
        return "none"
    return "auto"


@router.post("/v1/messages/count_tokens")
async def count_tokens(body: MessagesRequest):
    raw = json.dumps([body.system, body.messages, body.tools], ensure_ascii=False, default=str)
    return {"input_tokens": _approx_tokens(raw)}


@router.post("/v1/messages")
async def messages(body: MessagesRequest, request: Request,
                   svc: Services = Depends(get_services)):
    request.state.model = body.model
    spec = resolve_model(body.model, "chat")
    request.state.model = body.model or spec.id

    msgs = to_openai_messages(body.system, body.messages)
    tools = active_tools(to_openai_tools(body.tools), to_openai_choice(body.tool_choice))
    if tools:
        msgs = inject(msgs, tools, to_openai_choice(body.tool_choice))
    flat = flatten_messages(msgs, FINAL_INSTRUCTION if tools else None)
    if not flat.text and not flat.images:
        raise InvalidRequest("messages contain no text or images")

    images = [InputImage(*(await load_image_ref(ref))) for ref in flat.images]
    cancel = asyncio.Event()
    req = ChatRequest(
        prompt=flat.text, model=spec.id, images=images,
        timeout=svc.settings.chat_timeout,
        first_token_timeout=svc.settings.first_token_timeout,
        conversation_hint=(body.metadata or {}).get("user_id"),
        turns=message_turns(msgs), cancel=cancel,
    )
    msg_id = "msg_" + uuid.uuid4().hex[:24]
    model_name = body.model or spec.id
    in_tokens = _approx_tokens(flat.text)

    def envelope(content: list[dict], stop: str, out_tokens: int) -> dict:
        return {"id": msg_id, "type": "message", "role": "assistant", "model": model_name,
                "content": content, "stop_reason": stop, "stop_sequence": None,
                "usage": {"input_tokens": in_tokens, "output_tokens": out_tokens}}

    def start_event() -> bytes:
        msg = envelope([], "", 1)
        msg["stop_reason"] = None
        return _event("message_start", {"type": "message_start", "message": msg})

    def finish_events(stop: str, out_tokens: int) -> list[bytes]:
        return [_event("message_delta", {
                    "type": "message_delta",
                    "delta": {"stop_reason": stop, "stop_sequence": None},
                    "usage": {"output_tokens": out_tokens}}),
                _event("message_stop", {"type": "message_stop"})]

    def text_block(index: int, text: str) -> list[bytes]:
        return [_event("content_block_start", {"type": "content_block_start", "index": index,
                                               "content_block": {"type": "text", "text": ""}}),
                _event("content_block_delta", {"type": "content_block_delta", "index": index,
                                               "delta": {"type": "text_delta", "text": text}}),
                _event("content_block_stop", {"type": "content_block_stop", "index": index})]

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

    if tools:
        try:
            parsed = await collect_tool_reply(svc, req, tools)
        finally:
            cancel.set()
        blocks: list[dict] = []
        if parsed.content:
            blocks.append({"type": "text", "text": parsed.content})
        for c in parsed.tool_calls:
            try:
                args = json.loads(c["function"]["arguments"])
            except ValueError:
                args = {}
            blocks.append({"type": "tool_use", "id": "toolu_" + c["id"].removeprefix("call_"),
                           "name": c["function"]["name"], "input": args})
        if not blocks:
            blocks.append({"type": "text", "text": ""})
        stop = "tool_use" if parsed.tool_calls else "end_turn"
        out_tokens = _approx_tokens(parsed.content + json.dumps(parsed.tool_calls))
        if not body.stream:
            return envelope(blocks, stop, out_tokens)

        async def tool_events() -> AsyncIterator[bytes]:
            yield start_event()
            for i, b in enumerate(blocks):
                if b["type"] == "text":
                    for ev in text_block(i, b["text"]):
                        yield ev
                    continue
                yield _event("content_block_start", {
                    "type": "content_block_start", "index": i,
                    "content_block": {**b, "input": {}}})
                yield _event("content_block_delta", {
                    "type": "content_block_delta", "index": i,
                    "delta": {"type": "input_json_delta",
                              "partial_json": json.dumps(b["input"], ensure_ascii=False)}})
                yield _event("content_block_stop", {"type": "content_block_stop", "index": i})
            for ev in finish_events(stop, out_tokens):
                yield ev

        return StreamingResponse(tool_events(), media_type="text/event-stream", headers=headers)

    stream = svc.gateway.chat_stream(req)
    if not body.stream:
        try:
            text = "".join([d async for d in stream])
        finally:
            cancel.set()
        return envelope([{"type": "text", "text": text}], "end_turn", _approx_tokens(text))

    try:
        first = await anext(stream)
    except StopAsyncIteration:
        first = ""

    async def events() -> AsyncIterator[bytes]:
        produced = first
        try:
            yield start_event()
            yield _event("content_block_start", {"type": "content_block_start", "index": 0,
                                                 "content_block": {"type": "text", "text": ""}})
            yield _event("ping", {"type": "ping"})
            if first:
                yield _event("content_block_delta", {
                    "type": "content_block_delta", "index": 0,
                    "delta": {"type": "text_delta", "text": first}})
            async for delta in stream:
                produced += delta
                yield _event("content_block_delta", {
                    "type": "content_block_delta", "index": 0,
                    "delta": {"type": "text_delta", "text": delta}})
            yield _event("content_block_stop", {"type": "content_block_stop", "index": 0})
            for ev in finish_events("end_turn", _approx_tokens(produced)):
                yield ev
        except Muse2APIError as exc:
            yield _event("error", {"type": "error", "error": {
                "type": exc.error_type, "message": exc.message}})
        except Exception:  # noqa: BLE001
            log.exception("anthropic stream crashed")
            yield _event("error", {"type": "error", "error": {
                "type": "api_error", "message": "internal error"}})
        finally:
            cancel.set()
            await stream.aclose()

    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)
