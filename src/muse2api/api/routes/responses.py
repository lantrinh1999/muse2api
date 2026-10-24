"""OpenAI Responses API (``/v1/responses``).

Maps ``input`` / ``instructions`` onto ``flatten_messages`` and reuses
``Gateway.chat_stream``. Streaming emits the standard Responses events
(``response.created`` / ``response.output_text.delta`` / ``response.completed``).

muse.ai has no function calling, so tool items (``function_call``,
``function_call_output``) are rejected with a clear error instead of being
silently dropped (see C3).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict

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
router = APIRouter(tags=["responses"], dependencies=[Depends(require_api_key)])


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _sse(payload: dict | str) -> bytes:
    data = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return f"data: {data}\n\n".encode()


class ResponseRequest(BaseModel):
    model: str | None = None
    input: Any = None
    instructions: str | None = None
    stream: bool = False
    user: str | None = None
    max_output_tokens: int | None = None

    model_config = ConfigDict(extra="allow")


# Item types the official SDK round-trips but muse.ai cannot fulfil.
_TOOL_ITEMS = {"computer_call", "file_search_call", "web_search_call"}
_CUSTOM_SCHEMA = {"type": "object", "required": ["input"], "properties": {
    "input": {"type": "string", "description": "Raw freeform input for this tool"}}}


def _output_text(output: Any) -> str:
    if isinstance(output, list):
        return "\n".join(p.get("text", "") for p in output if isinstance(p, dict))
    return output if isinstance(output, str) else json.dumps(output, ensure_ascii=False)


def _to_chat_tools(tools: list[dict] | None) -> tuple[list[dict], set[str]]:
    """Responses tools -> chat-style function tools, plus the names of freeform tools."""
    out: list[dict] = []
    custom: set[str] = set()
    for t in tools or []:
        kind, name = t.get("type"), t.get("name")
        if not name:
            continue
        if kind == "function":
            out.append({"type": "function", "function": {
                "name": name, "description": t.get("description", ""),
                "parameters": t.get("parameters") or {"type": "object", "properties": {}}}})
        elif kind == "custom":
            custom.add(name)
            fmt = (t.get("format") or {}).get("definition", "")
            out.append({"type": "function", "function": {
                "name": name, "parameters": _CUSTOM_SCHEMA,
                "description": (t.get("description", "") + "\n" + fmt).strip()}})
    return out, custom


def _to_chat_choice(choice: Any) -> Any:
    if isinstance(choice, dict) and choice.get("name"):
        return {"type": "function", "function": {"name": choice["name"]}}
    return choice if choice in ("none", "required") else "auto"


def _to_messages(body: ResponseRequest) -> list[dict]:
    """Convert Responses ``input`` / ``instructions`` to chat-style messages."""
    messages: list[dict] = []
    if body.instructions:
        messages.append({"role": "system", "content": body.instructions})
    inp = body.input
    if inp is None or (isinstance(inp, str) and not inp.strip()):
        pass
    elif isinstance(inp, str):
        messages.append({"role": "user", "content": inp})
    elif isinstance(inp, list):
        for item in inp:
            if not isinstance(item, dict):
                raise InvalidRequest("input items must be objects")
            itype = item.get("type")
            if itype in _TOOL_ITEMS:
                raise InvalidRequest(
                    f"input item type '{itype}' is not supported by muse.ai")
            if itype == "reasoning":
                continue
            if itype in ("function_call", "custom_tool_call"):
                args = item.get("arguments") if itype == "function_call" else json.dumps(
                    {"input": item.get("input", "")}, ensure_ascii=False)
                messages.append({"role": "assistant", "content": None, "tool_calls": [{
                    "id": item.get("call_id", ""), "type": "function",
                    "function": {"name": item.get("name", ""), "arguments": args or "{}"}}]})
            elif itype in ("function_call_output", "custom_tool_call_output"):
                messages.append({"role": "tool", "tool_call_id": item.get("call_id", ""),
                                 "content": _output_text(item.get("output"))})
            elif "role" in item or itype in ("message", None):
                messages.append({"role": item.get("role", "user"),
                                 "content": item.get("content")})
            else:
                raise InvalidRequest(f"unsupported input item type '{itype}'")
    else:
        raise InvalidRequest("input must be a string or a list of items")
    return messages


def _message_item(msg_id: str, text: str) -> dict:
    return {"id": msg_id, "type": "message", "status": "completed", "role": "assistant",
            "content": [{"type": "output_text", "annotations": [], "text": text}]}


def _usage(input_text: str, output_text: str) -> dict:
    i, o = _approx_tokens(input_text), _approx_tokens(output_text)
    return {"input_tokens": i, "output_tokens": o, "total_tokens": i + o,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0}}


def _response_object(resp_id: str, msg_id: str, created: int, model: str, text: str,
                     input_text: str, *, status: str = "completed") -> dict:
    return {"id": resp_id, "object": "response", "created_at": created, "model": model,
            "status": status,
            "output": [_message_item(msg_id, text)] if status == "completed" else [],
            "usage": _usage(input_text, text), "parallel_tool_calls": True,
            "tool_choice": "auto", "tools": []}


def _tool_output_response(body: ResponseRequest, resp_id: str, created: int, model: str,
                          output: list[dict], input_text: str):
    out_text = json.dumps(output, ensure_ascii=False)

    def snapshot(status: str) -> dict:
        return {"id": resp_id, "object": "response", "created_at": created, "model": model,
                "status": status, "output": output if status == "completed" else [],
                "usage": _usage(input_text, out_text), "parallel_tool_calls": True,
                "tool_choice": "auto", "tools": []}

    if not body.stream:
        return snapshot("completed")

    async def events() -> AsyncIterator[bytes]:
        seq = 0

        def ev(payload: dict) -> bytes:
            nonlocal seq
            payload["sequence_number"] = seq
            seq += 1
            return _sse(payload)

        yield ev({"type": "response.created", "response": snapshot("in_progress")})
        yield ev({"type": "response.in_progress", "response": snapshot("in_progress")})
        for i, item in enumerate(output):
            kind, iid = item["type"], item["id"]
            if kind == "message":
                text = item["content"][0]["text"]
                yield ev({"type": "response.output_item.added", "output_index": i,
                          "item": {**item, "status": "in_progress", "content": []}})
                part = {"type": "output_text", "text": "", "annotations": []}
                yield ev({"type": "response.content_part.added", "item_id": iid,
                          "output_index": i, "content_index": 0, "part": part})
                yield ev({"type": "response.output_text.delta", "item_id": iid,
                          "output_index": i, "content_index": 0, "delta": text, "logprobs": []})
                yield ev({"type": "response.output_text.done", "item_id": iid,
                          "output_index": i, "content_index": 0, "text": text, "logprobs": []})
                yield ev({"type": "response.content_part.done", "item_id": iid,
                          "output_index": i, "content_index": 0,
                          "part": item["content"][0]})
            else:
                custom = kind == "custom_tool_call"
                field = "input" if custom else "arguments"
                yield ev({"type": "response.output_item.added", "output_index": i,
                          "item": {**item, "status": "in_progress", field: ""}})
                prefix = "response.custom_tool_call_input" if custom else (
                    "response.function_call_arguments")
                yield ev({"type": f"{prefix}.delta", "item_id": iid, "output_index": i,
                          "delta": item[field]})
                done = {"type": f"{prefix}.done", "item_id": iid, "output_index": i,
                        field: item[field]}
                if not custom:
                    done["name"] = item["name"]
                yield ev(done)
            yield ev({"type": "response.output_item.done", "output_index": i, "item": item})
        yield ev({"type": "response.completed", "response": snapshot("completed")})

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/v1/responses")
async def create_response(body: ResponseRequest, request: Request,
                          svc: Services = Depends(get_services)):
    request.state.model = body.model  # logged even if it does not resolve
    spec = resolve_model(body.model, "chat")
    request.state.model = body.model or spec.id
    messages = _to_messages(body)
    chat_tools, custom_names = _to_chat_tools(body.model_extra.get("tools"))
    choice = _to_chat_choice(body.model_extra.get("tool_choice"))
    tools = active_tools(chat_tools, choice)
    if tools:
        messages = inject(messages, tools, choice)
    flat = flatten_messages(messages, FINAL_INSTRUCTION if tools else None)
    if not flat.text and not flat.images:
        raise InvalidRequest("input contains no text or images")

    images = [InputImage(*(await load_image_ref(ref))) for ref in flat.images]
    cancel = asyncio.Event()
    req = ChatRequest(
        prompt=flat.text,
        model=spec.id,
        images=images,
        timeout=svc.settings.chat_timeout,
        first_token_timeout=svc.settings.first_token_timeout,
        conversation_hint=body.user,
        turns=message_turns(messages),
        cancel=cancel,
    )
    resp_id = "resp_" + uuid.uuid4().hex[:24]
    msg_id = "msg_" + uuid.uuid4().hex[:24]
    created = int(time.time())
    model_name = body.model or spec.id

    if tools:
        try:
            parsed = await collect_tool_reply(svc, req, tools)
        finally:
            cancel.set()
        output: list[dict] = []
        if parsed.content or not parsed.tool_calls:
            output.append(_message_item(msg_id, parsed.content))
        for c in parsed.tool_calls:
            fn, cid = c["function"], c["id"]
            if fn["name"] in custom_names:
                try:
                    raw = json.loads(fn["arguments"]).get("input", "")
                except (ValueError, AttributeError):
                    raw = fn["arguments"]
                output.append({"id": "ctc_" + cid[5:], "type": "custom_tool_call",
                               "status": "completed", "call_id": cid, "name": fn["name"],
                               "input": raw})
            else:
                output.append({"id": "fc_" + cid[5:], "type": "function_call",
                               "status": "completed", "call_id": cid, "name": fn["name"],
                               "arguments": fn["arguments"]})
        return _tool_output_response(body, resp_id, created, model_name, output, flat.text)

    stream = svc.gateway.chat_stream(req)

    if not body.stream:
        text = "".join([d async for d in stream])
        return _response_object(resp_id, msg_id, created, model_name, text, flat.text)

    async def events(first: str | None) -> AsyncIterator[bytes]:
        # ``first`` is the pre-fetched first delta when heartbeats are disabled;
        # when enabled it is None and the first token is awaited here, after the
        # 200 (headers) has already been committed to the client.
        seq = 0
        try:
            if first is None and svc.settings.stream_heartbeat_interval > 0:
                # Same C4 pattern as chat completions: send SSE comment
                # heartbeats while the gateway waits for the first token.
                pending = asyncio.ensure_future(anext(stream))
                try:
                    while True:
                        done, _ = await asyncio.wait(
                            {pending},
                            timeout=svc.settings.stream_heartbeat_interval)
                        if done:
                            try:
                                first = pending.result()
                            except StopAsyncIteration:
                                first = ""
                            break
                        yield b": keepalive\n\n"
                finally:
                    if not pending.done():
                        pending.cancel()
            yield _sse({"type": "response.created", "sequence_number": seq,
                        "response": _response_object(resp_id, msg_id, created, model_name,
                                                     "", flat.text, status="in_progress")})
            seq += 1
            # The official SDK builds its accumulated snapshot from item/part
            # lifecycle events, so emit them around the text deltas.
            yield _sse({"type": "response.in_progress", "sequence_number": seq,
                        "response": _response_object(resp_id, msg_id, created, model_name,
                                                     "", flat.text, status="in_progress")})
            seq += 1
            yield _sse({"type": "response.output_item.added", "sequence_number": seq,
                        "output_index": 0,
                        "item": {"id": msg_id, "type": "message", "status": "in_progress",
                                 "role": "assistant", "content": []}})
            seq += 1
            yield _sse({"type": "response.content_part.added", "sequence_number": seq,
                        "item_id": msg_id, "output_index": 0, "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []}})
            seq += 1
            produced = first or ""

            def delta_event(delta: str) -> dict:
                return {"type": "response.output_text.delta", "sequence_number": seq,
                        "item_id": msg_id, "output_index": 0, "content_index": 0,
                        "delta": delta, "logprobs": []}

            if produced:
                yield _sse(delta_event(produced))
                seq += 1
            async for delta in stream:
                produced += delta
                yield _sse(delta_event(delta))
                seq += 1
            yield _sse({"type": "response.output_item.done", "sequence_number": seq,
                        "output_index": 0, "item": _message_item(msg_id, produced)})
            seq += 1
            yield _sse({"type": "response.completed", "sequence_number": seq,
                        "response": _response_object(resp_id, msg_id, created, model_name,
                                                     produced, flat.text)})
        except Muse2APIError as exc:
            yield _sse(exc.to_body())
        except Exception:  # noqa: BLE001
            log.exception("responses stream crashed")
            yield _sse({"error": {"message": "internal error", "type": "server_error"}})
        finally:
            cancel.set()
            await stream.aclose()

    # With stream_heartbeat_interval set (C4), the 200 goes out immediately;
    # upstream errors then surface as SSE error events instead of HTTP codes.
    if svc.settings.stream_heartbeat_interval > 0:
        return StreamingResponse(events(None), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache",
                                          "X-Accel-Buffering": "no"})

    # Default: pull the first delta before committing to a 200 so that
    # account/upstream failures surface as proper HTTP errors instead of a
    # broken event stream.
    try:
        first = await anext(stream)
    except StopAsyncIteration:
        first = ""

    return StreamingResponse(events(first), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})
