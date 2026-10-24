from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from ...core.media import load_image_ref
from ...core.models import resolve_model
from ...core.prompt import flatten_messages, message_turns
from ...core.tools import (
    FINAL_INSTRUCTION,
    NUDGE_INSTRUCTION,
    REPAIR_INSTRUCTION,
    ParsedReply,
    active_tools,
    inject,
    looks_unfulfilled,
    parse_reply,
)
from ...drivers.base import ChatRequest, InputImage
from ...errors import InvalidRequest, Muse2APIError
from ...services.container import Services
from ..deps import get_services, require_api_key
from ..schemas import ChatCompletionRequest

log = logging.getLogger(__name__)
router = APIRouter(tags=["chat"], dependencies=[Depends(require_api_key)])


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _sse(payload: dict | str) -> bytes:
    data = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return f"data: {data}\n\n".encode()


async def collect_tool_reply(svc: Services, req: ChatRequest,
                             tools: list[dict]) -> ParsedReply:
    """Run a tool-enabled chat, retrying once when the reply is a broken tool call or
    announces an action without calling a tool."""
    text = "".join([d async for d in svc.gateway.chat_stream(req)])
    parsed = parse_reply(text, tools)
    for _ in range(2):
        if parsed.malformed:
            nudge = REPAIR_INSTRUCTION
        elif not parsed.tool_calls and looks_unfulfilled(parsed.content):
            # Show the model its own announcement so it follows through with the call.
            nudge = f"[Assistant]\n{parsed.content}\n\n[Instruction]\n{NUDGE_INSTRUCTION}"
        else:
            break
        retry = replace(req, prompt=f"{req.prompt}\n\n{nudge}" if nudge.startswith("[") else
                        f"{req.prompt}\n\n[Instruction]\n{nudge}",
                        turns=[], conversation_hint=None)
        text = "".join([d async for d in svc.gateway.chat_stream(retry)])
        parsed = parse_reply(text, tools)
    if parsed.malformed:  # still no valid call: hand back plain text, never an error
        parsed = ParsedReply(text, [])
    return parsed


def _tool_response(body: ChatCompletionRequest, parsed, prompt: str, completion_id: str,
                   created: int, model_name: str):
    finish = "tool_calls" if parsed.tool_calls else "stop"
    content = parsed.content or None
    usage = {"prompt_tokens": _approx_tokens(prompt),
             "completion_tokens": _approx_tokens(parsed.content + json.dumps(parsed.tool_calls))}
    usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
    if not body.stream:
        message: dict = {"role": "assistant", "content": content}
        if parsed.tool_calls:
            message["tool_calls"] = parsed.tool_calls
        return {"id": completion_id, "object": "chat.completion", "created": created,
                "model": model_name,
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": usage}

    def chunk(delta: dict, fin: str | None = None) -> dict:
        return {"id": completion_id, "object": "chat.completion.chunk", "created": created,
                "model": model_name,
                "choices": [{"index": 0, "delta": delta, "finish_reason": fin}]}

    async def events() -> AsyncIterator[bytes]:
        yield _sse(chunk({"role": "assistant", "content": content or ""}))
        if parsed.tool_calls:
            yield _sse(chunk({"tool_calls": [{"index": i, **c}
                                             for i, c in enumerate(parsed.tool_calls)]}))
        yield _sse(chunk({}, finish))
        if (body.stream_options or {}).get("include_usage"):
            yield _sse({"id": completion_id, "object": "chat.completion.chunk",
                        "created": created, "model": model_name, "choices": [], "usage": usage})
        yield _sse("[DONE]")

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/v1/chat/completions")
async def chat_completions(body: ChatCompletionRequest, request: Request,
                           svc: Services = Depends(get_services)):
    request.state.model = body.model  # logged even if it does not resolve
    spec = resolve_model(body.model, "chat")
    request.state.model = body.model or spec.id
    messages = [m.model_dump() for m in body.messages]
    tools = active_tools(body.tools, body.tool_choice)
    if tools:
        messages = inject(messages, tools, body.tool_choice)
    flat = flatten_messages(messages, FINAL_INSTRUCTION if tools else None)
    if not flat.text and not flat.images:
        raise InvalidRequest("messages contain no text or images")

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
    completion_id = "chatcmpl-" + uuid.uuid4().hex[:24]
    created = int(time.time())
    model_name = body.model or spec.id
    if tools:
        # Tool calls can only be recognised once the whole reply is in, so buffer it.
        try:
            parsed = await collect_tool_reply(svc, req, tools)
        finally:
            cancel.set()
        return _tool_response(body, parsed, flat.text, completion_id, created, model_name)

    stream = svc.gateway.chat_stream(req)

    if not body.stream:
        text = "".join([d async for d in stream])
        return {
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": model_name,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": _approx_tokens(flat.text),
                "completion_tokens": _approx_tokens(text),
                "total_tokens": _approx_tokens(flat.text) + _approx_tokens(text),
            },
        }

    def chunk(delta: dict, finish: str | None = None) -> dict:
        return {"id": completion_id, "object": "chat.completion.chunk", "created": created,
                "model": model_name,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}

    async def events(first: str | None) -> AsyncIterator[bytes]:
        # ``first`` is the pre-fetched first delta when heartbeats are disabled;
        # when enabled it is None and the first token is awaited here, after the
        # 200 (headers) has already been committed to the client.
        try:
            if first is None and svc.settings.stream_heartbeat_interval > 0:
                # C4: some clients/proxies drop idle SSE connections while the
                # gateway waits for the first upstream token (up to
                # first_token_timeout). Send SSE comment heartbeats until it
                # arrives. asyncio.wait (not wait_for) polls so the in-flight
                # __anext__ is never cancelled between heartbeats.
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
            produced = first or ""
            yield _sse(chunk({"role": "assistant", "content": produced}))
            async for delta in stream:
                produced += delta
                yield _sse(chunk({"content": delta}))
            yield _sse(chunk({}, "stop"))
            if (body.stream_options or {}).get("include_usage"):
                usage = {"prompt_tokens": _approx_tokens(flat.text),
                         "completion_tokens": _approx_tokens(produced)}
                usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
                yield _sse({"id": completion_id, "object": "chat.completion.chunk",
                            "created": created, "model": model_name, "choices": [],
                            "usage": usage})
        except Muse2APIError as exc:
            yield _sse(exc.to_body())
        except Exception:  # noqa: BLE001
            log.exception("stream crashed")
            yield _sse({"error": {"message": "internal error", "type": "server_error"}})
        finally:
            cancel.set()
            await stream.aclose()
        yield _sse("[DONE]")

    # With stream_heartbeat_interval set (C4), the 200 goes out immediately and
    # ': keepalive' SSE comments are sent while the first token is pending;
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
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
