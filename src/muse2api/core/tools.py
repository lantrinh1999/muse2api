"""Prompt-based function calling for /v1/chat/completions.

muse.ai has no native tool calling, so tools are emulated: the tool schemas are
described to the model in a leading system message and it is asked to answer with a
``<tool_calls>[...]</tool_calls>`` block when it wants to call one. The reply is parsed
back into OpenAI ``tool_calls``. Prior tool calls/results in the history are rendered
into the same text format so multi-step agent loops stay consistent.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

OPEN_TAG = "<tool_calls>"
CLOSE_TAG = "</tool_calls>"
_BLOCK_RE = re.compile(r"<tool_calls>\s*(.*?)\s*</tool_calls>", re.S)
_OPEN_ONLY_RE = re.compile(r"<tool_calls>\s*(.*)$", re.S)
_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.S)

FINAL_INSTRUCTION = (
    "Continue the conversation above and reply as the Assistant to the last message. "
    "Output only the reply. If the task needs you to look at, search, read, edit or run "
    f"anything, your reply MUST contain the {OPEN_TAG}...{CLOSE_TAG} block right now. Never "
    "say you are going to do something without including the block in that same reply: a "
    "reply without the block is treated as your final answer."
)
NUDGE_INSTRUCTION = (
    "You said what you are going to do but did not call a tool, so nothing happened. "
    f"Reply now with ONLY the {OPEN_TAG}[...]{CLOSE_TAG} block for that action."
)
_INTENT_RE = re.compile(
    r"(tôi sẽ|mình sẽ|để tôi|để mình|tiếp theo|bây giờ tôi|bây giờ mình|i['’]ll |i will |"
    r"i['’]m going to|let me |let['’]s |i need to |first,? i|next,? i|now i|going to )",
    re.I)


REPAIR_INSTRUCTION = (
    "Your previous reply tried to call a tool but was not valid. Reply again with ONLY a "
    f"{OPEN_TAG}[...]{CLOSE_TAG} block that uses a listed tool name and includes every "
    "required argument, or answer in plain text if no tool is needed."
)
_DROP_KEYS = {"$schema", "additionalProperties", "title"}
_MAX_DESC = 1200


@dataclass
class ParsedReply:
    content: str
    tool_calls: list[dict[str, Any]]
    malformed: bool = False
    """The reply looked like a tool call but could not be turned into a valid one."""


def _compact(schema: Any) -> Any:
    """Drop schema keys that only cost prompt tokens."""
    if isinstance(schema, dict):
        return {k: _compact(v) for k, v in schema.items() if k not in _DROP_KEYS}
    if isinstance(schema, list):
        return [_compact(v) for v in schema]
    return schema


def _missing_required(args: str, schema: dict[str, Any]) -> bool:
    required = schema.get("required") if isinstance(schema, dict) else None
    if not required:
        return False
    try:
        data = json.loads(args)
    except ValueError:
        return True
    return not isinstance(data, dict) or any(k not in data for k in required)


def looks_unfulfilled(text: str) -> bool:
    """A short reply announcing an action (or ending on a colon) with no tool call."""
    t = text.strip()
    return bool(t) and len(t) < 600 and (bool(_INTENT_RE.search(t)) or t.endswith(":"))


def active_tools(tools: list[dict[str, Any]] | None, tool_choice: Any) -> list[dict[str, Any]]:
    """Function tools to expose, or [] when tools are absent or disabled."""
    if not tools or tool_choice == "none":
        return []
    return [t for t in tools if isinstance(t, dict) and isinstance(t.get("function"), dict)
            and t["function"].get("name")]


def build_system_prompt(tools: list[dict[str, Any]], tool_choice: Any) -> str:
    specs = []
    for t in tools:
        fn = t["function"]
        specs.append({"name": fn["name"],
                      "description": (fn.get("description") or "")[:_MAX_DESC],
                      "parameters": _compact(fn.get("parameters")
                                             or {"type": "object", "properties": {}})})
    lines = [
        "IMPORTANT: act only as a tool-calling assistant for the CLIENT's machine. Do NOT use "
        "any of your own built-in tools, shell, files, browser or sandbox, and never describe or "
        "inspect your own environment. The only way to read files, run commands or take actions "
        "is to call the client tools listed below.",
        "You can call the tools listed below. To call one or more tools, reply with ONLY a block "
        f"in exactly this format and nothing else:\n{OPEN_TAG}"
        '[{"name": "<tool name>", "arguments": {<arguments as a JSON object>}}]'
        f"{CLOSE_TAG}",
        "Use only the listed tool names and follow each tool's parameter schema. "
        "Tool results come back in later messages labelled [Tool result]. "
        "When no tool is needed, or once you have the information you need, answer normally "
        "in plain text without the block.",
    ]
    if tool_choice == "required":
        lines.append("You MUST call at least one tool in your next reply.")
    elif isinstance(tool_choice, dict) and tool_choice.get("function", {}).get("name"):
        lines.append(f"You MUST call the tool \"{tool_choice['function']['name']}\" "
                     "in your next reply.")
    lines.append("Available tools (JSON):\n" + json.dumps(specs, ensure_ascii=False, separators=(",", ":")))
    return "\n\n".join(lines)


def _render_calls(calls: list[dict[str, Any]]) -> str:
    out = []
    for c in calls:
        fn = c.get("function") or {}
        args = fn.get("arguments", "{}")
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except ValueError:
                pass
        out.append({"name": fn.get("name", ""), "arguments": args})
    return OPEN_TAG + json.dumps(out, ensure_ascii=False) + CLOSE_TAG


def inject(messages: list[dict[str, Any]], tools: list[dict[str, Any]],
           tool_choice: Any) -> list[dict[str, Any]]:
    """Message list with a tool system prompt prepended and tool history rendered as text."""
    out: list[dict[str, Any]] = [{"role": "system",
                                  "content": build_system_prompt(tools, tool_choice)}]
    names: dict[str, str] = {}
    for msg in messages:
        role = msg.get("role")
        calls = msg.get("tool_calls")
        if role == "assistant" and calls:
            for c in calls:
                names[c.get("id", "")] = (c.get("function") or {}).get("name", "")
            text = msg.get("content") if isinstance(msg.get("content"), str) else ""
            out.append({"role": "assistant",
                        "content": ((text + "\n") if text else "") + _render_calls(calls)})
        elif role == "tool":
            name = msg.get("name") or names.get(msg.get("tool_call_id", ""), "")
            label = f"[Tool result: {name}]\n" if name else "[Tool result]\n"
            content = msg.get("content")
            if isinstance(content, list):
                content = "\n".join(p.get("text", "") for p in content if isinstance(p, dict))
            out.append({"role": "tool", "content": label + (content or "")})
        else:
            out.append(msg)
    return out


def _load_calls(raw: str) -> list[Any] | None:
    raw = raw.strip()
    fence = _FENCE_RE.match(raw)
    if fence:
        raw = fence.group(1)
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if isinstance(data, dict):
        data = data.get("tool_calls", [data])
    return data if isinstance(data, list) else None


def parse_reply(text: str, tools: list[dict[str, Any]]) -> ParsedReply:
    """Split a model reply into plain content and validated OpenAI tool_calls."""
    m = _BLOCK_RE.search(text) or _OPEN_ONLY_RE.search(text)
    raw = m.group(1) if m else None
    if raw is None:
        # Models sometimes drop the tags and answer with bare JSON.
        stripped = text.strip()
        if stripped.startswith(("{", "[", "```")) and '"name"' in stripped:
            raw = stripped
    calls = _load_calls(raw) if raw is not None else None
    if not calls:
        # Tags present but unparsable JSON: ask the model to try again.
        return ParsedReply(text, [], malformed=m is not None)

    schemas = {t["function"]["name"]: t["function"].get("parameters") or {} for t in tools}
    allowed = set(schemas)
    result = []
    bad = False
    for c in calls:
        if not isinstance(c, dict):
            continue
        fn = c.get("function") if isinstance(c.get("function"), dict) else c
        name = fn.get("name")
        if name not in allowed:
            bad = True
            continue
        args = fn.get("arguments", fn.get("parameters", {}))
        if not isinstance(args, str):
            args = json.dumps(args if args is not None else {}, ensure_ascii=False)
        if _missing_required(args, schemas[name]):
            bad = True
            continue
        result.append({"id": "call_" + uuid.uuid4().hex[:24], "type": "function",
                       "function": {"name": name, "arguments": args}})
    if not result:
        return ParsedReply(text, [], malformed=True)
    before = text[:m.start()].strip() if m else ""
    return ParsedReply(before, result, malformed=bad)
