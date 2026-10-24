"""Parsing helpers for the chat NDJSON events.

The gateway's events are intentionally loose JSON: text deltas are transient,
while the final ``delta.message_done`` carries a transcript and a list of
resources. Media can appear as a top-level resource, inside a tool result, or as
a markdown link to ``sandbox://``. These helpers normalise all three into
:class:`MediaRef` values without depending on a browser DOM.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

_SANDBOX_MARKDOWN = re.compile(r"!\[[^\]]*\]\((sandbox://[^)]+)\)")
_HTTP_MEDIA = re.compile(
    r"https?://[^\s)\"'<>]+\.(?:png|jpe?g|webp|gif|avif|mp4|webm|mov)(?:\?[^\s)\"'<>]*)?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class MediaRef:
    uri: str
    mime: str
    kind: str
    width: int | None = None
    height: int | None = None
    expires_at_ms: int | None = None

    @property
    def is_remote(self) -> bool:
        return self.uri.startswith(("http://", "https://"))


def _as_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _one_of(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _mime_from_uri(uri: str) -> str:
    suffix = uri.lower().split("?", 1)[0].rsplit(".", 1)[-1] if "." in uri else ""
    return {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
        "gif": "image/gif",
        "avif": "image/avif",
        "mp4": "video/mp4",
        "webm": "video/webm",
        "mov": "video/quicktime",
    }.get(suffix, "")


def _kind_for(mime: str, kind: Any = None) -> str:
    if kind in ("image", "video"):
        return str(kind)
    if mime.startswith("image/"):
        return "image"
    if mime.startswith("video/"):
        return "video"
    return ""


def _media_from_resource(value: Any) -> MediaRef | None:
    data = _as_dict(value)
    if data is None or data.get("missing") is True:
        return None
    # ``metadata`` is often a JSON string containing a signed media descriptor.
    metadata = _as_dict(data.get("metadata"))
    media = _as_dict(data.get("media")) or (metadata or {}).get("media")
    if media is not None:
        merged = dict(data)
        merged.update(_as_dict(media) or {})
        data = merged
    uri = _one_of(data, "uri", "path", "url", "src", "download_url", "media_url")
    if not isinstance(uri, str) or not uri.strip():
        return None
    mime = _one_of(data, "mime_type", "mime", "content_type") or _mime_from_uri(uri)
    if not isinstance(mime, str):
        mime = ""
    kind = _kind_for(mime, data.get("kind") or data.get("file_type") or data.get("type"))
    if uri.startswith(("sandbox://", "http://", "https://", "/")):
        normalized = uri
    elif _mime_from_uri(uri) or kind:
        # ``fs.list`` and presentation cards sometimes expose paths relative to
        # the workspace root (e.g. ``workspace/imagine_media/x.webp``).
        normalized = _to_sandbox(uri)
    else:
        return None
    if not kind:
        return None
    # ``variants.original`` can point at the VM's private gateway host, which
    # is not resolvable from this process. The sandbox path is the reliable
    # source (``fs.raw``) and should win when both are present.
    width = _int_or_none(_one_of(data, "width", "w"))
    height = _int_or_none(_one_of(data, "height", "h"))
    expires = _int_or_none(data.get("expires_at_ms"))
    return MediaRef(
        uri=normalized,
        mime=mime,
        kind=kind,
        width=width,
        height=height,
        expires_at_ms=expires,
    )


def _walk_content(content: Any, found: list[MediaRef]) -> None:
    if not isinstance(content, list):
        return
    for part in content:
        if not isinstance(part, dict):
            continue
        kind = part.get("type")
        if kind in ("image", "video", "media", "file"):
            ref = _media_from_resource(part)
            if ref is not None:
                found.append(ref)
        elif kind == "tool_call":
            result = _as_dict(part.get("result"))
            if result is None:
                continue
            metadata = _as_dict(result.get("metadata"))
            if metadata is not None:
                file_paths = metadata.get("file_paths")
                file_type = str(metadata.get("file_type") or "file")
                if isinstance(file_paths, list):
                    for path in file_paths:
                        if isinstance(path, str):
                            mime = _mime_from_uri(path)
                            if mime:
                                found.append(
                                    MediaRef(
                                        uri=_to_sandbox(path),
                                        mime=mime,
                                        kind=_kind_for(mime, file_type),
                                    )
                                )
                for key in ("output", "stdout"):
                    output = _as_dict(result.get(key))
                    if output is not None:
                        _walk_media_paths(output, found)
            resources = result.get("resources")
            if isinstance(resources, list):
                for resource in resources:
                    ref = _media_from_resource(resource)
                    if ref is not None:
                        found.append(ref)
        elif kind == "text" and isinstance(part.get("text"), str):
            _walk_text(part["text"], found)
        elif kind == "sub_agent":
            _walk_content(part.get("content"), found)


def _walk_media_paths(data: dict[str, Any], found: list[MediaRef]) -> None:
    paths = data.get("media_paths")
    if not isinstance(paths, list):
        return
    media_type = str(data.get("media_type") or "image")
    for path in paths:
        if not isinstance(path, str):
            continue
        mime = _mime_from_uri(path)
        if mime:
            found.append(
                MediaRef(uri=_to_sandbox(path), mime=mime, kind=_kind_for(mime, media_type))
            )


def _walk_text(text: str, found: list[MediaRef]) -> None:
    for match in _SANDBOX_MARKDOWN.finditer(text):
        uri = match.group(1)
        mime = _mime_from_uri(uri)
        if mime:
            found.append(MediaRef(uri=uri, mime=mime, kind=_kind_for(mime)))
    for match in _HTTP_MEDIA.finditer(text):
        uri = match.group(0)
        mime = _mime_from_uri(uri)
        if mime:
            found.append(MediaRef(uri=uri, mime=mime, kind=_kind_for(mime)))


def _to_sandbox(path: str) -> str:
    if path.startswith("sandbox://"):
        return path
    if path.startswith("/home/hatch/workspace/"):
        return "sandbox://workspace/" + path[len("/home/hatch/workspace/") :]
    if path.startswith("/home/hatch/"):
        return "sandbox://workspace/" + path[len("/home/hatch/") :]
    if path.startswith("/workspace/"):
        return "sandbox://" + path.lstrip("/")
    if path.startswith("workspace/"):
        return "sandbox://" + path
    if path.startswith("/"):
        return "sandbox://workspace" + path
    return "sandbox://workspace/" + path


def _dedupe_media(found: list[MediaRef]) -> list[MediaRef]:
    unique: list[MediaRef] = []
    seen: set[str] = set()
    for ref in found:
        if ref.uri in seen:
            continue
        seen.add(ref.uri)
        unique.append(ref)
    return unique


def _media_from_path(path: str, *, kind: Any = None, mime: Any = None) -> MediaRef | None:
    if not isinstance(path, str) or not path.strip():
        return None
    resolved_mime = mime if isinstance(mime, str) and mime else _mime_from_uri(path)
    resolved_kind = _kind_for(resolved_mime, kind)
    if not resolved_kind:
        return None
    if not resolved_mime:
        resolved_mime = "image/webp" if resolved_kind == "image" else "video/mp4"
    return MediaRef(uri=_to_sandbox(path), mime=resolved_mime, kind=resolved_kind)


def extract_media_from_presentation(payload: dict[str, Any]) -> list[MediaRef]:
    """Extract media from a ``delta.presentation`` (image/video card) payload.

    Presentation cards are separate events from ``delta.message_done``. Their
    shape is ``{"kind":"image", "image_path": "...", "data":{"images":[...]}}``;
    the same event may be replayed as an update, so callers should dedupe by
    ``id``/``message_id`` when a turn is being assembled.
    """
    found: list[MediaRef] = []
    kind = payload.get("kind")

    for key in ("image_path", "video_path", "file_path"):
        ref = _media_from_path(
            payload.get(key),
            kind=kind,
            mime=payload.get("mime_type") or payload.get("mime"),
        )
        if ref is not None:
            found.append(ref)

    data = _as_dict(payload.get("data"))
    containers = [data] if data is not None else []
    containers.append(payload)
    for container in containers:
        for key in ("images", "videos", "files", "media", "attachments"):
            items = container.get(key)
            if not isinstance(items, list):
                continue
            for item in items:
                ref = _media_from_resource(item)
                if ref is not None:
                    found.append(ref)
        for key in ("image", "video", "media", "file"):
            ref = _media_from_resource(container.get(key))
            if ref is not None:
                found.append(ref)
        ref = _media_from_resource(container)
        if ref is not None:
            found.append(ref)

    for key in ("fallback_text", "display_text", "content"):
        text = payload.get(key)
        if isinstance(text, str):
            _walk_text(text, found)

    presentations = payload.get("presentations")
    if isinstance(presentations, list):
        for presentation in presentations:
            if isinstance(presentation, dict):
                found.extend(extract_media_from_presentation(presentation))

    return _dedupe_media(found)


def extract_media(payload: dict[str, Any]) -> list[MediaRef]:
    """Return every image/video referenced by a ``delta.message_done`` payload."""
    found: list[MediaRef] = []

    resources = payload.get("resources")
    if isinstance(resources, list):
        for resource in resources:
            ref = _media_from_resource(resource)
            if ref is not None:
                found.append(ref)

    transcript = _as_dict(payload.get("transcript"))
    if transcript is not None:
        messages = transcript.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if isinstance(message, dict):
                    _walk_content(message.get("content"), found)

    display_text = payload.get("display_text")
    if isinstance(display_text, str):
        _walk_text(display_text, found)

    # Presentation cards are emitted as separate ``delta.presentation`` events,
    # but a done payload can also carry them (history replay and some updates).
    for presentation in payload.get("presentations") or []:
        if isinstance(presentation, dict):
            found.extend(extract_media_from_presentation(presentation))

    return _dedupe_media(found)


def extract_final_text(payload: dict[str, Any]) -> str:
    """Best-effort final assistant text from a message-done payload.

    A message-done transcript can contain the whole thread. Use the last
    assistant message so follow-up turns do not re-emit earlier replies.
    """
    transcript = _as_dict(payload.get("transcript"))
    text = ""
    if transcript is not None:
        for message in transcript.get("messages") or []:
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            parts: list[str] = []
            for part in message.get("content") or []:
                if isinstance(part, dict) and part.get("type") == "text":
                    part_text = part.get("text")
                    if isinstance(part_text, str):
                        parts.append(part_text)
            if parts:
                text = "".join(parts)
    display = payload.get("display_text")
    if isinstance(display, str) and len(display.strip()) > len(text.strip()):
        text = display
    return text


def extract_error(payload: dict[str, Any]) -> tuple[str, str] | None:
    """Return ``(code, message)`` for an error surfaced in an event payload."""
    error = payload.get("error")
    if error is None:
        code = payload.get("code")
        message = payload.get("message") or payload.get("detail")
        if code or message:
            return str(code or "upstream_error"), str(message or code)
        return None
    if not isinstance(error, dict):
        if isinstance(error, str) and error:
            return "upstream_error", error
        return None
    code = str(error.get("code") or "upstream_error")
    message = str(error.get("message") or error.get("detail") or code)
    return code, message


def is_subagent(payload: dict[str, Any]) -> bool:
    return (payload.get("stream_lane") or payload.get("transcript_surface")) == "subagent"
