"""Direct HTTP/Noise driver for muse.ai.

The browser driver automates the web UI. This driver speaks the same protocol the
web UI uses, without Chromium:

* discover the account's workspace from ``/thread/new``,
* mint a short-lived hatch token over plain HTTP,
* open the account's Noise WebSocket and tunnel HTTP requests through it,
* create an isolated thread per API call by sending a fresh UUID as
  ``session_id`` (the gateway treats that as a new thread),
* stream the assistant NDJSON events and download generated media from the
  workspace with ``fs.raw``.

The protocol is undocumented and can change; the constants and parsing live in
``auth.py``, ``noise.py`` and ``events.py`` so a site update is easy to isolate.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from urllib.parse import quote

from ...accounts.model import Account
from ...config import Settings
from ...errors import (
    FeatureNotImplemented,
    UpstreamAuthError,
    UpstreamError,
    UpstreamQuotaError,
    UpstreamRefused,
    UpstreamTimeout,
)
from ...upstream import muse
from ..base import (
    ChatRequest,
    DriverCapabilities,
    ImageRequest,
    InputImage,
    MediaResult,
    MuseDriver,
    ProgressCallback,
    SessionInfo,
    VideoRequest,
)
from .auth import GatewayTarget, fetch_token, resolve_target
from .events import (
    MediaRef,
    extract_error,
    extract_final_text,
    extract_media,
    extract_media_from_presentation,
    is_subagent,
)
from .noise import (
    GatewayError,
    GatewayToken,
    HttpStatusError,
    NoiseClient,
    StreamReset,
    Subscription,
    TransportClosed,
    TransportError,
    connect_gateway,
)

log = logging.getLogger(__name__)

CAPABILITIES = [
    "hotel_results",
    "chat_cancel",
    "delta_stream",
    "custom_reactions",
    "custom_reactions_facebook_thumbs_up_v1",
]
SUBSCRIBE_CAPABILITIES = CAPABILITIES + ["voice_reply_captions", "voice_user_live_transcript"]

_UPLOAD_DIR = "workspace/user/files"
_CHUNK = 512 * 1024
_IMAGE_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/avif": ".avif",
}


@dataclass
class _Grant:
    cookie_sig: str
    credentials: GatewayToken
    expires_at: float


@dataclass
class ChatOutcome:
    text: str = ""
    media: list[MediaRef] = field(default_factory=list)
    done_id: str = ""
    status: str = ""


def _unique_media(items: list[MediaRef]) -> list[MediaRef]:
    out: list[MediaRef] = []
    seen: set[str] = set()
    for item in items:
        if item.uri in seen:
            continue
        seen.add(item.uri)
        out.append(item)
    return out


class _LiveChat:
    """One isolated thread, reusing one Noise connection for follow-up turns."""

    def __init__(
        self, driver: HttpDriver, account: Account, *, timeout: float, cancel: asyncio.Event | None
    ) -> None:
        self.driver = driver
        self.account = account
        self.timeout = timeout
        self.cancel = cancel
        self.client: NoiseClient | None = None
        self.sub: Subscription | None = None
        self.session_id = str(uuid.uuid4())
        self.session_ack: dict = {}
        self.uploaded: list[str] = []
        self.done_ids: set[str] = set()
        self.presentation_media: dict[str, list[MediaRef]] = {}
        self.last_outcome = ChatOutcome()

    async def __aenter__(self) -> _LiveChat:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def open(
        self, prompt: str, images: list[InputImage], *, first_token_timeout: float | None = None
    ) -> dict:
        if self.client is None:
            self.client = await self.driver._connect(self.account)
        refs = await self.driver._upload_images(self.client, images, self.uploaded)

        # A follow-up after a closed subscription must subscribe before sending,
        # otherwise the transient text deltas for the new turn are lost. The
        # replayed old message-done events are filtered by ``done_ids``.
        is_followup = bool(self.session_ack)
        if is_followup and (self.sub is None or self.sub.done):
            if self.sub is not None:
                await self.sub.close()
            self.sub = await self.driver._subscribe(self.client, self.session_id)

        ack = await self.driver._send_chat(
            self.client, self.session_id, prompt, refs, timeout=min(self.timeout, 60.0)
        )
        self.session_ack = ack
        returned = str(ack.get("session_id") or "")
        if returned and returned != self.session_id:
            # The server normally creates the thread for a fresh UUID. If it
            # ever falls back to the primary session, do not silently mix users.
            log.warning(
                "gateway did not create a new thread (asked %s, got %s)", self.session_id, returned
            )
            self.session_id = returned
        if not is_followup and self.sub is None:
            self.sub = await self.driver._subscribe(self.client, self.session_id)
        return ack

    async def turn(
        self,
        *,
        first_token_timeout: float | None,
        on_progress: ProgressCallback | None = None,
        ignore_done_ids: set[str] | None = None,
    ) -> AsyncIterator[str]:
        if self.sub is None or self.client is None:
            raise UpstreamError("chat session is not open")
        ignore = ignore_done_ids or set()
        emitted: list[str] = []
        started = time.monotonic()
        deadline = started + max(1.0, self.timeout)
        first_deadline = (
            started + max(1.0, first_token_timeout) if first_token_timeout is not None else None
        )
        got_text = False
        outcome = ChatOutcome()
        known_presentations = set(self.presentation_media)
        turn_presentations: set[str] = set()
        live_media: list[MediaRef] = []
        while True:
            if self.cancel is not None and self.cancel.is_set():
                raise asyncio.CancelledError
            now = time.monotonic()
            if now >= deadline:
                raise UpstreamTimeout("muse.ai chat timed out")
            if first_deadline is not None and not got_text and now >= first_deadline:
                raise UpstreamTimeout("muse.ai did not send a first token in time")
            try:
                line = await self.sub.read_line(timeout=min(0.75, max(0.05, deadline - now)))
            except asyncio.TimeoutError:
                continue
            except EOFError:
                if outcome.done_id:
                    break
                raise UpstreamError(
                    "muse.ai subscription ended before the reply completed"
                ) from None
            except HttpStatusError as exc:
                raise self.driver._map_status(exc.status, exc.body) from exc
            except (StreamReset, TransportClosed) as exc:
                raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
            except TransportError as exc:
                raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            name = event.get("event")
            payload = event.get("payload")
            if not isinstance(payload, dict):
                payload = {}
            if not name:
                # A failed subscription can be a bare JSON error object rather
                # than a named event.
                err = extract_error(event)
                if err is None:
                    err = extract_error(payload)
                if err is not None:
                    code, message = err
                    raise _map_gateway_error(code, message)
                continue
            if name == "error":
                self._raise_payload_error(payload)
            elif name == "delta.text_append":
                if is_subagent(payload):
                    continue
                text = payload.get("text")
                if isinstance(text, str) and text:
                    emitted.append(text)
                    got_text = True
                    yield text
            elif name == "delta.presentation":
                if is_subagent(payload):
                    continue
                presentation_id = str(payload.get("message_id") or payload.get("id") or "")
                if (
                    presentation_id
                    and presentation_id in known_presentations
                    and presentation_id not in turn_presentations
                ):
                    # A replayed/updated card from an earlier turn.
                    continue
                refs = extract_media_from_presentation(payload)
                if not refs:
                    continue
                if presentation_id:
                    # Replacing an update for the same card keeps the latest
                    # path/variants without duplicating it.
                    self.presentation_media[presentation_id] = refs
                    turn_presentations.add(presentation_id)
                else:
                    live_media.extend(refs)
                if on_progress:
                    on_progress(50)
            elif name == "message.assistant":
                if is_subagent(payload):
                    continue
                message_id = str(payload.get("message_id") or "")
                if message_id and (message_id in ignore or message_id in self.done_ids):
                    continue
                live_media.extend(extract_media(payload))
            elif name == "delta.message_done":
                if is_subagent(payload):
                    continue
                message_id = str(payload.get("message_id") or "")
                if message_id and (message_id in ignore or message_id in self.done_ids):
                    continue
                self._raise_payload_error(payload)
                status = str(payload.get("status") or "")
                if status and status not in ("completed", "complete", "success"):
                    self._raise_payload_error(payload)
                    raise UpstreamError(f"muse.ai reply ended with status {status!r}")
                final_text = extract_final_text(payload)
                current = "".join(emitted)
                if final_text:
                    if not current:
                        emitted.append(final_text)
                        current = final_text
                        got_text = True
                        yield final_text
                    elif final_text.startswith(current) and len(final_text) > len(current):
                        suffix = final_text[len(current) :]
                        emitted.append(suffix)
                        current = final_text
                        yield suffix
                    elif not current.startswith(final_text):
                        log.debug("final reply text differs from streamed deltas; keeping deltas")
                outcome.text = current or final_text
                media = extract_media(payload) + live_media
                for presentation_id in turn_presentations:
                    media.extend(self.presentation_media.get(presentation_id, []))
                outcome.media = _unique_media(media)
                outcome.done_id = message_id
                outcome.status = status or "completed"
                if message_id:
                    self.done_ids.add(message_id)
                if on_progress:
                    on_progress(90)
                break
        self.last_outcome = outcome

    @staticmethod
    def _raise_payload_error(payload: dict) -> None:
        err = extract_error(payload)
        if err is None:
            return
        code, message = err
        raise _map_gateway_error(code, message)

    async def collect_late_media(self, seconds: float) -> None:
        """Read briefly after ``message_done`` for a late presentation card.

        The UI's attachment can be emitted as its own ``delta.presentation``
        event around the done frame. Normally it arrives before done, but a
        small grace period makes the media flow robust to ordering changes.
        """
        if self.sub is None or self.client is None:
            return
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            if self.cancel is not None and self.cancel.is_set():
                raise asyncio.CancelledError
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                line = await self.sub.read_line(timeout=remaining)
            except asyncio.TimeoutError:
                return
            except EOFError:
                return
            except HttpStatusError as exc:
                raise self.driver._map_status(exc.status, exc.body) from exc
            except (StreamReset, TransportClosed) as exc:
                raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
            except TransportError as exc:
                raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            name = event.get("event")
            payload = event.get("payload")
            if not isinstance(payload, dict) or is_subagent(payload):
                continue
            refs: list[MediaRef] = []
            if name == "delta.presentation":
                presentation_id = str(payload.get("message_id") or payload.get("id") or "")
                if presentation_id and presentation_id in self.presentation_media:
                    continue
                refs = extract_media_from_presentation(payload)
                if presentation_id and refs:
                    self.presentation_media[presentation_id] = refs
            elif name in ("message.assistant", "delta.message_done"):
                refs = extract_media(payload)
            if refs:
                self.last_outcome.media = _unique_media(self.last_outcome.media + refs)

    async def close(self) -> None:
        if self.client is not None and self.uploaded:
            for path in self.uploaded:
                try:
                    await self.driver._fs_delete(self.client, path)
                except Exception:  # noqa: BLE001 - cleanup is best effort
                    pass
            self.uploaded.clear()
        if self.sub is not None:
            await self.sub.close()
            self.sub = None
        if self.client is not None:
            await self.client.close()
            self.client = None


class HttpDriver(MuseDriver):
    name = "http"
    # muse.ai can post the completion text before the presentation card (and
    # for video, up to a minute before the file finishes encoding). Match the
    # browser driver's grace periods after a text-only done frame.
    _MEDIA_GRACE = {"image": 8.0, "video": 90.0}
    capabilities = DriverCapabilities(
        chat=True,
        chat_images=True,
        image=True,
        image_edit=True,
        video=True,
        renew_session=True,
    )

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._grants: dict[str, _Grant] = {}
        self._grant_locks: dict[str, asyncio.Lock] = {}

    async def startup(self) -> None:
        # The transport dependencies are imported lazily; fail with the
        # project's normal 501 rather than an ImportError at startup.
        try:
            import cryptography  # noqa: F401
            import curl_cffi  # noqa: F401
            import zstandard  # noqa: F401
        except ImportError as exc:
            raise FeatureNotImplemented(
                "the http driver requires 'curl_cffi', 'cryptography' and 'zstandard'"
            ) from exc

    async def shutdown(self) -> None:
        self._grants.clear()

    async def health(self) -> dict:
        return {"driver": self.name, "ok": True, "cached_grants": len(self._grants)}

    # ------------------------------------------------------------------ auth
    @staticmethod
    def _cookie_sig(cookies: dict[str, str]) -> str:
        payload = "\x00".join(f"{k}={v}" for k, v in sorted(cookies.items()) if v)
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def _invalidate_grant(self, account: Account) -> None:
        self._grants.pop(account.id, None)

    async def _credentials(self, account: Account, *, force: bool = False) -> GatewayToken:
        sig = self._cookie_sig(account.cookies)
        now = time.monotonic()
        cached = self._grants.get(account.id)
        if (
            not force
            and cached is not None
            and cached.cookie_sig == sig
            and cached.expires_at > now
        ):
            return cached.credentials
        lock = self._grant_locks.setdefault(account.id, asyncio.Lock())
        async with lock:
            cached = self._grants.get(account.id)
            if (
                not force
                and cached is not None
                and cached.cookie_sig == sig
                and cached.expires_at > time.monotonic()
            ):
                return cached.credentials
            target: GatewayTarget = await resolve_target(account.cookies)
            credentials = await fetch_token(account.cookies, target)
            self._grants[account.id] = _Grant(
                cookie_sig=sig, credentials=credentials, expires_at=time.monotonic() + 300.0
            )
            return credentials

    async def _connect(self, account: Account, *, force: bool = False) -> NoiseClient:
        credentials = await self._credentials(account, force=force)
        try:
            return await self._connect_once(credentials)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - token may be expired/stale
            if force:
                raise UpstreamError(f"could not open the muse.ai gateway: {exc}") from exc
            log.info("gateway connection failed on %s (%s); refreshing token", account.id, exc)
            self._invalidate_grant(account)
            credentials = await self._credentials(account, force=True)
            try:
                return await self._connect_once(credentials)
            except Exception as exc2:  # noqa: BLE001
                raise UpstreamError(f"could not open the muse.ai gateway: {exc2}") from exc2

    @staticmethod
    async def _connect_once(credentials: GatewayToken) -> NoiseClient:
        try:
            return await connect_gateway(credentials, user_agent=muse.USER_AGENT)
        except TransportError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError(f"gateway handshake failed: {exc}") from exc

    # --------------------------------------------------------------- requests
    @staticmethod
    def _map_status(status: int, body: bytes) -> UpstreamError:
        message = body.decode("utf-8", "replace").strip()[:400]
        if status in (401, 403):
            return UpstreamAuthError(f"muse.ai rejected the session (HTTP {status})")
        if status in (402, 429):
            return UpstreamQuotaError(message or f"muse.ai quota/rate limit (HTTP {status})")
        if status == 408:
            return UpstreamTimeout("muse.ai gateway timed out")
        return UpstreamError(message or f"muse.ai gateway returned HTTP {status}")

    async def _request(
        self,
        client: NoiseClient,
        method: str,
        path: str,
        *,
        payload: dict | None = None,
        timeout: float = 30.0,
        max_output: int = 256 * 1024 * 1024,
    ) -> tuple[int, dict[str, str], bytes]:
        body = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        headers = [("Content-Type", "application/json")] if payload is not None else []
        try:
            status, response_headers, data = await client.request(
                method, path, body, headers, timeout=timeout, max_output=max_output
            )
        except HttpStatusError as exc:
            raise self._map_status(exc.status, exc.body) from exc
        except (StreamReset, TransportClosed) as exc:
            raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
        except TransportError as exc:
            raise UpstreamError(f"muse.ai transport failed: {exc}") from exc
        if status >= 400:
            raise self._map_status(status, data)
        return status, response_headers, data

    async def _request_json(
        self,
        client: NoiseClient,
        method: str,
        path: str,
        payload: dict | None = None,
        *,
        timeout: float = 30.0,
    ) -> dict:
        _, _, data = await self._request(client, method, path, payload=payload, timeout=timeout)
        if not data:
            return {}
        try:
            body = json.loads(data)
        except ValueError as exc:
            raise UpstreamError(f"muse.ai returned non-JSON from {path}: {data[:200]!r}") from exc
        if isinstance(body, dict) and body.get("ok") is False:
            err = body.get("error")
            if isinstance(err, dict):
                raise _map_gateway_error(
                    str(err.get("code") or "upstream_error"),
                    str(err.get("message") or "upstream error"),
                )
            if isinstance(err, str) and err:
                raise _map_gateway_error("upstream_error", err)
            raise UpstreamError(str(body.get("message") or "muse.ai returned ok=false"))
        return body if isinstance(body, dict) else {}

    async def _subscribe(self, client: NoiseClient, session_id: str) -> Subscription:
        body = json.dumps(
            {
                "after_stream_seq": 0,
                "after_chat_event_seq": 0,
                "capabilities": SUBSCRIBE_CAPABILITIES,
                "session_id": session_id,
            }
        ).encode()
        last: Exception | None = None
        for attempt in range(3):
            try:
                return await client.subscribe(
                    "/chat/subscribe",
                    body,
                    [("Content-Type", "application/json")],
                    timeout=30.0,
                )
            except (StreamReset, TransportClosed, HttpStatusError, GatewayError) as exc:
                last = exc
                await asyncio.sleep(0.2 * (attempt + 1))
        raise UpstreamError(f"could not subscribe to the muse.ai thread: {last}") from last

    async def _send_chat(
        self,
        client: NoiseClient,
        session_id: str,
        prompt: str,
        images: list[dict],
        *,
        timeout: float,
    ) -> dict:
        body: dict = {
            "node_id": str(uuid.uuid4()),
            "capabilities": CAPABILITIES,
            "session_id": session_id,
            "timezone": "UTC",
            "metadata": {"thread_is_dictation_used": False},
        }
        if images:
            items: list[dict] = []
            if prompt:
                items.append({"type": "text", "text": prompt})
            items.extend(images)
            body["items"] = items
        else:
            body["message"] = prompt
        return await self._request_json(client, "POST", "/chat/stream", body, timeout=timeout)

    # --------------------------------------------------------------- uploads
    @staticmethod
    def _upload_path(image: InputImage) -> tuple[str, str]:
        ext = _IMAGE_EXT.get(image.mime.split(";")[0].strip().lower())
        if ext is None:
            ext = mimetypes.guess_extension(image.mime.split(";")[0].strip()) or ".bin"
        name = f"muse2api_{uuid.uuid4().hex[:10]}{ext}"
        filename = name
        return f"{_UPLOAD_DIR}/{name}", filename

    async def _upload_images(
        self,
        client: NoiseClient,
        images: list[InputImage],
        uploaded: list[str] | None = None,
    ) -> list[dict]:
        refs: list[dict] = []
        for image in images:
            path, filename = self._upload_path(image)
            # Record before writing so a partial upload is also cleaned up by
            # the caller's finally block.
            if uploaded is not None:
                uploaded.append(path)
            data = image.data
            chunks = [data[i : i + _CHUNK] for i in range(0, len(data), _CHUNK)] or [b""]
            for index, chunk in enumerate(chunks):
                payload = {
                    "path": path,
                    "data_base64": base64.b64encode(chunk).decode(),
                    "overwrite": index == 0,
                    "append": index > 0,
                    "create_parent": index == 0,
                }
                await self._request_json(client, "POST", "/fs/write", payload, timeout=60.0)
            refs.append(
                {
                    "type": "file_ref",
                    "mime_type": image.mime.split(";")[0].strip() or "application/octet-stream",
                    "path": path,
                    "filename": filename,
                }
            )
        return refs

    async def _fs_delete(self, client: NoiseClient, path: str) -> None:
        await self._request(
            client,
            "POST",
            "/fs/delete",
            payload={"path": path.lstrip("/"), "recursive": False},
            timeout=15.0,
        )

    # ------------------------------------------------------------------- chat
    async def chat_stream(self, account: Account, req: ChatRequest) -> AsyncIterator[str]:
        chat = _LiveChat(self, account, timeout=req.timeout, cancel=req.cancel)
        try:
            await chat.open(req.prompt, req.images, first_token_timeout=req.first_token_timeout)
            async for delta in chat.turn(first_token_timeout=req.first_token_timeout):
                yield delta
        finally:
            await chat.close()

    # ------------------------------------------------------------------ media
    @staticmethod
    def _media_prompt(prompt: str, kind: str, size: str | None, duration: int | None = None) -> str:
        hints: list[str] = []
        if size:
            hints.append(f"aspect ratio {size}")
        if duration:
            hints.append(f"{duration} seconds long")
        verb = "Generate an image" if kind == "image" else "Generate a video"
        suffix = f" ({', '.join(hints)})" if hints else ""
        return f"{verb}{suffix}: {prompt}"

    def _generated_media(
        self, refs: list[MediaRef], kind: str, uploaded: list[str]
    ) -> list[MediaRef]:
        """Keep output media of the requested kind, not uploaded reference files."""
        blocked = {path.lstrip("/") for path in uploaded}
        out: list[MediaRef] = []
        for ref in refs:
            if ref.kind != kind:
                continue
            if not ref.is_remote and self._fs_path(ref.uri).lstrip("/") in blocked:
                continue
            out.append(ref)
        # A new thread can also receive onboarding/system cards. Prefer the
        # app's generated-media directory when it is present.
        preferred = [ref for ref in out if "/imagine_media/" in ref.uri]
        return preferred or out

    async def _run_media(
        self,
        account: Account,
        prompt: str,
        images: list[InputImage],
        kind: str,
        timeout: float,
        on_progress: ProgressCallback | None,
        cancel: asyncio.Event | None,
    ) -> MediaResult:
        chat = _LiveChat(self, account, timeout=timeout, cancel=cancel)
        try:
            await chat.open(prompt, images, first_token_timeout=None)
            if on_progress:
                on_progress(10)
            async for _ in chat.turn(
                first_token_timeout=None,
                on_progress=on_progress,
            ):
                pass
            first = chat.last_outcome
            media = self._generated_media(first.media, kind, chat.uploaded)
            if not media:
                await chat.collect_late_media(self._MEDIA_GRACE.get(kind, 8.0))
                media = self._generated_media(chat.last_outcome.media, kind, chat.uploaded)
            if not media:
                if on_progress:
                    on_progress(60)
                followup = (
                    f"Please attach the final {kind} file here in the chat, as a single attachment."
                )
                await chat.open(followup, [], first_token_timeout=None)
                async for _ in chat.turn(
                    first_token_timeout=None,
                    on_progress=on_progress,
                    ignore_done_ids=set(chat.done_ids),
                ):
                    pass
                second = chat.last_outcome
                media = self._generated_media(second.media, kind, chat.uploaded)
                if not media:
                    await chat.collect_late_media(self._MEDIA_GRACE.get(kind, 8.0))
                    media = self._generated_media(chat.last_outcome.media, kind, chat.uploaded)
                first.text = (first.text + "\n" + second.text).strip()
            if not media:
                raise UpstreamRefused(f"upstream replied with text only: {first.text[:300]}")
            if on_progress:
                on_progress(90)
            ref = media[-1]
            if chat.client is None:
                raise UpstreamError("gateway connection closed before media download")
            data, mime, width, height = await self._download(chat.client, account, ref, timeout)
            if on_progress:
                on_progress(100)
            return MediaResult(
                data=data,
                mime=mime,
                kind=kind,
                width=width or ref.width,
                height=height or ref.height,
            )
        finally:
            await chat.close()

    async def generate_image(self, account: Account, req: ImageRequest) -> list[MediaResult]:
        prompt = self._media_prompt(req.prompt, "image", req.size)
        results: list[MediaResult] = []
        for _ in range(max(1, req.n)):
            result = await self._run_media(
                account,
                prompt,
                req.reference_images,
                "image",
                req.timeout,
                req.on_progress,
                req.cancel,
            )
            result.revised_prompt = req.prompt
            results.append(result)
        return results

    async def generate_video(self, account: Account, req: VideoRequest) -> MediaResult:
        prompt = self._media_prompt(req.prompt, "video", req.size, req.duration)
        images = [req.first_frame] if req.first_frame else []
        return await self._run_media(
            account,
            prompt,
            images,
            "video",
            req.timeout,
            req.on_progress,
            req.cancel,
        )

    async def _download(
        self, client: NoiseClient, account: Account, ref: MediaRef, timeout: float
    ) -> tuple[bytes, str, int | None, int | None]:
        if ref.is_remote:
            data, mime = await self._download_remote(account, ref, timeout)
            return data, mime, ref.width, ref.height
        path = self._fs_path(ref.uri)
        encoded = quote(path, safe="/")
        status, headers, data = await self._request(
            client,
            "GET",
            f"/fs/raw/{encoded}",
            timeout=min(timeout, 300.0),
        )
        if status != 200:
            raise UpstreamError(f"could not download generated media (HTTP {status})")
        mime = headers.get("content-type", "").split(";", 1)[0].strip()
        if not mime or mime == "application/octet-stream":
            mime = ref.mime or "application/octet-stream"
        if not data:
            raise UpstreamError("generated media download was empty")
        self._validate_media(data, mime, ref.kind)
        return data, mime, ref.width, ref.height

    @staticmethod
    def _validate_media(data: bytes, mime: str, kind: str) -> None:
        head = data[:256].lstrip()
        if head.startswith(b"<"):
            raise UpstreamError("generated media download returned an HTML/XML page")
        lowered = mime.lower()
        if head.startswith(b"#EXTM3U") or "mpegurl" in lowered:
            raise UpstreamError(
                "generated video is an HLS playlist; this driver expects a direct MP4/WebM file"
            )
        if lowered.startswith(("text/", "application/json")):
            raise UpstreamError(f"generated {kind} download returned {mime}")
        if kind == "image" and lowered.startswith("video/"):
            raise UpstreamError("image request downloaded a video file")
        if kind == "video" and lowered.startswith("image/"):
            raise UpstreamError("video request downloaded an image file")

    @staticmethod
    def _fs_path(uri: str) -> str:
        # The gateway filesystem API rejects absolute paths. Every path it
        # accepts is relative to the workspace root (``workspace/...``).
        if uri.startswith("sandbox://"):
            path = uri[len("sandbox://") :]
        elif uri.startswith("/home/hatch/workspace/"):
            path = "workspace/" + uri[len("/home/hatch/workspace/") :]
        elif uri.startswith("/home/hatch/"):
            path = "workspace/" + uri[len("/home/hatch/") :]
        elif uri.startswith("/"):
            path = uri.lstrip("/")
        else:
            path = uri
        return path.lstrip("/")

    async def _download_remote(
        self, account: Account, ref: MediaRef, timeout: float
    ) -> tuple[bytes, str]:
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError as exc:  # pragma: no cover
            raise UpstreamError("curl_cffi is required to download remote media") from exc
        headers = {
            "accept": "*/*",
            "referer": muse.NEW_THREAD_URL,
            "user-agent": muse.USER_AGENT,
        }
        try:
            async with AsyncSession(impersonate="chrome", timeout=min(timeout, 300.0)) as session:
                response = await session.get(
                    ref.uri,
                    cookies={k: v for k, v in account.cookies.items() if v},
                    headers=headers,
                    allow_redirects=True,
                )
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError(f"could not download generated media: {exc}") from exc
        if response.status_code >= 400:
            raise UpstreamError(f"generated media host returned HTTP {response.status_code}")
        data = response.content or b""
        if not data:
            raise UpstreamError("generated media download was empty")
        mime = (response.headers.get("content-type") or "").split(";", 1)[0].strip()
        if not mime or mime == "application/octet-stream":
            mime = ref.mime or "application/octet-stream"
        self._validate_media(data, mime, ref.kind)
        return data, mime

    # --------------------------------------------------------------- lifecycle
    async def renew_session(self, account: Account) -> SessionInfo:
        info = await muse.renew_session(account.cookies)
        self._invalidate_grant(account)
        return info


def _map_gateway_error(code: str, message: str) -> UpstreamError:
    """Map a gateway JSON error to the service-layer error hierarchy."""
    combined = f"{code} {message}".lower()
    if any(
        word in combined
        for word in ("auth", "session_expired", "unauthorized", "forbidden", "login", "cookie")
    ):
        return UpstreamAuthError(message or code)
    if any(
        word in combined
        for word in (
            "quota",
            "credit",
            "insufficient",
            "rate_limit",
            "rate limit",
            "too_many",
            "capacity",
        )
    ):
        return UpstreamQuotaError(message or code)
    if any(word in combined for word in ("invalid", "bad_request", "unsupported")):
        error = UpstreamRefused(message or code)
        return error
    return UpstreamError(message or code)
