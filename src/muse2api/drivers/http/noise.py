"""Noise_XX WebSocket transport used by the muse.ai web client.

The browser talks to the account's "hatch" workspace through a WebSocket that
carries a Noise_XX_25519_AESGCM_SHA256 handshake. After the handshake each frame
is an encrypted protobuf envelope containing one or more HTTP-like requests and
responses. This module implements just enough of that protocol for the HTTP
driver:

* the Noise handshake (initiator side, matching the web client),
* protobuf framing for request/response/chunk/reset messages,
* unary requests and long-lived NDJSON subscriptions.

It deliberately contains no muse.ai-specific policy; authentication and high
level driver behaviour live in :mod:`muse2api.drivers.http.driver`.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import ssl
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from websockets.exceptions import ConnectionClosed

try:  # zstandard is an optional-runtime dependency of this driver.
    import zstandard
except ImportError:  # pragma: no cover - reported by the driver at startup
    zstandard = None  # type: ignore[assignment]

NOISE_PROTOCOL = b"Noise_XX_25519_AESGCM_SHA256\x00\x00\x00\x00"
GATEWAY_WS_URL = "wss://hatch.metaaivm.com/v1/noise"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_MAX_TRANSPORT_FRAME = 256 * 1024 * 1024
_MAX_DECOMPRESSED_STREAM = 256 * 1024 * 1024


class TransportError(RuntimeError):
    """Base class for transport failures."""


class TransportClosed(TransportError):
    """The WebSocket closed before the request completed."""


class StreamReset(TransportError):
    """The gateway reset one stream."""


class HttpStatusError(TransportError):
    """The gateway returned a non-2xx HTTP response over the tunnel."""

    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self.body = body
        super().__init__(f"gateway returned HTTP {status}: {_short(body)}")


class GatewayError(TransportError):
    """A JSON error returned by the gateway."""

    def __init__(self, code: str, message: str, *, status: int | None = None) -> None:
        self.code = code
        self.message = message
        self.status = status
        super().__init__(f"{code}: {message}")


def _short(data: bytes, limit: int = 300) -> str:
    text = data.decode("utf-8", "replace").strip().replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "..."


# ---------------------------------------------------------------------------
# Noise_XX_25519_AESGCM_SHA256
# ---------------------------------------------------------------------------
def _hkdf(chaining_key: bytes, input_key_material: bytes, count: int) -> bytes:
    temp = hmac.new(chaining_key, input_key_material, hashlib.sha256).digest()
    out = bytearray()
    block = b""
    for i in range(1, count + 1):
        block = hmac.new(temp, block + bytes([i]), hashlib.sha256).digest()
        out.extend(block)
    return bytes(out)


def _sha256(*parts: bytes) -> bytes:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part)
    return digest.digest()


class _CipherState:
    def __init__(self, key: bytes | None = None) -> None:
        self.key = key
        self.n = 0

    def _nonce(self) -> bytes:
        if self.n >= 2**64:
            raise TransportError("Noise nonce exhausted")
        return b"\x00\x00\x00\x00" + self.n.to_bytes(8, "big")

    def encrypt(self, ad: bytes, data: bytes) -> bytes:
        if self.key is None:
            return data
        nonce = self._nonce()
        self.n += 1
        return AESGCM(self.key).encrypt(nonce, data, ad)

    def decrypt(self, ad: bytes, data: bytes) -> bytes:
        if self.key is None:
            return data
        nonce = self._nonce()
        self.n += 1
        return AESGCM(self.key).decrypt(nonce, data, ad)


class NoiseXXInitiator:
    """One Noise XX handshake, initiator side.

    The web client sends a 34-byte payload in message 1 (``0a 20`` + random
    nonce); the value is opaque to the protocol but is preserved for fidelity.
    """

    def __init__(self) -> None:
        self.ck = NOISE_PROTOCOL
        self.h = _sha256(self.ck)
        self.cipher: _CipherState | None = None
        self.eph: X25519PrivateKey | None = None
        self.peer_eph: bytes | None = None

    def _mix_hash(self, data: bytes) -> None:
        self.h = _sha256(self.h, data)

    def _mix_key(self, ikm: bytes) -> None:
        out = _hkdf(self.ck, ikm, 2)
        self.ck, key = out[:32], out[32:]
        self.cipher = _CipherState(key)

    def _encrypt_and_hash(self, payload: bytes) -> bytes:
        out = payload if self.cipher is None else self.cipher.encrypt(self.h, payload)
        self._mix_hash(out)
        return out

    def _decrypt_and_hash(self, data: bytes) -> bytes:
        out = data if self.cipher is None else self.cipher.decrypt(self.h, data)
        self._mix_hash(data)
        return out

    @staticmethod
    def _public(key: X25519PrivateKey) -> bytes:
        return key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )

    def write_message1(self, payload: bytes) -> bytes:
        self.eph = X25519PrivateKey.generate()
        ephemeral = self._public(self.eph)
        self._mix_hash(ephemeral)
        return ephemeral + self._encrypt_and_hash(payload)

    def read_message2(self, message: bytes) -> bytes:
        if len(message) < 96:
            raise TransportError(f"handshake message 2 too short ({len(message)} bytes)")
        if self.eph is None:
            raise TransportError("handshake message 1 was not written")
        peer = message[:32]
        self.peer_eph = peer
        self._mix_hash(peer)
        self._mix_key(self.eph.exchange(X25519PublicKey.from_public_bytes(peer)))
        static = self._decrypt_and_hash(message[32:80])
        self._mix_key(self.eph.exchange(X25519PublicKey.from_public_bytes(static)))
        return self._decrypt_and_hash(message[80:])

    def write_message3(self, payload: bytes = b"") -> bytes:
        if self.peer_eph is None:
            raise TransportError("handshake message 2 was not read")
        static = X25519PrivateKey.generate()
        first = self._encrypt_and_hash(self._public(static))
        self._mix_key(static.exchange(X25519PublicKey.from_public_bytes(self.peer_eph)))
        return first + self._encrypt_and_hash(payload)

    def split(self) -> tuple[_CipherState, _CipherState]:
        out = _hkdf(self.ck, b"", 2)
        return _CipherState(out[:32]), _CipherState(out[32:])


# ---------------------------------------------------------------------------
# Protobuf framing
# ---------------------------------------------------------------------------
def _varint(value: int) -> bytes:
    if value < 0:
        value += 1 << 64
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _f_varint(field: int, value: int) -> bytes:
    return _varint((field << 3) | 0) + _varint(value)


def _f_bytes(field: int, data: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(data)) + data


def _f_str(field: int, value: str) -> bytes:
    return _f_bytes(field, value.encode())


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7
    raise TransportError("truncated protobuf varint")


def _parse_fields(data: bytes) -> list[tuple[int, int, Any]]:
    out: list[tuple[int, int, Any]] = []
    offset = 0
    while offset < len(data):
        key, offset = _read_varint(data, offset)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, offset = _read_varint(data, offset)
        elif wire == 1:
            value, offset = data[offset : offset + 8], offset + 8
        elif wire == 2:
            size, offset = _read_varint(data, offset)
            value, offset = data[offset : offset + size], offset + size
        elif wire == 5:
            value, offset = data[offset : offset + 4], offset + 4
        else:
            raise TransportError(f"unsupported protobuf wire type {wire}")
        if offset > len(data):
            raise TransportError("truncated protobuf field")
        out.append((field, wire, value))
    return out


def _get_field(fields: list[tuple[int, int, Any]], number: int) -> Any:
    for field, _, value in fields:
        if field == number:
            return value
    return None


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value) if value is not None else ""


def _encode_header(key: str, value: str) -> bytes:
    return _f_str(1, key) + _f_str(2, value)


def _encode_http_request(
    method: str,
    path: str,
    body: bytes = b"",
    headers: list[tuple[str, str]] | None = None,
) -> bytes:
    out = _f_str(1, method) + _f_str(2, path)
    for key, value in headers or ():
        out += _f_bytes(3, _encode_header(key, value))
    out += _f_bytes(4, body)
    out += _f_varint(5, 1)
    return out


def _encode_service_request(stream_id: int, service: int, http_request: bytes) -> bytes:
    frame = _f_varint(1, stream_id) + _f_bytes(2, http_request)
    return _f_varint(1, service) + _f_bytes(2, frame)


def _encode_transport_frame(payload: bytes) -> bytes:
    # ``flags=1`` marks a request frame, as observed from the web client.
    return (
        _f_varint(1, secrets.randbits(63))
        + _f_varint(2, 0)
        + _f_varint(3, 1)
        + _f_bytes(4, payload)
    )


def _parse_service_frame(payload: bytes) -> dict[str, Any] | None:
    response = _parse_fields(payload)
    service_frame = _get_field(response, 1)
    if not isinstance(service_frame, (bytes, bytearray)):
        return None
    fields = _parse_fields(bytes(service_frame))
    stream_id = int(_get_field(fields, 1) or 0)
    for field, _, value in fields:
        if field == 3 and isinstance(value, (bytes, bytearray)):
            response_fields = _parse_fields(bytes(value))
            headers: list[tuple[str, str]] = []
            for hf, _, hv in response_fields:
                if hf == 2 and isinstance(hv, (bytes, bytearray)):
                    parts = _parse_fields(bytes(hv))
                    headers.append((_text(_get_field(parts, 1)), _text(_get_field(parts, 2))))
            return {
                "kind": "response",
                "stream_id": stream_id,
                "status": int(_get_field(response_fields, 1) or 0),
                "headers": headers,
                "body": bytes(_get_field(response_fields, 3) or b""),
                "end": bool(_get_field(response_fields, 4)),
            }
        if field == 4 and isinstance(value, (bytes, bytearray)):
            chunk = _parse_fields(bytes(value))
            return {
                "kind": "chunk",
                "stream_id": stream_id,
                "data": bytes(_get_field(chunk, 1) or b""),
                "end": bool(_get_field(chunk, 2)),
            }
        if field == 5 and isinstance(value, (bytes, bytearray)):
            reset = _parse_fields(bytes(value))
            return {
                "kind": "reset",
                "stream_id": stream_id,
                "code": _get_field(reset, 1),
                "reason": _text(_get_field(reset, 2)),
            }
    return None


def _decompress(data: bytes, *, limit: int = 256 * 1024 * 1024) -> bytes:
    if not data.startswith(ZSTD_MAGIC):
        return data
    if zstandard is None:
        raise TransportError("received zstd data but the 'zstandard' package is not installed")
    return zstandard.ZstdDecompressor().decompress(data, max_output_size=limit)


# ---------------------------------------------------------------------------
# Connection + streams
# ---------------------------------------------------------------------------
class NoiseClient:
    """An established Noise tunnel with HTTP/stream primitives."""

    def __init__(
        self,
        websocket: Any,
        send_cipher: _CipherState,
        recv_cipher: _CipherState,
    ) -> None:
        self.ws = websocket
        self._send_cipher = send_cipher
        self._recv_cipher = recv_cipher
        self._streams: dict[int, asyncio.Queue[dict[str, Any]]] = {}
        self._assemblies: dict[int, dict[str, Any]] = {}
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self.closed = False
        self.close_reason = ""

    async def start(self) -> None:
        self._reader = asyncio.create_task(self._read_loop(), name="muse-noise-reader")

    async def _read_loop(self) -> None:
        try:
            while True:
                raw = await self.ws.recv()
                if isinstance(raw, str):
                    continue
                try:
                    plain = self._recv_cipher.decrypt(b"", raw)
                except Exception as exc:  # noqa: BLE001 - cryptographic failure is terminal
                    self.close_reason = f"noise decrypt failed: {type(exc).__name__}"
                    break
                try:
                    fields = _parse_fields(plain)
                except TransportError as exc:
                    self.close_reason = str(exc)
                    break
                payload = _get_field(fields, 4)
                if not isinstance(payload, (bytes, bytearray)):
                    continue
                chunk_id = int(_get_field(fields, 1) or 0)
                index = int(_get_field(fields, 2) or 0)
                total = int(_get_field(fields, 3) or 1)
                if total <= 1:
                    joined = bytes(payload)
                else:
                    state = self._assemblies.setdefault(
                        chunk_id, {"total": total, "parts": {}, "bytes": 0}
                    )
                    if index in state["parts"]:
                        continue
                    state["parts"][index] = bytes(payload)
                    state["bytes"] += len(payload)
                    if state["bytes"] > _MAX_TRANSPORT_FRAME:
                        self.close_reason = "transport frame exceeds the size limit"
                        break
                    if len(state["parts"]) < state["total"]:
                        continue
                    self._assemblies.pop(chunk_id, None)
                    joined = b"".join(state["parts"][i] for i in range(state["total"]))
                item = _parse_service_frame(joined)
                if item is None:
                    continue
                queue = self._streams.get(int(item["stream_id"]))
                if queue is not None:
                    queue.put_nowait(item)
        except ConnectionClosed as exc:
            self.close_reason = f"websocket closed ({exc.code})"
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep the reason for callers
            self.close_reason = f"{type(exc).__name__}: {exc}"
        finally:
            self.closed = True
            for queue in list(self._streams.values()):
                queue.put_nowait({"kind": "closed"})

    async def _send(self, stream_id: int, service: int, request: bytes) -> None:
        if self.closed:
            raise TransportClosed(self.close_reason or "tunnel is closed")
        service_request = _encode_service_request(stream_id, service, request)
        frame = _encode_transport_frame(service_request)
        # The Noise send nonce and the WebSocket frame order must match when
        # concurrent requests share one tunnel.
        async with self._send_lock:
            try:
                await self.ws.send(self._send_cipher.encrypt(b"", frame))
            except ConnectionClosed as exc:
                self.closed = True
                self.close_reason = f"websocket closed while sending ({exc.code})"
                raise TransportClosed(self.close_reason) from exc
            except OSError as exc:
                self.closed = True
                self.close_reason = f"websocket send failed: {exc}"
                raise TransportClosed(self.close_reason) from exc

    def _register(self, stream_id: int) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._streams[stream_id] = queue
        return queue

    def _unregister(self, stream_id: int) -> None:
        self._streams.pop(stream_id, None)

    async def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        headers: list[tuple[str, str]] | None = None,
        *,
        timeout: float = 30.0,
        service: int = 0,
        max_output: int = 256 * 1024 * 1024,
    ) -> tuple[int, dict[str, str], bytes]:
        stream_id = secrets.randbits(63)
        queue = self._register(stream_id)
        deadline = time.monotonic() + timeout
        chunks = bytearray()
        status = 0
        response_headers: dict[str, str] = {}
        try:
            await self._send(stream_id, service, _encode_http_request(method, path, body, headers))
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportClosed("request timed out")
                try:
                    item = await asyncio.wait_for(queue.get(), remaining)
                except asyncio.TimeoutError as exc:
                    raise TransportClosed("request timed out") from exc
                kind = item["kind"]
                if kind == "response":
                    status = int(item["status"] or 0)
                    response_headers = {k.lower(): v for k, v in item["headers"]}
                    chunks.extend(item["body"] or b"")
                    if item["end"]:
                        break
                elif kind == "chunk":
                    chunks.extend(item["data"] or b"")
                    if item["end"]:
                        break
                elif kind == "reset":
                    raise StreamReset(
                        f"gateway reset stream: {item.get('reason') or item.get('code')}"
                    )
                else:
                    raise TransportClosed(self.close_reason or "tunnel closed")
        finally:
            self._unregister(stream_id)
        data = bytes(chunks)
        encoding = response_headers.get("content-encoding", "").lower()
        if encoding == "zstd" or data.startswith(ZSTD_MAGIC):
            data = _decompress(data, limit=max_output)
        return status, response_headers, data

    async def subscribe(
        self,
        path: str,
        body: bytes,
        headers: list[tuple[str, str]] | None = None,
        *,
        timeout: float = 30.0,
    ) -> Subscription:
        stream_id = secrets.randbits(63)
        queue = self._register(stream_id)
        try:
            await self._send(stream_id, 0, _encode_http_request("POST", path, body, headers))
        except Exception:
            self._unregister(stream_id)
            raise
        return Subscription(self, stream_id, queue, timeout=timeout)

    async def close(self) -> None:
        self.closed = True
        for queue in list(self._streams.values()):
            queue.put_nowait({"kind": "closed"})
        if self._reader is not None and not self._reader.done():
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
        try:
            await self.ws.close()
        except Exception:  # noqa: BLE001
            pass


class Subscription:
    """A streaming HTTP response carried over a Noise stream."""

    def __init__(
        self,
        client: NoiseClient,
        stream_id: int,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        timeout: float,
    ) -> None:
        self.client = client
        self.stream_id = stream_id
        self._queue = queue
        self._timeout = timeout
        self._buffer = b""
        self._decompressor: Any = None
        self._headers: dict[str, str] = {}
        self._status: int | None = None
        self._done = False
        self._error: bytes | None = None
        self._decompressed_bytes = 0

    @property
    def status(self) -> int | None:
        return self._status

    @property
    def headers(self) -> dict[str, str]:
        return dict(self._headers)

    @property
    def done(self) -> bool:
        return self._done

    async def _next_payload(self, timeout: float | None = None) -> bytes | None:
        if self._done:
            return None
        wait = self._timeout if timeout is None else timeout
        try:
            item = await asyncio.wait_for(self._queue.get(), wait)
        except asyncio.TimeoutError:
            raise
        kind = item["kind"]
        if kind == "response":
            self._status = int(item["status"] or 0)
            self._headers = {k.lower(): v for k, v in item["headers"]}
            data = item["body"] or b""
            if item["end"]:
                self._done = True
            encoding = self._headers.get("content-encoding", "").lower()
            if encoding == "zstd" or data.startswith(ZSTD_MAGIC):
                if zstandard is None:
                    raise TransportError("received zstd data but 'zstandard' is missing")
                self._decompressor = zstandard.ZstdDecompressor().decompressobj()
            if self._status >= 400:
                error_data = data
                if self._decompressor is not None:
                    try:
                        error_data = self._decompress_piece(data, final=True)
                    except TransportError:
                        pass
                self._error = error_data
                self._done = True
                raise HttpStatusError(self._status, error_data)
            return self._decompress_piece(data, final=self._done)
        if kind == "chunk":
            data = item["data"] or b""
            if item["end"]:
                self._done = True
            if self._decompressor is None and data.startswith(ZSTD_MAGIC):
                if zstandard is None:
                    raise TransportError("received zstd data but 'zstandard' is missing")
                self._decompressor = zstandard.ZstdDecompressor().decompressobj()
            return self._decompress_piece(data, final=self._done)
        if kind == "reset":
            self._done = True
            raise StreamReset(
                f"gateway reset subscription: {item.get('reason') or item.get('code')}"
            )
        self._done = True
        raise TransportClosed(self.client.close_reason or "subscription closed")

    def _decompress_piece(self, data: bytes, *, final: bool) -> bytes:
        if self._decompressor is None:
            return data
        try:
            out = self._decompressor.decompress(data) if data else b""
        except Exception as exc:  # noqa: BLE001
            raise TransportError(f"invalid zstd stream: {exc}") from exc
        if final:
            try:
                out += self._decompressor.flush()
            except Exception:  # noqa: BLE001
                pass
        self._decompressed_bytes += len(out)
        if self._decompressed_bytes > _MAX_DECOMPRESSED_STREAM:
            raise TransportError("decompressed subscription exceeds the size limit")
        return out

    async def read_line(self, timeout: float | None = None) -> str:
        """Return one non-empty NDJSON line.

        A timeout does not consume or close the stream: callers can poll for
        cancellation and continue. Raises :class:`EOFError` at end of stream.
        """
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                line, self._buffer = self._buffer[:newline], self._buffer[newline + 1 :]
                line = line.strip()
                if line:
                    return line.decode("utf-8", "replace")
                continue
            if self._done:
                tail = self._buffer.strip()
                self._buffer = b""
                if tail:
                    return tail.decode("utf-8", "replace")
                raise EOFError
            data = await self._next_payload(timeout)
            if data:
                self._buffer += data

    async def lines(self, timeout: float | None = None) -> AsyncIterator[str]:
        while True:
            try:
                yield await self.read_line(timeout)
            except EOFError:
                return

    async def close(self) -> None:
        self._done = True
        self.client._unregister(self.stream_id)
        # Wake a reader blocked in ``_next_payload`` so it can observe ``_done``.
        self._queue.put_nowait({"kind": "closed"})


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GatewayToken:
    token: str
    notary_token: str
    vm_id: str
    vm_name: str


def _connect_kwargs(origin: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "origin": origin,
        "max_size": None,
        "ping_interval": 20,
        "ping_timeout": 20,
        "open_timeout": 20,
        "close_timeout": 5,
        "compression": None,
        "ssl": ssl.create_default_context(),
    }
    return kwargs


async def connect_gateway(
    credentials: GatewayToken,
    *,
    origin: str = "https://muse.ai",
    user_agent: str,
    request_id: str | None = None,
    timeout: float = 20.0,
) -> NoiseClient:
    """Open the WebSocket, run the Noise handshake, and start the reader."""
    from urllib.parse import quote, urlencode

    params = urlencode(
        [
            ("vm_id", credentials.vm_id),
            ("auth_token", credentials.token),
            ("notary_token", credentials.notary_token),
            ("app_id", "hatch-web"),
            ("request_id", request_id or str(uuid.uuid4())),
        ],
        quote_via=quote,
    )
    url = f"{GATEWAY_WS_URL}?{params}"
    handshake = NoiseXXInitiator()
    message1 = handshake.write_message1(b"\x0a\x20" + secrets.token_bytes(32))
    kwargs = _connect_kwargs(origin)
    kwargs["user_agent_header"] = user_agent
    try:
        ws = await asyncio.wait_for(websockets.connect(url, **kwargs), timeout)
    except TypeError:
        # websockets < 14 does not expose ``user_agent_header``; the URL token is
        # what authenticates, so retry without it.
        kwargs.pop("user_agent_header", None)
        ws = await asyncio.wait_for(websockets.connect(url, **kwargs), timeout)
    try:
        await ws.send(message1)
        message2 = await asyncio.wait_for(ws.recv(), timeout)
        if isinstance(message2, str):
            raise TransportError("gateway answered the Noise handshake with text")
        handshake.read_message2(message2)
        await ws.send(handshake.write_message3(b""))
        send_cipher, recv_cipher = handshake.split()
        client = NoiseClient(ws, send_cipher, recv_cipher)
        await client.start()
        return client
    except Exception:
        await ws.close()
        raise
