from __future__ import annotations

import asyncio

import pytest
import zstandard

from muse2api.config import Settings
from muse2api.drivers.base import InputImage
from muse2api.drivers.http import auth, noise
from muse2api.drivers.http.driver import HttpDriver, _map_gateway_error
from muse2api.drivers.http.events import (
    MediaRef,
    extract_error,
    extract_final_text,
    extract_media,
    extract_media_from_presentation,
)
from muse2api.errors import UpstreamAuthError, UpstreamError, UpstreamQuotaError, UpstreamRefused


def test_noise_protocol_name_is_padded_to_32_bytes():
    assert len(noise.NOISE_PROTOCOL) == 32


def test_discover_target_from_escaped_next_payload():
    html = (
        'self.__next_f.push([1,"6e:{\\\\"gatewayUrl\\\\":\\\\"wss://vm-123.metaaivm.com/\\\\",'
        '\\\\"vmName\\\\":\\\\"workspace-a\\\\",\\\\"vmId\\\\":\\\\"vm-123\\\\"}"])'
    )
    target = auth.discover_target(html)
    assert target.vm_id == "vm-123"
    assert target.vm_name == "workspace-a"
    assert target.gateway_url == "wss://vm-123.metaaivm.com"


def test_discover_target_uses_gateway_host_not_a_stray_vm_id():
    html = (
        '{"vmId":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",'
        '"gatewayUrl":"wss://vm-123.metaaivm.com/","vmName":"workspace-a"}'
        '{"gatewayUrl":"wss://vm-999.metaaivm.com/","vmName":"decoy",'
        '"vmId":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}'
    )
    target = auth.discover_target(html)
    assert target.vm_id == "vm-123"
    assert target.vm_name == "workspace-a"


def test_discover_target_rejects_a_logged_out_page():
    with pytest.raises(UpstreamAuthError):
        auth.discover_target("<html><body>Sign in</body></html>")


def _response_envelope(
    stream_id: int,
    status: int,
    body: bytes,
    *,
    end: bool = True,
    headers: list[tuple[str, str]] | None = None,
) -> bytes:
    header_fields = b"".join(
        noise._f_bytes(2, noise._encode_header(k, v)) for k, v in (headers or [])
    )
    response = (
        noise._f_varint(1, status)
        + header_fields
        + noise._f_bytes(3, body)
        + noise._f_varint(4, 1 if end else 0)
    )
    service = noise._f_varint(1, stream_id) + noise._f_bytes(3, response)
    return noise._f_bytes(1, service)


def test_parse_http_response_frame():
    envelope = _response_envelope(
        42, 200, b'{"ok":true}', headers=[("content-type", "application/json")]
    )
    item = noise._parse_service_frame(envelope)
    assert item == {
        "kind": "response",
        "stream_id": 42,
        "status": 200,
        "headers": [("content-type", "application/json")],
        "body": b'{"ok":true}',
        "end": True,
    }


def test_parse_chunk_frame():
    chunk = noise._f_bytes(1, b"hello") + noise._f_varint(2, 1)
    service = noise._f_varint(1, 7) + noise._f_bytes(4, chunk)
    item = noise._parse_service_frame(noise._f_bytes(1, service))
    assert item == {
        "kind": "chunk",
        "stream_id": 7,
        "data": b"hello",
        "end": True,
    }


class _DummyClient:
    close_reason = ""

    def __init__(self) -> None:
        self.unregistered: list[int] = []

    def _unregister(self, stream_id: int) -> None:
        self.unregistered.append(stream_id)


class _FakeWebSocket:
    def __init__(self, frames: list[bytes]) -> None:
        self.frames = list(frames)
        self.closed = False

    async def recv(self) -> bytes:
        if self.frames:
            return self.frames.pop(0)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


async def test_noise_reassembles_chunked_transport_frames():
    envelope = _response_envelope(42, 200, b"hello")
    cut = len(envelope) // 2
    ws = _FakeWebSocket(
        [
            noise._f_varint(1, 99)
            + noise._f_varint(2, 0)
            + noise._f_varint(3, 2)
            + noise._f_bytes(4, envelope[:cut]),
            noise._f_varint(1, 99)
            + noise._f_varint(2, 1)
            + noise._f_varint(3, 2)
            + noise._f_bytes(4, envelope[cut:]),
        ]
    )
    client = noise.NoiseClient(ws, noise._CipherState(), noise._CipherState())
    queue = client._register(42)
    await client.start()
    try:
        item = await asyncio.wait_for(queue.get(), 1)
    finally:
        await client.close()
    assert item["kind"] == "response"
    assert item["stream_id"] == 42
    assert item["body"] == b"hello"
    assert ws.closed


async def test_subscription_close_wakes_a_blocked_reader():
    queue: asyncio.Queue = asyncio.Queue()
    sub = noise.Subscription(_DummyClient(), 1, queue, timeout=30)  # type: ignore[arg-type]
    task = asyncio.create_task(sub.read_line())
    await asyncio.sleep(0)
    await sub.close()
    with pytest.raises(noise.TransportClosed):
        await asyncio.wait_for(task, 1)


async def test_subscription_timeout_does_not_close_the_stream():
    queue: asyncio.Queue = asyncio.Queue()
    sub = noise.Subscription(_DummyClient(), 1, queue, timeout=30)  # type: ignore[arg-type]
    with pytest.raises(asyncio.TimeoutError):
        await sub.read_line(timeout=0.01)
    await queue.put({"kind": "chunk", "stream_id": 1, "data": b"line\n", "end": True})
    assert await sub.read_line(timeout=1) == "line"
    assert sub.done
    await sub.close()


async def test_subscription_decompresses_ndjson_and_keeps_partial_lines():
    events = b'{"event":"one"}\n{"event":"two"}\n{"event":"partial"}'
    compressed = zstandard.ZstdCompressor().compress(events)
    queue: object = __import__("asyncio").Queue()
    await queue.put(
        {
            "kind": "response",
            "stream_id": 1,
            "status": 200,
            "headers": [("content-type", "application/x-ndjson"), ("content-encoding", "zstd")],
            "body": compressed,
            "end": True,
        }
    )
    client = _DummyClient()
    sub = noise.Subscription(client, 1, queue, timeout=1)  # type: ignore[arg-type]
    lines = [line async for line in sub.lines()]
    assert lines == ['{"event":"one"}', '{"event":"two"}', '{"event":"partial"}']
    await sub.close()
    assert client.unregistered == [1]


def test_extract_media_from_resources_transcript_and_markdown():
    payload = {
        "resources": [
            {
                "kind": "image",
                "uri": "sandbox://workspace/user/files/generated.png",
                "mime_type": "image/png",
            },
            {
                "kind": "video",
                "uri": "https://cdn.example/video.mp4",
                "mime_type": "video/mp4",
            },
        ],
        "transcript": {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": "Here: ![preview](sandbox://workspace/user/files/other.jpg)",
                        },
                        {
                            "type": "tool_call",
                            "result": {
                                "metadata": {
                                    "file_paths": ["/home/hatch/workspace/user/files/third.webp"],
                                    "file_type": "image",
                                },
                            },
                        },
                    ],
                }
            ],
        },
    }
    media = extract_media(payload)
    assert [m.uri for m in media] == [
        "sandbox://workspace/user/files/generated.png",
        "https://cdn.example/video.mp4",
        "sandbox://workspace/user/files/other.jpg",
        "sandbox://workspace/user/files/third.webp",
    ]
    assert [m.kind for m in media] == ["image", "video", "image", "image"]


def test_extract_media_from_presentation_card():
    payload = {
        "kind": "image",
        "image_path": "workspace/imagine_media/generated.webp",
        "fallback_text": "![preview](sandbox://workspace/imagine_media/generated.webp)",
        "data": {
            "images": [
                {
                    "path": "sandbox://workspace/imagine_media/generated.webp",
                    "mime": "image/webp",
                    "byte_len": 14586,
                    "missing": False,
                }
            ]
        },
    }
    media = extract_media_from_presentation(payload)
    assert len(media) == 1
    assert media[0].uri == "sandbox://workspace/imagine_media/generated.webp"
    assert media[0].mime == "image/webp"
    assert media[0].kind == "image"


def test_extract_media_from_video_presentation_card():
    payload = {
        "kind": "video",
        "data": {
            "videos": [
                {
                    "path": "sandbox://workspace/imagine_media/clip.mp4",
                    "mime": "video/mp4",
                    "width": 1280,
                    "height": 720,
                    "missing": False,
                }
            ]
        },
    }
    media = extract_media_from_presentation(payload)
    assert len(media) == 1
    assert media[0].kind == "video"
    assert media[0].mime == "video/mp4"
    assert media[0].width == 1280
    assert media[0].height == 720


def test_extract_media_skips_missing_presentation_files():
    payload = {
        "kind": "image",
        "data": {
            "images": [
                {
                    "path": "sandbox://workspace/imagine_media/x.png",
                    "mime": "image/png",
                    "missing": True,
                }
            ]
        },
    }
    assert extract_media_from_presentation(payload) == []


def test_extract_media_from_presentation_relative_path():
    payload = {
        "kind": "image",
        "data": {"images": [{"path": "workspace/user/files/photo.png"}]},
    }
    media = extract_media_from_presentation(payload)
    assert [m.uri for m in media] == ["sandbox://workspace/user/files/photo.png"]
    assert media[0].mime == "image/png"


def test_extract_media_walks_done_presentations():
    presentation = {
        "kind": "image",
        "data": {
            "images": [{"path": "sandbox://workspace/imagine_media/x.png", "mime": "image/png"}]
        },
    }
    media = extract_media({"presentations": [presentation]})
    assert [m.uri for m in media] == ["sandbox://workspace/imagine_media/x.png"]


def test_extract_error_accepts_top_level_shape():
    assert extract_error({"code": "rate_limit", "message": "slow down"}) == (
        "rate_limit",
        "slow down",
    )
    assert extract_error({"error": {"code": "bad", "message": "nope"}}) == ("bad", "nope")


def test_generated_media_excludes_uploaded_reference_files(tmp_path):
    driver = HttpDriver(Settings(_env_file=None, data_dir=tmp_path))
    media = [
        MediaRef("sandbox://workspace/onboarding_tour/crons.png", "image/png", "image"),
        MediaRef("sandbox://workspace/user/files/reference.png", "image/png", "image"),
        MediaRef("sandbox://workspace/imagine_media/result.webp", "image/webp", "image"),
    ]
    selected = driver._generated_media(media, "image", ["workspace/user/files/reference.png"])
    assert [m.uri for m in selected] == ["sandbox://workspace/imagine_media/result.webp"]


def test_validate_media_rejects_html_and_hls():
    with pytest.raises(UpstreamError, match="HTML"):
        HttpDriver._validate_media(b"<html>nope</html>", "text/html", "image")
    with pytest.raises(UpstreamError, match="HLS"):
        HttpDriver._validate_media(b"#EXTM3U\n...", "application/vnd.apple.mpegurl", "video")


async def test_upload_records_path_before_writing(tmp_path, monkeypatch):
    driver = HttpDriver(Settings(_env_file=None, data_dir=tmp_path))
    uploaded: list[str] = []

    async def fail(*args, **kwargs):
        raise UpstreamError("write failed")

    monkeypatch.setattr(driver, "_request_json", fail)
    with pytest.raises(UpstreamError, match="write failed"):
        await driver._upload_images(None, [InputImage(b"x", "image/png")], uploaded)
    assert len(uploaded) == 1
    assert uploaded[0].startswith("workspace/user/files/")
    assert uploaded[0].endswith(".png")


def test_extract_final_text_uses_last_assistant_message():
    payload = {
        "transcript": {
            "messages": [
                {"role": "assistant", "content": [{"type": "text", "text": "first"}]},
                {"role": "user", "content": [{"type": "text", "text": "again"}]},
                {"role": "assistant", "content": [{"type": "text", "text": "second"}]},
            ]
        }
    }
    assert extract_final_text(payload) == "second"


def test_fs_path_is_workspace_relative():
    assert (
        HttpDriver._fs_path("sandbox://workspace/user/files/a.png") == "workspace/user/files/a.png"
    )
    assert (
        HttpDriver._fs_path("/home/hatch/workspace/user/files/a.png")
        == "workspace/user/files/a.png"
    )
    assert HttpDriver._fs_path("/workspace/user/files/a.png") == "workspace/user/files/a.png"


def test_error_mapping():
    assert isinstance(HttpDriver._map_status(401, b"no"), UpstreamAuthError)
    assert isinstance(HttpDriver._map_status(429, b""), UpstreamQuotaError)
    assert isinstance(_map_gateway_error("insufficient_credits", "no credits"), UpstreamQuotaError)
    assert isinstance(_map_gateway_error("invalid_params", "bad"), UpstreamRefused)


def test_upload_path_has_safe_extension_and_filename():
    from muse2api.drivers.base import InputImage

    path, filename = HttpDriver._upload_path(InputImage(data=b"x", mime="image/jpeg"))
    assert path.startswith("workspace/user/files/muse2api_")
    assert path.endswith(".jpg")
    assert filename.endswith(".jpg")
    assert "/" not in filename


def test_media_ref_is_remote():
    assert MediaRef("https://example/a.png", "image/png", "image").is_remote
    assert not MediaRef("sandbox://workspace/a.png", "image/png", "image").is_remote
