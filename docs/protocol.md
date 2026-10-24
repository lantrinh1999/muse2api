# Direct HTTP / Noise protocol

This document describes the transport used by `drivers/http/`, the driver that
talks to muse.ai without Chromium. The protocol is private and undocumented; it
is reproduced from the web client and can change without notice. Keep the
protocol constants and parsers in the small modules listed below so a site
change is easy to isolate.

| Module | Responsibility |
|---|---|
| `drivers/http/auth.py` | `/thread/new` discovery and hatch-token bootstrap |
| `drivers/http/noise.py` | Noise_XX handshake, protobuf framing, tunneled HTTP and NDJSON streams |
| `drivers/http/events.py` | event, transcript and media parsing |
| `drivers/http/driver.py` | chat, uploads, image/video generation, media download and lifecycle |

## 1. Bootstrap

The front door is protected by browser/TLS fingerprinting, so the two bootstrap
calls use `curl_cffi` with `impersonate="chrome"`. The normal account cookies
(`hatch_sess`, `hatch_vml`, `hatch_native_auth_device`, plus `hatch_gw` in older
exports, ...) are sent as a cookie jar.

### 1.1 Discover the workspace

```http
GET https://muse.ai/thread/new
Accept: text/html
```

The HTML contains an inline Next.js payload with the active workspace:

```json
{"gatewayUrl":"wss://<vm-id>.metaaivm.com/","vmName":"...","vmId":"..."}
```

The page payload may escape quotes inside the inline JSON. `auth.discover_target`
normalises those escapes, takes `vm_id` from the gateway host (the authoritative
value), and reads `vmName` next to the gateway URL. A missing assignment means
the cookies are expired or the account has no workspace.

### 1.2 Mint a gateway token

```http
POST https://muse.ai/api/hatch/token
Content-Type: application/json

{"vmAddress":"wss://<vm-id>.metaaivm.com/","vmName":"<vm-name>"}
```

The response contains `token` and `notary_token`. The driver caches both for
five minutes per account and invalidates the cache when the cookie signature
changes or the WebSocket handshake fails. A transient token failure triggers a
best-effort VM wake (`POST` to the wake endpoint) and one retry.

## 2. Noise transport

### 2.1 WebSocket handshake

```text
wss://hatch.metaaivm.com/v1/noise
  ?vm_id=<vm-id>
  &auth_token=<token>
  &notary_token=<notary-token>
  &app_id=hatch-web
  &request_id=<uuid>
```

After the WebSocket opens, the initiator performs
`Noise_XX_25519_AESGCM_SHA256`:

1. message 1: ephemeral X25519 public key plus a 34-byte payload
   (`0a 20` followed by 32 random bytes);
2. message 2: responder ephemeral, encrypted static and encrypted payload;
3. message 3: initiator encrypted static and an empty encrypted payload.

The handshake is implemented in `noise.NoiseXXInitiator`; `split()` produces the
send and receive cipher states. Each transport frame is AES-GCM encrypted with
the Noise nonce format used by the client.

### 2.2 Protobuf envelope

An encrypted frame is a protobuf message:

| Field | Meaning |
|---|---|
| 1 | chunk/request id |
| 2 | chunk index |
| 3 | chunk count |
| 4 | service payload |

The service payload contains a service number (field 1) and a service frame
(field 2). A service frame contains a stream id (field 1) and one of:

- HTTP response (field 3): status, headers, body, `end`;
- HTTP chunk (field 4): bytes, `end`;
- reset (field 5): code and reason.

The driver's `NoiseClient` registers a queue per stream id, sends a request
frame, and assembles response/chunk frames until `end`. Large frames are
reassembled by chunk index. `Subscription` exposes the same stream as NDJSON
lines. Unary responses and subscriptions may be zstd-compressed; the driver
decompresses both (including streams whose compression starts on a later chunk).

## 3. Chat threads

### 3.1 Create a thread

The web client treats a fresh UUID supplied as `session_id` as a new thread.
The driver deliberately creates one isolated thread per API request and sends:

```http
POST /chat/stream
Content-Type: application/json

{
  "node_id": "<uuid>",
  "capabilities": ["hotel_results", "chat_cancel", "delta_stream",
                   "custom_reactions", "custom_reactions_facebook_thumbs_up_v1"],
  "session_id": "<fresh-uuid>",
  "timezone": "UTC",
  "metadata": {"thread_is_dictation_used": false},
  "message": "<prompt>"
}
```

When images are attached, the body uses `items` instead of `message`:

```json
{
  "items": [
    {"type": "text", "text": "<prompt>"},
    {"type": "file_ref", "mime_type": "image/webp",
     "path": "workspace/user/files/<name>", "filename": "<name>"}
  ]
}
```

The acknowledgement normally returns the same `session_id`; if it falls back
to another session the driver logs a warning and follows the returned id.

### 3.2 Subscribe

Immediately after the `/chat/stream` acknowledgement the driver opens:

```http
POST /chat/subscribe
Content-Type: application/json

{
  "after_stream_seq": 0,
  "after_chat_event_seq": 0,
  "capabilities": [ ...chat capabilities..., "voice_reply_captions",
                    "voice_user_live_transcript" ],
  "session_id": "<session-id>"
}
```

A subscription is a long-lived NDJSON response. The first subscription for a
fresh thread catches text deltas. A late subscription may replay final messages
but not text deltas, so `chat.turn` reconciles the streamed text with the final
transcript:

- if no delta arrived, emit `display_text` / final transcript text;
- if the final text extends the delta text, emit only the suffix;
- if the final text is a shorter replay, keep the deltas.

`done_ids` filters replayed old `delta.message_done` events after a follow-up
subscription; presentation cards are filtered by their `message_id`/`id`.

### 3.3 Events

| Event | Use |
|---|---|
| `delta.text_append` | transient assistant text delta |
| `delta.message_done` | final status, transcript and resources |
| `delta.presentation` | image/video card (`data.images` / `data.videos`, `image_path`) |
| `message.assistant` | full assistant message (may carry resources) |
| `error` | gateway error; mapped by code/message |

Events with `stream_lane` or `transcript_surface` equal to `subagent` are
ignored for the main reply. A done status other than `completed`, `complete` or
`success` is an error.

### 3.4 Generated media

On the web app, an image or video attachment arrives as a separate
`delta.presentation` card, not necessarily inside the done event's `resources`.
A card looks like:

```json
{
  "kind": "image",
  "image_path": "workspace/imagine_media/<name>.webp",
  "fallback_text": "![label](sandbox://workspace/imagine_media/<name>.webp)",
  "data": {
    "images": [{
      "path": "sandbox://workspace/imagine_media/<name>.webp",
      "mime": "image/webp",
      "byte_len": 14586,
      "missing": false,
      "variants": {}
    }]
  }
}
```

`extract_media_from_presentation` accepts `images`, `videos`, `files`, `media`
and `attachments`, as well as `image_path`, `video_path`, markdown fallback and
top-level `presentations` in a done event. Paths are normalised to
`sandbox://workspace/...`.

`variants.original` is intentionally ignored. It can point at the VM's private
gateway host (`https://<vm-id>.metaaivm.com/...`), which is not resolvable from
the API process. The sandbox path is the stable source.

The media flow is:

1. `POST /chat/stream` with an image/video prompt;
2. collect `delta.presentation` cards, plus late cards for 8 s (image) or 90 s (video) after a text-only done frame;
3. filter to the requested kind and exclude uploaded reference files;
4. `GET /fs/raw/<url-encoded relative path>` over the same Noise tunnel;
5. return bytes, MIME and optional dimensions to the service layer.

## 4. Workspace files

All filesystem paths are relative to the workspace root. Absolute paths are
rejected by the gateway. The driver uses:

| Operation | Request |
|---|---|
| write/upload | `POST /fs/write` with `path`, `data_base64`, `overwrite`, `append`, `create_parent` |
| download | `GET /fs/raw/<encoded path>` |
| delete | `POST /fs/delete` with `path`, `recursive: false` |

Uploads are chunked at 512 KiB. The first chunk uses `overwrite: true` and
`create_parent: true`; later chunks use `append: true`. Upload paths are recorded
before writing so a partial upload is deleted in the driver's `finally` block.

## 5. Errors and lifecycle

- Unary HTTP status 401/403 → `UpstreamAuthError`;
- 402/429 → `UpstreamQuotaError`;
- 408 → `UpstreamTimeout`;
- gateway JSON `error`/`code` → `_map_gateway_error`;
- reset/closed tunnel → `UpstreamError` (the account pool can retry on another
  account before the first client chunk);
- a done status or `error` event is surfaced as the corresponding upstream
  error.

The token grant is invalidated when a connection fails, and again by
`renew_session`. Each chat or media call closes its subscription and Noise
socket and deletes its uploads in `finally`.

## 6. Known limits

- The protocol is private and may change; test fixtures in
  `tests/test_http_driver.py` cover the offline parsers and framing.
- Image chat input, image generation/download and text chat have been verified
  live. Video generation uses the same presentation/media path, but the test
  account was blocked by an upstream region restriction ("Video generation isn't
  available for your account in your country or region"), so the video path is
  covered offline only.
- HLS/m3u8 playlists are not remuxed; the driver expects a directly readable
  media file such as MP4/WebM.
- Quota lookup is not wired to the HTTP driver yet.
- Session renewal still uses `upstream/muse.py`; it is independent of the Noise
  tunnel.
