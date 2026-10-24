"""Plain-HTTP bootstrap for the Noise gateway.

Two browser requests are needed before the WebSocket can be opened:

1. ``GET /thread/new`` to discover the account's VM id/name and gateway URL.
2. ``POST /api/hatch/token`` to mint the short-lived gateway token.

Both are part of the normal web app flow. ``curl_cffi`` is used because the
front door is protected by TLS/browser fingerprinting; after this bootstrap the
session runs over the Noise tunnel and needs no browser.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

from ...errors import (
    FeatureNotImplemented,
    UpstreamAuthError,
    UpstreamError,
    UpstreamQuotaError,
)
from ...upstream import muse
from .noise import GatewayToken

log = logging.getLogger(__name__)

_TOKEN_URL = f"{muse.SITE}/api/hatch/token"
_WAKE_URL = muse.VM_WAKE_API
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
)
_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "origin": muse.SITE,
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": muse.NEW_THREAD_URL,
    "sec-ch-prefers-color-scheme": "dark",
    "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not(A:Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": _UA,
}
_JSON_HEADERS = {**_HEADERS, "accept": "application/json", "content-type": "application/json"}
_GATEWAY_RE = re.compile(r'gatewayUrl\\?"\s*:\s*\\?"(wss://[^"\\]+)')


@dataclass(frozen=True)
class GatewayTarget:
    vm_id: str
    vm_name: str
    gateway_url: str


def _cookie_dict(account_cookies: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in account_cookies.items() if v}


def _import_curl():
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:  # pragma: no cover - dependency error
        raise FeatureNotImplemented(
            "the http driver needs the 'curl_cffi', 'cryptography' and 'zstandard' packages"
        ) from exc
    return AsyncSession


async def _get_page(session: Any, cookies: dict[str, str]) -> str:
    response = await session.get(
        muse.NEW_THREAD_URL,
        cookies=cookies,
        headers={**_HEADERS, "accept": "text/html,application/xhtml+xml"},
        allow_redirects=True,
        timeout=30,
    )
    if response.status_code in (401, 403):
        raise UpstreamAuthError(
            f"muse.ai rejected the account cookies (HTTP {response.status_code})"
        )
    if response.status_code >= 400:
        raise UpstreamError(f"could not load the muse.ai thread page (HTTP {response.status_code})")
    text = response.text or ""
    # Newer Next.js payloads escape quotes inside the inline JSON. Normalise
    # only for discovery; this text is never sent back upstream.
    return text.replace('\\\\"', '"').replace('\\"', '"')


def discover_target(html: str, cookies: dict[str, str] | None = None) -> GatewayTarget:
    """Extract the selected VM from a ``/thread/new`` page.

    Exposed separately so tests can exercise a recorded page without network.
    """
    html = html.replace('\\\\"', '"').replace('\\"', '"')
    match = _GATEWAY_RE.search(html)
    gateway = match.group(1).rstrip("/") if match else ""
    vm_id = ""
    vm_name = ""
    if gateway:
        host = gateway.split("://", 1)[-1].split("/", 1)[0]
        vm_id = host.split(".", 1)[0]
    vm_match = re.search(r'vmId"\s*:\s*"([0-9a-fA-F-]{16,})"', html)
    if not vm_id and vm_match:
        vm_id = vm_match.group(1)
    # Prefer the name next to the selected gateway; a page can contain snapshot
    # entries for several VMs before the active assignment.
    if match:
        after = html[match.start() : match.start() + 2000]
        # Stay inside the object that owns this gatewayUrl; the next payload
        # entry may describe a different snapshot VM.
        object_end = after.find("}")
        segment = after[: object_end + 1] if object_end >= 0 else after
        before = html[max(0, match.start() - 2000) : match.start()]
        name_match = re.search(r'vmName"\s*:\s*"([^"]+)"', segment) or re.search(
            r'vmName"\s*:\s*"([^"]+)"', before
        )
    else:
        name_match = re.search(r'vmName"\s*:\s*"([^"]+)"', html)
    if name_match:
        vm_name = name_match.group(1)
    if vm_id and not vm_name:
        vm_name = vm_id
    if not gateway:
        gateway = f"wss://{vm_id}.metaaivm.com/" if vm_id else ""
    if not vm_id or not gateway:
        raise UpstreamAuthError(
            "the account page did not contain a workspace assignment; "
            "the cookies are probably expired"
        )
    return GatewayTarget(vm_id=vm_id, vm_name=vm_name or vm_id, gateway_url=gateway)


async def resolve_target(cookies: dict[str, str]) -> GatewayTarget:
    AsyncSession = _import_curl()
    async with AsyncSession(impersonate="chrome", timeout=30) as session:
        html = await _get_page(session, _cookie_dict(cookies))
    return discover_target(html, cookies)


async def _token_request(
    session: Any, cookies: dict[str, str], target: GatewayTarget
) -> tuple[str, str]:
    response = await session.post(
        _TOKEN_URL,
        json={"vmAddress": target.gateway_url, "vmName": target.vm_name},
        cookies=cookies,
        headers=_JSON_HEADERS,
        timeout=30,
    )
    if response.status_code in (401, 403):
        raise UpstreamAuthError(
            f"muse.ai rejected the gateway token request (HTTP {response.status_code})"
        )
    if response.status_code == 429:
        raise UpstreamQuotaError("muse.ai rate-limited the gateway token request")
    if response.status_code >= 400:
        raise UpstreamError(f"gateway token endpoint returned HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError as exc:
        raise UpstreamError("gateway token endpoint returned non-JSON") from exc
    token = body.get("token") if isinstance(body, dict) else None
    notary = body.get("notary_token") if isinstance(body, dict) else None
    if not isinstance(token, str) or not token or not isinstance(notary, str) or not notary:
        raise UpstreamError("gateway token response is missing token/notary_token")
    return token, notary


async def fetch_token(cookies: dict[str, str], target: GatewayTarget | None = None) -> GatewayToken:
    """Resolve the VM if necessary, then mint a gateway token.

    A transient failure is retried once after asking muse.ai to wake the VM; this
    covers the common cold-start case without hiding real auth errors.
    """
    AsyncSession = _import_curl()
    jar = _cookie_dict(cookies)
    if target is None:
        target = await resolve_target(jar)
    last_error: Exception | None = None
    async with AsyncSession(impersonate="chrome", timeout=30) as session:
        for attempt in range(2):
            try:
                token, notary = await _token_request(session, jar, target)
                return GatewayToken(
                    token=token,
                    notary_token=notary,
                    vm_id=target.vm_id,
                    vm_name=target.vm_name,
                )
            except UpstreamAuthError:
                raise
            except UpstreamQuotaError:
                raise
            except UpstreamError as exc:
                last_error = exc
                if attempt == 0:
                    await _wake_vm(session, jar, target.vm_id)
                    await asyncio.sleep(1.5)
                    continue
                raise
    raise last_error or UpstreamError("could not obtain a gateway token")


async def _wake_vm(session: Any, cookies: dict[str, str], vm_id: str) -> None:
    """Best-effort cold-start wake; token acquisition is authoritative."""
    try:
        import uuid

        response = await session.post(
            _WAKE_URL,
            json={"vm_id": vm_id, "retry_count": 0, "connect_attempt_id": str(uuid.uuid4())},
            cookies=cookies,
            headers=_JSON_HEADERS,
            timeout=20,
        )
        log.debug("hatch vm wake returned HTTP %s", response.status_code)
    except Exception as exc:  # noqa: BLE001 - optional warm start
        log.debug("hatch vm wake failed: %s", exc)
