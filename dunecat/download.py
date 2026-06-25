"""Download a single replica PFN to a local file.

The Python counterpart of ``scripts/dune-xrdcp.sh``: it picks the transfer
tool by URL scheme (``xrdcp`` for ``root://``, ``curl`` for
``davs://``/``https://``), rewrites FNAL dCache's gsi-only xrootd door to its
token-capable WebDAV door, and fails fast on tape-resident (NEARLINE) dCache
files instead of hitting a cryptic 403 on GET.

We shell out to ``curl`` (not Python ``requests``) for HTTPS for the same
reason the script does: grid storage like FNAL dCache presents a cert whose
intermediate CA (``InCommon RSA IGTF Server CA 3``) isn't in certifi's bundle
and the server doesn't send it, so ``requests`` fails verification. ``curl``
uses the system trust store, which has it.

Other grid endpoints (INFN/CNAF StoRM, FNAL dCache on Linux) present host
certs signed by IGTF CAs that aren't in any public/system store either. WLCG
nodes keep those CAs under ``$X509_CERT_DIR`` (default
``/etc/grid-security/certificates``); when that directory exists we point
curl at it with ``--capath`` so verification succeeds, and otherwise emit a
hint on the resulting cert error instead of a bare exit code.

Auth is the OIDC bearer minted by ``dunecat login``: ``xrdcp`` reads it from
``BEARER_TOKEN_FILE``; ``curl`` sends it as an Authorization header.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger(__name__)

# FNAL dCache's xrootd door advertises only gsi (X.509 proxy) auth; its WebDAV
# door on :2880 accepts bearer tokens, and the REST frontend on :3880 reports
# file locality (disk vs tape).
_FNAL_DOOR = "root://fndcadoor.fnal.gov:1094/"
_FNAL_WEBDAV = "https://fndcadoor.fnal.gov:2880/"
_FNAL_API = "https://fndcadoor.fnal.gov:3880/api/v1/namespace/"

_LOCALITY_TIMEOUT = 30


class DownloadError(Exception):
    """Anything that stops the transfer, with a user-facing message."""


def _https_form(url: str) -> str:
    """FNAL gsi door -> WebDAV door, and davs:// -> https://."""
    if url.startswith(_FNAL_DOOR):
        url = _FNAL_WEBDAV + url[len(_FNAL_DOOR):]
    if url.startswith("davs://"):
        url = "https://" + url[len("davs://"):]
    return url


def _require(tool: str, hint: str) -> str:
    path = shutil.which(tool)
    if not path:
        raise DownloadError(f"{tool} not found: {hint}")
    return path


def _capath_args() -> list[str]:
    """``--capath <dir>`` for curl when a grid CA directory is available,
    else empty. Honors ``$X509_CERT_DIR`` (WLCG convention), falling back to
    ``/etc/grid-security/certificates``. Additive to curl's default trust
    store, so publicly-signed hosts (e.g. PIC) keep working."""
    cadir = os.environ.get("X509_CERT_DIR") or "/etc/grid-security/certificates"
    return ["--capath", cadir] if os.path.isdir(cadir) else []


def _tls_hint(url: str) -> str:
    host = urlsplit(url).hostname or url
    return (
        f"TLS verification failed for {host} (curl exit 60): the host likely "
        "uses an IGTF/grid CA absent from your trust store. Install the grid "
        "CA bundle (e.g. /etc/grid-security/certificates via fetch-crl or "
        "osg-ca-certs) or point $X509_CERT_DIR at it, then retry."
    )


def _dcache_locality(webdav_url: str, token: str) -> str | None:
    """fileLocality for a FNAL dCache WebDAV URL via the :3880 REST frontend,
    or None if it can't be determined. Uses curl for the same CA reasons as the
    download itself."""
    api = _FNAL_API + webdav_url[len(_FNAL_WEBDAV):] + "?locality=true&qos=true"
    try:
        out = subprocess.run(
            [_require("curl", "needed for dCache locality check"),
             "-s", *_capath_args(), "-H", f"Authorization: Bearer {token}", api],
            capture_output=True,
            text=True,
            timeout=_LOCALITY_TIMEOUT,
        ).stdout
        return json.loads(out).get("fileLocality")
    except Exception as e:  # noqa: BLE001 — diagnostic only, never fatal
        log.debug("dCache locality check failed for %s: %s", api, e)
        return None


def download(url: str, dest_dir: str | Path, *, token: str) -> Path:
    """Download ``url`` into ``dest_dir`` and return the written path.

    ``token`` is the bearer string; callers must also have exported
    ``BEARER_TOKEN_FILE`` so ``xrdcp`` can find it for ``root://`` URLs.
    """
    url = _https_form(url)
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    scheme = urlsplit(url).scheme
    if scheme == "root":
        return _download_xrdcp(url, dest)
    if scheme == "https":
        # Fail fast on tape-resident FNAL dCache files (a GET would 403).
        if url.startswith(_FNAL_WEBDAV):
            loc = _dcache_locality(url, token)
            if loc and "ONLINE" not in loc:
                raise DownloadError(
                    f"file is tape-resident (fileLocality={loc}); request "
                    "staging to a disk RSE before downloading, or use a disk "
                    "replica."
                )
        return _download_curl(url, dest, token)
    raise DownloadError(f"unsupported URL scheme: {scheme or url!r}")


def _out_path(url: str, dest: Path) -> Path:
    name = Path(urlsplit(url).path).name
    if not name:
        raise DownloadError(f"could not derive a filename from {url!r}")
    return dest / name


def _download_xrdcp(url: str, dest: Path) -> Path:
    xrdcp = _require(
        "xrdcp",
        "install it (e.g. `brew install xrootd`) or use an https/davs replica",
    )
    rc = subprocess.call([xrdcp, "-f", url, f"{dest}/"])
    if rc != 0:
        raise DownloadError(f"xrdcp failed (exit {rc})")
    return _out_path(url, dest)


def _download_curl(url: str, dest: Path, token: str) -> Path:
    curl = _require("curl", "needed for https/davs downloads")
    out = _out_path(url, dest)
    # -f: fail (non-zero) on HTTP errors rather than saving an error page;
    # -L: follow redirects; -C -: resume a partial file on retry.
    cmd = [curl, "-fL", "-C", "-", "-H", f"Authorization: Bearer {token}",
           *_capath_args(), "-o", str(out), url]
    rc = subprocess.call(cmd)
    if rc == 60:  # peer certificate could not be authenticated
        raise DownloadError(_tls_hint(url))
    if rc != 0:
        raise DownloadError(f"curl failed (exit {rc})")
    return out
