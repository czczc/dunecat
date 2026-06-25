import pytest
from typer.testing import CliRunner

import dunecat.download as d
from dunecat import cli
from dunecat.web import auth

runner = CliRunner()


# ---- URL rewriting ---------------------------------------------------------


def test_https_form_rewrites_fnal_gsi_door():
    url = "root://fndcadoor.fnal.gov:1094/pnfs/fnal.gov/usr/dune/x.hdf5"
    assert (
        d._https_form(url)
        == "https://fndcadoor.fnal.gov:2880/pnfs/fnal.gov/usr/dune/x.hdf5"
    )


def test_https_form_davs_to_https():
    assert (
        d._https_form("davs://eosctapublic.cern.ch:8444/eos/x.hdf5")
        == "https://eosctapublic.cern.ch:8444/eos/x.hdf5"
    )


def test_https_form_leaves_cern_root_url_untouched():
    url = "root://eospublic.cern.ch:1094//eos/x.hdf5"
    assert d._https_form(url) == url


# ---- scheme dispatch -------------------------------------------------------
#
# Both xrdcp and curl run via subprocess.call; fake it per-tool by inspecting
# argv[0]. A curl call "writes" the -o target so the returned path is real.


def _fake_subprocess(monkeypatch, *, rc=0, write=b""):
    """Patch shutil.which (every tool resolves) and subprocess.call. Records
    the last argv in the returned dict; for a curl -o call, writes `write` to
    the output path so the file exists."""
    seen: dict = {}

    monkeypatch.setattr(d.shutil, "which", lambda tool: f"/usr/bin/{tool}")

    def fake_call(cmd):
        seen["cmd"] = cmd
        if "-o" in cmd:
            out = cmd[cmd.index("-o") + 1]
            from pathlib import Path as _P

            _P(out).write_bytes(write)
        return rc

    monkeypatch.setattr(d.subprocess, "call", fake_call)
    return seen


def test_download_root_uses_xrdcp(monkeypatch, tmp_path):
    seen = _fake_subprocess(monkeypatch)
    out = d.download(
        "root://eospublic.cern.ch:1094//eos/dir/file.hdf5", tmp_path, token="T"
    )
    assert seen["cmd"][:2] == ["/usr/bin/xrdcp", "-f"]
    assert out == tmp_path / "file.hdf5"


def test_download_root_without_xrdcp_errors(monkeypatch, tmp_path):
    monkeypatch.setattr(d.shutil, "which", lambda _: None)
    with pytest.raises(d.DownloadError, match="xrdcp not found"):
        d.download("root://h:1094//eos/f.hdf5", tmp_path, token="T")


def test_download_https_uses_curl_and_writes_file(monkeypatch, tmp_path):
    seen = _fake_subprocess(monkeypatch, write=b"hello")
    out = d.download("https://host/path/file.hdf5", tmp_path, token="T")
    assert seen["cmd"][0] == "/usr/bin/curl"
    assert "Authorization: Bearer T" in seen["cmd"]
    assert out.read_bytes() == b"hello"


def test_download_curl_failure_raises(monkeypatch, tmp_path):
    _fake_subprocess(monkeypatch, rc=22)  # curl exit 22 == HTTP error (e.g. 403)
    with pytest.raises(d.DownloadError, match="curl failed"):
        d.download("https://host/path/file.hdf5", tmp_path, token="T")


def test_download_unsupported_scheme_errors(tmp_path):
    with pytest.raises(d.DownloadError, match="unsupported URL scheme"):
        d.download("ftp://host/file.hdf5", tmp_path, token="T")


# ---- tape (NEARLINE) gating ------------------------------------------------


def test_download_https_tape_resident_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(d, "_dcache_locality", lambda url, token: "NEARLINE")
    url = "https://fndcadoor.fnal.gov:2880/pnfs/fnal.gov/usr/dune/x.hdf5"
    with pytest.raises(d.DownloadError, match="tape-resident"):
        d.download(url, tmp_path, token="T")


def test_download_fnal_door_rewritten_then_online_downloads(monkeypatch, tmp_path):
    seen_loc = {}

    def fake_loc(url, token):
        seen_loc["url"] = url
        return "ONLINE"

    monkeypatch.setattr(d, "_dcache_locality", fake_loc)
    _fake_subprocess(monkeypatch, write=b"x")

    out = d.download(
        "root://fndcadoor.fnal.gov:1094/pnfs/fnal.gov/usr/dune/x.hdf5",
        tmp_path,
        token="T",
    )

    assert seen_loc["url"].startswith("https://fndcadoor.fnal.gov:2880/")
    assert out.read_bytes() == b"x"


# ---- CLI command -----------------------------------------------------------


def _stub_auth(monkeypatch):
    monkeypatch.setattr(cli, "load_dotenv", lambda: None)
    monkeypatch.setattr(auth, "prime", lambda: None)
    monkeypatch.setattr(auth, "ensure_fresh_bearer", lambda: None)


def test_cli_download_success(monkeypatch, tmp_path):
    _stub_auth(monkeypatch)
    tok = tmp_path / "bt"
    tok.write_text("dummytoken")
    monkeypatch.setenv("BEARER_TOKEN_FILE", str(tok))

    captured = {}

    def fake_download(url, dest, *, token):
        captured.update(url=url, dest=dest, token=token)
        return tmp_path / "file.hdf5"

    monkeypatch.setattr(d, "download", fake_download)

    result = runner.invoke(
        cli.app, ["download", "https://h/p/file.hdf5", "--dest", str(tmp_path)]
    )

    assert result.exit_code == 0, result.output
    assert "saved:" in result.stdout
    assert captured["token"] == "dummytoken"
    assert captured["url"] == "https://h/p/file.hdf5"


def test_cli_download_no_token_exits_2(monkeypatch, tmp_path):
    _stub_auth(monkeypatch)
    monkeypatch.setenv("BEARER_TOKEN_FILE", str(tmp_path / "missing"))

    result = runner.invoke(cli.app, ["download", "https://h/p/f.hdf5"])

    assert result.exit_code == 2
    assert "dunecat login" in result.stderr


def test_cli_download_vault_expired_exits_2(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "load_dotenv", lambda: None)
    monkeypatch.setattr(auth, "prime", lambda: None)

    def boom():
        raise auth.VaultExpiredError("Vault token expired. Run: dunecat login")

    monkeypatch.setattr(auth, "ensure_fresh_bearer", boom)

    result = runner.invoke(cli.app, ["download", "https://h/p/f.hdf5"])

    assert result.exit_code == 2
    assert "Vault token expired" in result.stderr
