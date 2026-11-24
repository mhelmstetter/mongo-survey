"""Connections: the seed client from the user's URI, plus direct per-member clients.

Direct clients reuse the seed URI's credentials and TLS settings (after SRV
resolution) and add directConnection=true, so they talk to exactly one host.
"""
from __future__ import annotations

import os
import re
import threading
from typing import Dict, Optional

from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

from . import __version__

APP_NAME = f"mongo-survey/{__version__}"

# URI options that describe the seed topology and must not be copied onto a
# direct single-host connection.
_TOPOLOGY_OPTIONS = {
    "replicaset",
    "directconnection",
    "loadbalanced",
    "readpreference",
    "readpreferencetags",
    "maxstalenessseconds",
    "srvservicename",
    "srvmaxhosts",
    "appname",
}

_USERINFO_RE = re.compile(r"^(mongodb(?:\+srv)?://)([^@/]+)@")


def redact_uri(uri: str) -> str:
    """Replace the password in a MongoDB URI with ***. The username is kept."""
    m = _USERINFO_RE.match(uri)
    if not m:
        return uri
    userinfo = m.group(2)
    user = userinfo.split(":", 1)[0]
    return m.group(1) + (user + ":***" if ":" in userinfo else user) + "@" + uri[m.end():]


def read_uri(uri: Optional[str], uri_file: Optional[str]) -> str:
    if uri and uri_file:
        raise SystemExit("error: give only one of --uri / --uri-file")
    if uri_file:
        with open(os.path.expanduser(uri_file)) as f:
            value = f.read().strip()
        if not value:
            raise SystemExit(f"error: {uri_file} is empty")
        lines = [ln.strip() for ln in value.splitlines() if ln.strip()]
        if len(lines) != 1:
            raise SystemExit(f"error: {uri_file} must contain exactly one connection string "
                             f"(found {len(lines)} non-empty lines)")
        return lines[0]
    if uri:
        return uri
    env = os.environ.get("MONGODB_URI")
    if env:
        return env
    raise SystemExit("error: no connection string; use --uri, --uri-file or MONGODB_URI")


class Connector:
    """Owns the seed client and a cache of direct per-host clients."""

    def __init__(self, uri: str, timeout_ms: int = 15000):
        self.uri = uri
        self.redacted_uri = redact_uri(uri)
        self.timeout_ms = timeout_ms
        parsed = parse_uri(uri)
        self.username = parsed["username"]
        # First label of the first host as written in the URI (for SRV: "mycluster"
        # from mycluster.<id>.mongodb.net), used to name the capture file.
        first_host = re.split(r"[,/?]", re.sub(r"^mongodb(\+srv)?://([^@/]*@)?", "", uri))[0].split(":")[0]
        label = first_host.split(".")[0]
        self.display_name = first_host if re.fullmatch(r"[0-9.]+", first_host) else (label or "cluster")
        self.password = parsed["password"]
        self.direct_options = {
            k: v for k, v in dict(parsed["options"]).items() if k.lower() not in _TOPOLOGY_OPTIONS
        }
        self.seed = MongoClient(
            uri,
            appname=APP_NAME,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
        )
        self._direct: Dict[str, MongoClient] = {}
        self._lock = threading.Lock()

    def direct(self, host: str) -> MongoClient:
        """A client connected to exactly `host` ("hostname:port")."""
        with self._lock:
            client = self._direct.get(host)
            if client is None:
                kwargs = dict(self.direct_options)
                if self.username is not None:
                    kwargs["username"] = self.username
                    kwargs["password"] = self.password
                client = MongoClient(
                    f"mongodb://{host}/",
                    directConnection=True,
                    appname=APP_NAME,
                    serverSelectionTimeoutMS=self.timeout_ms,
                    connectTimeoutMS=self.timeout_ms,
                    **kwargs,
                )
                self._direct[host] = client
            return client

    def close(self) -> None:
        for c in self._direct.values():
            c.close()
        self.seed.close()
