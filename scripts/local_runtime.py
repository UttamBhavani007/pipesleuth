"""Serve an already installed Ollama binary and cached model in the foreground.

No installation, download, pull, account setup, or background service is provided.
The trusted caller supplies both local paths. Stop the foreground server with Ctrl-C.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import socket
import sys


ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "http://127.0.0.1:11436"
MODEL = "qwen3:1.7b"


def validate_paths(binary: Path, models: Path) -> tuple[Path, Path]:
    """Require existing trusted local artifacts; never create or populate a cache."""

    binary = binary.expanduser().absolute()
    models = models.expanduser().absolute()
    if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("--binary must identify an existing regular executable")
    if models.is_symlink() or not models.is_dir():
        raise ValueError("--models must identify an existing model cache")
    manifest = models / "manifests/registry.ollama.ai/library/qwen3/1.7b"
    if manifest.is_symlink() or not manifest.is_file() or manifest.stat().st_size > 1024 * 1024:
        raise ValueError("cached qwen3:1.7b manifest is missing or invalid; no automatic download")
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    layers = metadata.get("layers")
    if not isinstance(layers, list) or not 1 <= len(layers) <= 20:
        raise ValueError("cached model layers are invalid")
    for layer in layers:
        digest, size = layer.get("digest"), layer.get("size")
        if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise ValueError("cached model digest is invalid")
        if type(size) is not int or size < 0:
            raise ValueError("cached model size is invalid")
        blob = models / "blobs" / digest.replace(":", "-")
        if blob.is_symlink() or not blob.is_file() or blob.stat().st_size != size:
            raise ValueError("cached model is incomplete; no automatic download")
    return binary, models


def runtime_environment(models: Path) -> dict[str, str]:
    """Keep essential platform values unchanged; exclude proxies, tokens and overrides."""

    essential = {"PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "LANG", "LC_ALL", "TERM"}
    environment = {name: value for name, value in os.environ.items() if name in essential}
    environment.update({
        "OLLAMA_HOST": ENDPOINT,
        "OLLAMA_MODELS": str(models),
        "OLLAMA_NO_CLOUD": "1",
        "OLLAMA_CREATE_REMOTE": "0",
        "OLLAMA_NOPRUNE": "1",
        "OLLAMA_CONTEXT_LENGTH": "8192",
        "OLLAMA_NUM_PARALLEL": "1",
        "OLLAMA_MAX_LOADED_MODELS": "1",
        "OLLAMA_MAX_QUEUE": "4",
        "OLLAMA_KEEP_ALIVE": "0",
        "OLLAMA_LOAD_TIMEOUT": "120s",
        "OLLAMA_NOHISTORY": "1",
        "OLLAMA_DEBUG": "0",
        "OLLAMA_DEBUG_LOG_REQUESTS": "0",
        "OLLAMA_ORIGINS": ENDPOINT,
        "NO_PROXY": "127.0.0.1,::1,localhost",
        "no_proxy": "127.0.0.1,::1,localhost",
    })
    return environment


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True, help="Trusted installed Ollama executable")
    parser.add_argument("--models", type=Path, required=True, help="Existing cache containing qwen3:1.7b")
    arguments = parser.parse_args()
    try:
        binary, models = validate_paths(arguments.binary, arguments.models)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 11436))
        print(f"Foreground local runtime: {ENDPOINT}; cloud disabled; cached {MODEL}; Ctrl-C to stop", flush=True)
        # Replace this wrapper so Ctrl-C reaches the foreground Ollama process.
        os.chdir(ROOT)
        os.execve(binary, [str(binary), "serve"], runtime_environment(models))
    except (OSError, ValueError, TypeError, AttributeError) as error:
        # Runtime validation messages never include a credential or environment dump.
        print(f"Local runtime unavailable ({type(error).__name__}); check binary, cache and port 11436", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
