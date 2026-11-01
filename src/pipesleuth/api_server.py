"""Owned foreground loopback server and private, process-local session file."""

import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import stat

from pipesleuth.pipeline import _check_no_symlinks


class ApiServerError(ValueError):
    """Unsafe startup/session configuration; no credentials are included."""


def clean_environment(source):
    """Keep essential platform values unchanged; remove proxies/provider settings."""
    result = {key: value for key, value in source.items()
              if key in {'PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL', 'SYSTEMROOT'}}
    result.update(OTEL_SDK_DISABLED='true', PYTHONDONTWRITEBYTECODE='1')
    return result


def write_session(path, port):
    requested = Path(path).expanduser().absolute()
    if requested.is_symlink():
        raise ApiServerError('session path must not be a symlink')
    path = requested.parent.resolve() / requested.name
    _check_no_symlinks(path)
    if path.exists():
        raise ApiServerError('session file already exists; choose a fresh path')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    session = {'base_url': f'http://127.0.0.1:{port}',
               'read_token': secrets.token_urlsafe(32), 'operation_token': secrets.token_urlsafe(32),
               'approval_token': secrets.token_urlsafe(32)}
    encoded = (json.dumps(session, sort_keys=True, separators=(',', ':')) + '\n').encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
            info = os.fstat(handle.fileno())
    except BaseException:
        # A failed write is preserved rather than deleting an uncertain artifact.
        raise
    identity = (info.st_dev, info.st_ino, hashlib.sha256(encoded).hexdigest())
    return path, session, identity


def remove_owned_session(path, identity):
    """Remove only our unchanged regular session file; preserve replacements."""
    try:
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        with os.fdopen(os.open(path, flags), 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
                return False
            encoded = handle.read(4097)
        if (info.st_dev, info.st_ino, hashlib.sha256(encoded).hexdigest()) != identity:
            return False
        current = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != identity[:2]:
            return False
        path.unlink()
        return True
    except OSError:
        return False


def serve_api(*, data_dir, output_dir, investigation_state_dir, repair_state_dir,
              session_file, port=8765, model_digest=None,
              model_endpoint='http://127.0.0.1:11436', model_timeout=120.0, agent_timeout=60.0):
    """Run exactly one owned local worker; no reload, proxy trust or cloud config."""
    # This function is the foreground CLI process, not a reusable app factory.
    # Preserve HOME rather than repurposing it for credentials or model storage.
    environment = clean_environment(os.environ)
    os.environ.clear()
    os.environ.update(environment)
    try:
        import uvicorn
        from pipesleuth.api import ApiSettings, create_app
    except ImportError:
        raise ApiServerError('install the optional dependencies from requirements-api.txt') from None
    settings = ApiSettings(data_dir, output_dir, investigation_state_dir, repair_state_dir,
                           port=port, endpoint=model_endpoint, model_digest=model_digest,
                           timeout_seconds=model_timeout, agent_timeout=agent_timeout)
    requested = Path(session_file).expanduser().absolute()
    session_path = requested.parent.resolve() / requested.name
    if any(root == session_path or root in session_path.parents or session_path in root.parents
           for root in (settings.data_dir, settings.output_dir, settings.investigation_state_dir, settings.repair_state_dir)):
        raise ApiServerError('session file must remain outside input, output and state directories')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    owned = None
    previous_handlers = {}
    try:
        # Claim the port before publishing a session. An occupied port is never reused.
        sock.bind(('127.0.0.1', settings.port))
        sock.listen(16)
        path, session, identity = write_session(session_file, settings.port)
        owned = (path, identity)
        app = create_app(settings, read_token=session['read_token'],
                         operation_token=session['operation_token'], approval_token=session['approval_token'])
        config = uvicorn.Config(app, host='127.0.0.1', port=settings.port,
            loop='asyncio', http='h11', ws='none', workers=1, reload=False,
            proxy_headers=False, forwarded_allow_ips='', access_log=False,
            log_level='error', server_header=False, date_header=False,
            timeout_keep_alive=5, timeout_graceful_shutdown=5,
            limit_concurrency=8, backlog=16, h11_max_incomplete_event_size=8192)
        print(f"PipeSleuth API: {session['base_url']}\nPrivate session file: {path}", flush=True)
        server = uvicorn.Server(config)
        # Uvicorn re-raises handled signals after shutdown. Retain a local
        # graceful handler until our owned session has been removed.
        def graceful_stop(_signum, _frame):
            server.should_exit = True
        previous_handlers = {sig: signal.signal(sig, graceful_stop)
                             for sig in (signal.SIGINT, signal.SIGTERM)}
        server.run(sockets=[sock])
        if not server.started:
            raise ApiServerError('local API did not complete startup')
    finally:
        sock.close()
        if owned is not None:
            remove_owned_session(*owned)
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
