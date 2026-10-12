"""`pondra.open()`: a node in this process, reached through its library's C ABI (pondra_lib) (ADR-061 step 4).

The library is loaded with ctypes from `$PONDRA_LIBRARY`, or from `_lib/` beside this file, where a
wheel that ships it puts it. A request goes to the node's router inside the library, not to a socket:
`Pondra._call` sends every answer that doesn't stream through `opener(db)`. A streaming answer (a live
query, a watch) still goes to the node's loopback port, which the node opens anyway.
"""
import atexit
import ctypes
import io
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.parse
import uuid
import warnings

from . import client

_NAME = {"darwin": "libpondra_lib.dylib", "win32": "pondra_lib.dll"}.get(sys.platform, "libpondra_lib.so")
_LIB = None  # the library, loaded by the first open
_LIB_PID = None  # the process that loaded it (a fork's child has a copy without the library's threads)
_FORKED = ("this process is a fork of the one that loaded pondra's library, and a fork's copy of it has none of its threads, "
           "so it can neither use that process's node nor open one: start processes with multiprocessing's 'spawn', and open the lake there")


class Piece(ctypes.Structure):
    """A piece of an answer's body, where the library holds it until the next read or `pondra_free`."""

    _fields_ = [("data", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class Answer(ctypes.Structure):
    """What `pondra_request` returns: the answer's status and headers. Its body is read with `pondra_read`
    a piece at a time as the node writes it (about 1 MB each for a big answer, a piece or two ahead), and
    each piece is copied out before the next is read, so the library never holds a big answer whole."""

    _fields_ = [("status", ctypes.c_uint16), ("headers_json", ctypes.c_void_p)]


def _library():
    """The library, loaded once. CDLL (not PyDLL) lets the GIL go for each call, so the node's threads
    run while Python waits for an answer."""
    global _LIB, _LIB_PID
    if _LIB is not None and _LIB_PID != os.getpid():
        raise RuntimeError(_FORKED)
    if _LIB is None:
        path = os.environ.get("PONDRA_LIBRARY") or os.path.join(os.path.dirname(__file__), "_lib", _NAME)
        if not os.path.isfile(path):
            raise RuntimeError(f"pondra.open needs the library its package ships (pondra/_lib/{_NAME}), and this install has none: "
                               f"use pondra.local(), or set PONDRA_LIBRARY (looked at {path})")
        lib = ctypes.CDLL(path)
        signatures = {
            "pondra_open": ([ctypes.c_char_p], ctypes.c_uint64),
            "pondra_request": ([ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t],
                               ctypes.POINTER(Answer)),
            "pondra_read": ([ctypes.POINTER(Answer), ctypes.POINTER(Piece)], ctypes.c_int32),
            "pondra_free": ([ctypes.POINTER(Answer)], None),
            "pondra_close": ([ctypes.c_uint64], ctypes.c_int32),
            "pondra_last_error": ([], ctypes.c_char_p),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(lib, name)
            fn.argtypes, fn.restype = args, result
        _LIB, _LIB_PID = lib, os.getpid()
    return _LIB


def _last_error():
    """This thread's last error from the library: what a call that failed says why."""
    return (_library().pondra_last_error() or b"").decode(errors="replace")


class Headers:
    """An answer's headers. A name's case doesn't matter, as in HTTP and in urllib's."""

    def __init__(self, pairs):
        self._pairs = dict(pairs)
        self._lower = {name.lower(): value for name, value in self._pairs.items()}

    def get(self, name, default=None):
        return self._lower.get(name.lower(), default)

    def items(self):
        return list(self._pairs.items())

    def __contains__(self, name):
        return name.lower() in self._lower


class Response:
    """An answer that didn't fail, as urllib gives one: `status`, `headers`, `read()` (the body) and
    its lines (`for line in r`)."""

    def __init__(self, status, headers, body):
        self.status, self.headers, self._body = status, headers, io.BytesIO(body)

    def read(self, size=-1):
        return self._body.read(size)

    def __iter__(self):
        return iter(self._body)

    def close(self):
        self._body.close()


def opener(db):
    """What `Pondra._call` sends a request through: `send(req)` answers it from the node in this
    process, as urllib's `opener.open(req)` would. (`timeout` is accepted and not used: there is no
    socket to time out; the call waits for the node's answer.)"""

    def send(req, timeout=None):
        if os.getpid() != db._pid:  # (calling into a fork's copy of the library could hang)
            raise RuntimeError(_FORKED)
        url = urllib.parse.urlsplit(req.full_url)
        path = url.path + (f"?{url.query}" if url.query else "")
        headers = json.dumps(dict(req.header_items())).encode()
        body = req.data or b""
        lib = _library()
        answer = lib.pondra_request(db._handle, req.get_method().encode(), path.encode(), headers, body, len(body))
        if not answer:  # (the handle is closed or wrong: no retry makes sense)
            raise RuntimeError(_last_error())
        try:
            a = answer.contents
            status = a.status
            head = Headers(json.loads(ctypes.string_at(a.headers_json)) if a.headers_json else {})
            data = _body(lib, answer)
        finally:
            lib.pondra_free(answer)
        if status >= 400:  # (raised as urllib raises it: `_call` reads its code, sqlstate and body)
            raise urllib.error.HTTPError(req.full_url, status, "", head, io.BytesIO(data))
        return Response(status, head, data)

    return send


def _body(lib, answer):
    """An answer's body, its pieces copied out one by one as the node writes them. A body whose work failed
    before its end is an error, never a shorter body."""
    out, piece = io.BytesIO(), Piece()
    while True:
        got = lib.pondra_read(answer, ctypes.byref(piece))
        if got == 0:
            return out.getvalue()
        if got < 0:
            raise RuntimeError(_last_error())
        out.write(memoryview((ctypes.c_char * piece.len).from_address(piece.data)))


def open(path=None, token=None, python=True, **settings):
    """Open a node on a lake in this process, and connect to it: no server and no second process.
    `path` is a folder or s3://bucket/prefix; with no path, a lake in a temporary folder that close()
    removes (DuckDB's `:memory:`). It returns the connection `connect()` and `local()` return, so
    frames, `sql`, `write_table` and the rest work the same. Its SQL may read files on this machine,
    as `local()`'s may. `settings` are the node's settings by name (`memory_gb=2` is
    PONDRA_MEMORY_GB), fixed by the first open in a process. One node at a time in a process: open
    another lake with ATTACH, or close this one first. Python procedures run with this Python
    (`python=False`: none). A request waits for the node's answer however long it takes: there is no
    socket to time out."""
    lib = _library()  # (first: a missing library leaves no folder behind)
    temp = tempfile.mkdtemp(prefix="pondra-") if path is None else None
    lake = temp or os.fspath(path)
    if "://" not in lake:
        os.makedirs(lake, exist_ok=True)
        lake = os.path.abspath(lake)
    port, owner = client._free_port(), uuid.uuid4().hex
    options = {name: _text(value) for name, value in settings.items() if value is not None}
    options.update(lake=lake, addr=f"127.0.0.1:{port}", owner=owner)  # (these win over a setting of the same name)
    if temp:
        options["temporary"] = True  # (the library removes it once the node lets go of it)
    if python:
        options["python"] = sys.executable
    try:
        options["launcher"] = client.binary()
    except RuntimeError:
        pass  # (no pondra program here: only an ATTACH of a lake nobody leads needs one)
    handle = lib.pondra_open(json.dumps(options).encode())
    if not handle:
        if temp:
            shutil.rmtree(temp, ignore_errors=True)
        raise RuntimeError(_last_error())
    db = client.Pondra(f"http://127.0.0.1:{port}", token)
    db.owner, db._handle, db._pid = owner, handle, os.getpid()
    db.lake = lake  # (where the lake is: a temporary one's folder, for a caller that wants to see it)
    db._inproc = opener(db)
    atexit.register(db.close)
    return db


def close(db):
    """Close the node `open()` made (`Pondra.close()` calls this), and with it a temporary lake. In a
    fork's child it does nothing: the node and its temporary folder are its parent's, and the child must
    not stop or remove them."""
    if db._pid != os.getpid():
        return
    if db._handle is not None:
        if _library().pondra_close(db._handle) != 0:
            warnings.warn(_last_error())  # (an unknown handle: there is nothing to stop)
    db._handle, db._inproc = None, None
    atexit.unregister(db.close)  # (50 opens and closes keep no 50 closed connections alive)
    db.retry_secs = 0  # (a closed node refuses at once: there is nothing to wait for)


def _text(value):
    """A setting's value as the node reads it: `True` is "true"."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
