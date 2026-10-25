"""`pondra.connect(lake)`: a node in this process, reached through its library's C ABI (pondra_lib) (ADR-061 step 4).

The library is loaded with ctypes from `$PONDRA_LIBRARY`, or from `_lib/` beside this file, where a
wheel that ships it puts it. A request goes to the node's router inside the library, not to a socket:
`Pondra._call` sends every request through `opener(db)`, answers that stream (a live query, a watch)
included. The loopback port the node opens is for what lives outside this process: the console in a
browser, Python's workers calling back, other processes on the lake.
"""
import atexit
import ctypes
import io
import json
import os
import sys
import threading
import urllib.parse
import uuid
import warnings

from . import client

_NAME = {"darwin": "libpondra_lib.dylib", "win32": "pondra_lib.dll"}.get(sys.platform, "libpondra_lib.so")
_LIB = None  # the library, loaded by the first open
_LIB_PID = None  # the process that loaded it (a fork's child has a copy without the library's threads)
_HELD = {}  # the node this process holds (one at a time): its lake, handle, address, owner key, and connections to it
_SLICE_MS = 250  # how long one call into the library waits before Python looks at its signals again (Ctrl-C)
_FORKED = ("this process is a fork of the one that loaded pondra's library, and a fork's copy of it has none of its threads, "
           "so it can neither use that process's node nor open one: start processes with multiprocessing's 'spawn', and open the lake there")


class Piece(ctypes.Structure):
    """A piece of an answer's body, where the library holds it until the next read or `pondra_free`."""

    _fields_ = [("data", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class Answer(ctypes.Structure):
    """What `pondra_request` returns: the answer's status and headers, once `pondra_wait` has them. Its body
    is read with `pondra_read` a piece at a time as the node writes it (about 1 MB each for a big answer, a
    piece or two ahead), and each piece is copied out before the next is read, so the library never holds a
    big answer whole."""

    _fields_ = [("status", ctypes.c_uint16), ("headers_json", ctypes.c_void_p)]


def _library():
    """The library, loaded once. CDLL (not PyDLL) lets the GIL go for each call, so the node's threads
    run while Python waits for an answer."""
    global _LIB, _LIB_PID
    if _LIB is not None and _LIB_PID != os.getpid():
        raise RuntimeError(_FORKED)
    if _LIB is None:
        path = _path()
        if not os.path.isfile(path):
            raise RuntimeError(f"a node in this process needs the library pondra's package ships (pondra/_lib/{_NAME}), and this install has none: "
                               f"set PONDRA_LIBRARY (looked at {path})")
        lib = ctypes.CDLL(path)
        signatures = {
            "pondra_open": ([ctypes.c_char_p], ctypes.c_uint64),
            "pondra_request": ([ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t],
                               ctypes.POINTER(Answer)),
            "pondra_wait": ([ctypes.POINTER(Answer), ctypes.c_int64], ctypes.c_int32),
            "pondra_read": ([ctypes.POINTER(Answer), ctypes.POINTER(Piece), ctypes.c_int64], ctypes.c_int32),
            "pondra_free": ([ctypes.POINTER(Answer)], None),
            "pondra_close": ([ctypes.c_uint64], ctypes.c_int32),
            "pondra_last_error": ([], ctypes.c_char_p),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(lib, name)
            fn.argtypes, fn.restype = args, result
        _LIB, _LIB_PID = lib, os.getpid()
    return _LIB


def _path():
    """Where the library is: `$PONDRA_LIBRARY`, else beside this file, where a wheel puts it."""
    return os.environ.get("PONDRA_LIBRARY") or os.path.join(os.path.dirname(__file__), "_lib", _NAME)


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
    """An answer that didn't fail, as urllib gives one: `status`, `headers`, `read()` (the body), its lines
    (`for line in r`), and `close()`."""

    def __init__(self, status, headers, body):
        self.status, self.headers, self._body = status, headers, body

    def read(self, size=-1):
        return self._body.read(size)

    def __iter__(self):
        return iter(self._body)

    def close(self):
        self._body.close()


class _Body(io.RawIOBase):
    """An answer in the library: its head, then its body a piece at a time as the node writes it. Each call
    into the library waits `_SLICE_MS` at most, so Python sees Ctrl-C (a notebook's Interrupt) between them.
    Closing it, from any thread, frees the answer and stops its work, as a client that goes does."""

    def __init__(self, lib, answer):
        self._lib, self._answer, self._lock = lib, answer, threading.Lock()
        self._piece, self._at = memoryview(b""), 0

    def _slice(self, call):
        """One call into the library on the answer, under its lock (None once closed)."""
        with self._lock:
            return None if self._answer is None else call(self._answer)

    def head(self):
        """The answer's status and headers, once the node has them."""
        while True:
            got = self._slice(lambda a: self._lib.pondra_wait(a, _SLICE_MS))
            if got is None:
                raise RuntimeError("the answer was closed before its head came")
            if got == 1:
                a = self._answer.contents
                return a.status, Headers(json.loads(ctypes.string_at(a.headers_json)) if a.headers_json else {})
            if got < 0:
                raise RuntimeError(_last_error())

    def _next(self):
        """The body's next piece (a copy), or None at its end. A body whose work failed before its end is an
        error, never a shorter body."""
        piece = Piece()
        while True:
            got = self._slice(lambda a: self._lib.pondra_read(a, ctypes.byref(piece), _SLICE_MS))
            if got is None or got == 0:
                return None
            if got == 1:
                return ctypes.string_at(piece.data, piece.len)
            if got < 0:
                raise RuntimeError(_last_error())

    def readable(self):
        return True

    def readinto(self, buffer):
        if self._at == len(self._piece):
            piece = self._next()
            if piece is None:
                return 0
            self._piece, self._at = memoryview(piece), 0
        n = min(len(buffer), len(self._piece) - self._at)
        buffer[:n] = self._piece[self._at:self._at + n]
        self._at += n
        return n

    def readall(self):
        parts = [self._piece[self._at:].tobytes()]
        self._piece, self._at = memoryview(b""), 0
        while (piece := self._next()) is not None:
            parts.append(piece)
        return b"".join(parts)

    def close(self):
        with self._lock:
            if self._answer is not None:
                self._lib.pondra_free(self._answer)
                self._answer = None
        super().close()


def opener(db):
    """What `Pondra._call` sends a request through: `send(req)` answers it from the node in this
    process, as urllib's `opener.open(req)` would (a refusal, 400 or more, is a `client._Refused`); with
    `stream`, its body is read as the node writes it.
    (`timeout` is accepted and not used: there is no socket to time out; the call waits for the node's
    answer, and Ctrl-C stops it.)"""

    def send(req, timeout=None, stream=False):
        if os.getpid() != db._pid:  # (calling into a fork's copy of the library could hang)
            raise RuntimeError(_FORKED)
        url = urllib.parse.urlsplit(req.full_url)
        path = url.path + (f"?{url.query}" if url.query else "")
        headers = json.dumps(dict(req.header_items())).encode()
        data = req.data or b""
        lib = _library()
        answer = lib.pondra_request(db._handle, req.get_method().encode(), path.encode(), headers, data, len(data))
        if not answer:  # (the handle is closed or wrong: no retry makes sense)
            raise RuntimeError(_last_error())
        body = _Body(lib, answer)  # (from here, closing it frees the answer: Ctrl-C included)
        try:
            status, head = body.head()
            if status >= 400:  # (raised as `_call` reads a refusal: its code, headers and body)
                raise client._Refused(status, head, body.readall())
            if stream:
                answered, body = Response(status, head, io.BufferedReader(body)), None
                return answered
            return Response(status, head, io.BytesIO(body.readall()))
        finally:
            if body is not None:
                body.close()

    return send


def connect(target=None, token=None, python=True, port=None, **settings):
    """`pondra.connect()` for a lake (`client.connect` says what each target is): its node in this
    process, or another connection to the node this process holds on it. Another lake, while this
    process holds a node, any lake where this install has no library, and any lake in a fork's child
    get a node of their own beside this process (`client._child`), which stops with it. A request waits for the node's answer
    however long it takes (there is no socket to time out), and Ctrl-C stops it, its work on the node too."""
    lake = ":memory:" if target is None else target
    if "://" not in lake and lake != ":memory:":
        os.makedirs(lake, exist_ok=True)
        lake = os.path.abspath(lake)
    here = _LIB is None or _LIB_PID == os.getpid()  # (a fork's child can't use the library its parent loaded: its nodes go beside it)
    if here and _HELD and lake != ":memory:" and _HELD["lake"] == lake:  # (the same lake again: another connection, as DuckDB gives)
        return _connection(token)
    if not here or _HELD or not (_LIB or os.path.isfile(_path())):
        env = {f"PONDRA_{name.upper()}": _text(value) for name, value in settings.items() if value is not None}
        return client._child(lake, port, token, python=python, env=env)
    return _open(lake, token, python, port, settings)


def _open(lake, token, python, port, settings):
    """Open a node on `lake` in this process (the first `connect()` of one), and connect to it. `settings`
    are fixed by the first node a process opens: a later one may repeat them, never change one."""
    lib = _library()
    port, owner = port or client._free_port(), uuid.uuid4().hex
    options = {name: _text(value) for name, value in settings.items() if value is not None}
    options.update(lake=lake, addr=f"127.0.0.1:{port}", owner=owner)  # (these win over a setting of the same name)
    if python:
        options["python"] = sys.executable
    try:
        options["launcher"] = client.binary()
    except RuntimeError:
        pass  # (no pondra program here: only an ATTACH of a lake nobody leads needs one)
    handle = lib.pondra_open(json.dumps(options).encode())
    if not handle:
        raise RuntimeError(_last_error())
    _HELD.update(lake=lake, handle=handle, url=f"http://127.0.0.1:{port}", owner=owner, connections=0)
    return _connection(token)


def _connection(token):
    """A connection to the node this process holds: its own session, the node's handle."""
    db = client.Pondra(_HELD["url"], token)
    db.owner, db._handle, db._pid = _HELD["owner"], _HELD["handle"], os.getpid()
    db.lake = _HELD["lake"]  # (where the lake is: a folder, a bucket's URL, or ":memory:")
    db._inproc = opener(db)
    _HELD["connections"] += 1
    atexit.register(db.close)
    return db


def open(path=None, token=None, python=True, **settings):
    """`connect(path)` before 0.34. It goes at 1.0."""
    warnings.warn("pondra.open() goes at 1.0: pondra.connect(path) is the same (and connect() alone a lake in memory)", DeprecationWarning, stacklevel=2)
    return connect(path, token, python, **settings)  # (as before 0.34: a second lake here gets a node beside, no longer refused)


def close(db):
    """Let a connection to this process's node go (`Pondra.close()` calls this). The last one closes
    the node, and with it a lake in memory. In a fork's child it does nothing: the node and its lake
    are its parent's, and the child must not stop them."""
    if db._pid != os.getpid():
        return
    if db._handle is not None and db._handle == _HELD.get("handle"):
        _HELD["connections"] -= 1
        if not _HELD["connections"]:
            _HELD.clear()
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
