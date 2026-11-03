import asyncio
import threading
import weakref


_NO_HOST_MESSAGE = "no running asyncio, Trio, or AnyIO event loop"
_trio_hosts = {}
_trio_hosts_lock = threading.RLock()
_anyio_hosts = {}
_anyio_hosts_lock = threading.RLock()


def _load_trio():
    try:
        import trio
    except ModuleNotFoundError as error:
        if error.name != "trio":
            raise
        return None
    return trio


def _load_anyio():
    try:
        import anyio.lowlevel
    except ModuleNotFoundError as error:
        if error.name != "anyio":
            raise
        return None
    return anyio


def _current_library():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        return "asyncio"

    trio = _load_trio()
    if trio is not None:
        try:
            trio.lowlevel.current_trio_token()
        except RuntimeError:
            pass
        else:
            return "trio"

    anyio = _load_anyio()
    if anyio is None or not hasattr(anyio.lowlevel, "current_token"):
        return None
    try:
        anyio.lowlevel.current_token()
    except RuntimeError:
        return None
    return "anyio"


class _HostFuture:
    __slots__ = ("_backend", "_done", "_result", "_exception", "_event")

    def __init__(self, backend):
        self._backend = backend
        self._done = False
        self._result = None
        self._exception = None
        self._event = None

    def _finish(self):
        self._done = True
        if self._event is not None:
            self._event.set()

    def set_result(self, result):
        if self._done:
            raise RuntimeError("Host operation completed twice")
        self._result = result
        self._finish()

    def set_exception(self, exception):
        if self._done:
            raise RuntimeError("Host operation completed twice")
        self._exception = exception
        self._finish()

    async def _wait(self):
        if not self._done:
            if self._event is None:
                self._event = self._backend.Event()
            await self._event.wait()
        if self._exception is not None:
            raise self._exception
        return self._result

    def __await__(self):
        return self._wait().__await__()


class _TrioHost:
    __slots__ = ("_trio", "_token", "_closed", "__weakref__")

    def __init__(self, trio, token):
        self._trio = trio
        self._token = token
        self._closed = False

    def create_future(self):
        return _HostFuture(self._trio)

    def call_soon_threadsafe(self, callback):
        try:
            self._token.run_sync_soon(callback)
        except self._trio.RunFinishedError:
            self._closed = True
            raise

    def is_closed(self):
        return self._closed


def _discard_trio_host(token, reference):
    with _trio_hosts_lock:
        if _trio_hosts.get(token) is reference:
            del _trio_hosts[token]


def _current_trio_host():
    trio = _load_trio()
    if trio is None:
        raise RuntimeError(_NO_HOST_MESSAGE)
    try:
        token = trio.lowlevel.current_trio_token()
    except RuntimeError:
        raise RuntimeError(_NO_HOST_MESSAGE) from None

    with _trio_hosts_lock:
        reference = _trio_hosts.get(token)
        host = reference() if reference is not None else None
        if host is not None:
            return host
        host = _TrioHost(trio, token)
        reference = weakref.ref(
            host, lambda ref: _discard_trio_host(token, ref)
        )
        _trio_hosts[token] = reference
        return host


class _AnyIOHost:
    __slots__ = (
        "_anyio", "_token", "_run_finished_error", "_closed", "__weakref__"
    )

    def __init__(self, anyio, token, run_finished_error):
        self._anyio = anyio
        self._token = token
        self._run_finished_error = run_finished_error
        self._closed = False

    def create_future(self):
        return _HostFuture(self._anyio)

    def call_soon_threadsafe(self, callback):
        try:
            self._token.backend_class.run_sync_soon(
                callback, (), self._token.native_token
            )
        except self._run_finished_error:
            self._closed = True
            raise

    def is_closed(self):
        return self._closed


def _discard_anyio_host(token, reference):
    with _anyio_hosts_lock:
        if _anyio_hosts.get(token) is reference:
            del _anyio_hosts[token]


def _current_anyio_host():
    anyio = _load_anyio()
    if anyio is None or not hasattr(anyio.lowlevel, "current_token"):
        raise RuntimeError(_NO_HOST_MESSAGE)
    try:
        token = anyio.lowlevel.current_token()
    except RuntimeError:
        raise RuntimeError(_NO_HOST_MESSAGE) from None
    from anyio._core._exceptions import RunFinishedError

    with _anyio_hosts_lock:
        reference = _anyio_hosts.get(token)
        host = reference() if reference is not None else None
        if host is not None:
            return host
        host = _AnyIOHost(anyio, token, RunFinishedError)
        reference = weakref.ref(
            host, lambda ref: _discard_anyio_host(token, ref)
        )
        _anyio_hosts[token] = reference
        return host


async def _gather(awaitables, *, return_exceptions=False, cancel_on_error=False):
    awaitables = list(awaitables)
    library = _current_library()
    if library == "asyncio":
        if not cancel_on_error:
            return await asyncio.gather(
                *awaitables, return_exceptions=return_exceptions
            )
        tasks = [asyncio.ensure_future(awaitable) for awaitable in awaitables]
        try:
            return await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    if library == "trio":
        task_manager = _load_trio().open_nursery()
    elif library == "anyio":
        task_manager = _load_anyio().create_task_group()
    else:
        raise RuntimeError(_NO_HOST_MESSAGE)

    results = [None] * len(awaitables)

    async def wait_one(index, awaitable, nursery):
        try:
            results[index] = await awaitable
        except Exception as error:
            results[index] = error
            if cancel_on_error:
                nursery.cancel_scope.cancel()

    async with task_manager as nursery:
        for index, awaitable in enumerate(awaitables):
            nursery.start_soon(wait_one, index, awaitable, nursery)

    if not return_exceptions:
        for result in results:
            if isinstance(result, BaseException):
                raise result
    return results


def _semaphore(value):
    library = _current_library()
    if library == "asyncio":
        return asyncio.Semaphore(value)
    if library == "trio":
        return _load_trio().Semaphore(value)
    if library == "anyio":
        return _load_anyio().Semaphore(value)
    raise RuntimeError(_NO_HOST_MESSAGE)


async def _to_thread(function, *args):
    library = _current_library()
    if library == "asyncio":
        return await asyncio.to_thread(function, *args)
    if library == "trio":
        return await _load_trio().to_thread.run_sync(function, *args)
    if library == "anyio":
        return await _load_anyio().to_thread.run_sync(function, *args)
    raise RuntimeError(_NO_HOST_MESSAGE)
