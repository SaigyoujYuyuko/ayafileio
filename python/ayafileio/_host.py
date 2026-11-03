import asyncio
import threading
import weakref


_NO_HOST_MESSAGE = "no running asyncio or Trio event loop"
_trio_hosts = {}
_trio_hosts_lock = threading.RLock()


def _load_trio():
    try:
        import trio
    except ModuleNotFoundError as error:
        if error.name != "trio":
            raise
        return None
    return trio


def _current_library():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        return "asyncio"

    trio = _load_trio()
    if trio is None:
        return None
    try:
        trio.lowlevel.current_trio_token()
    except RuntimeError:
        return None
    return "trio"


class _TrioFuture:
    __slots__ = ("_trio", "_done", "_result", "_exception", "_event")

    def __init__(self, trio):
        self._trio = trio
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
            raise RuntimeError("Trio operation completed twice")
        self._result = result
        self._finish()

    def set_exception(self, exception):
        if self._done:
            raise RuntimeError("Trio operation completed twice")
        self._exception = exception
        self._finish()

    async def _wait(self):
        if not self._done:
            if self._event is None:
                self._event = self._trio.Event()
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
        return _TrioFuture(self._trio)

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
    if library != "trio":
        raise RuntimeError(_NO_HOST_MESSAGE)

    trio = _load_trio()
    results = [None] * len(awaitables)

    async def wait_one(index, awaitable, nursery):
        try:
            results[index] = await awaitable
        except Exception as error:
            results[index] = error
            if cancel_on_error:
                nursery.cancel_scope.cancel()

    async with trio.open_nursery() as nursery:
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
    raise RuntimeError(_NO_HOST_MESSAGE)


async def _to_thread(function, *args):
    library = _current_library()
    if library == "asyncio":
        return await asyncio.to_thread(function, *args)
    if library == "trio":
        return await _load_trio().to_thread.run_sync(function, *args)
    raise RuntimeError(_NO_HOST_MESSAGE)
