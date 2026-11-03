import anyio
import pytest

import ayafileio


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_anyio_file_roundtrip_and_task_group(tmp_path, backend):
    path = tmp_path / f"{backend}.bin"

    async def main():
        async with ayafileio.aopen(path, "w+b") as handle:
            assert await handle.write(b"anyio") == 5

        async with ayafileio.aopen(path, "rb") as handle:
            results = []

            async def read_at(offset, size):
                results.append((offset, await handle.read_at(offset, size)))

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(read_at, 0, 2)
                tasks.start_soon(read_at, 2, 3)
            assert sorted(results) == [(0, b"an"), (2, b"yio")]

        assert await ayafileio.read_bytes(path) == b"anyio"

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_generic_anyio_host(backend):
    from ayafileio import _host

    async def main():
        host = _host._current_anyio_host()
        assert host is _host._current_anyio_host()
        future = host.create_future()
        with anyio.fail_after(5):
            host.call_soon_threadsafe(lambda: future.set_result(9))
            assert await future == 9

    anyio.run(main, backend=backend)


def test_anyio_host_uses_backend_neutral_scheduling():
    from anyio._core._exceptions import RunFinishedError
    from ayafileio import _host

    calls = []

    class ThirdBackend:
        @classmethod
        def run_sync_soon(cls, function, args, token):
            calls.append((function, args, token))
            function(*args)

    class ClosedBackend:
        @classmethod
        def run_sync_soon(cls, function, args, token):
            raise RunFinishedError

    async def main():
        native_token = object()
        token = anyio.lowlevel.EventLoopToken(ThirdBackend, native_token)
        host = _host._AnyIOHost(anyio, token, RunFinishedError)
        future = host.create_future()
        host.call_soon_threadsafe(lambda: future.set_result(12))
        assert await future == 12
        assert calls[0][2] is native_token

        failed = host.create_future()
        host.call_soon_threadsafe(
            lambda: failed.set_exception(OSError("third backend"))
        )
        with pytest.raises(OSError, match="third backend"):
            await failed

        closed = _host._AnyIOHost(
            anyio,
            anyio.lowlevel.EventLoopToken(ClosedBackend, object()),
            RunFinishedError,
        )
        with pytest.raises(RunFinishedError):
            closed.call_soon_threadsafe(lambda: None)
        assert closed.is_closed()

    anyio.run(main)


def test_helpers_can_use_generic_anyio_backend(monkeypatch):
    from ayafileio import _host

    monkeypatch.setattr(_host, "_current_library", lambda: "anyio")

    async def main():
        async def value(number):
            await anyio.sleep(0)
            return number

        assert await _host._gather([value(1), value(2)]) == [1, 2]
        semaphore = _host._semaphore(1)
        async with semaphore:
            assert semaphore.value == 0
        assert await _host._to_thread(lambda: 5) == 5

    anyio.run(main)
