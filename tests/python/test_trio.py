import gc
import time
import weakref

import pytest
import trio.testing

import ayafileio
from ayafileio import _host


def test_trio_waiter_completion_and_multiple_waiters():
    async def main():
        host = _host._current_trio_host()
        assert host is _host._current_trio_host()

        completed = host.create_future()
        completed.set_result(7)
        assert await completed == 7
        assert await completed == 7

        pending = host.create_future()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_collect, pending, [])
            nursery.start_soon(_collect, pending, [])
            await trio.testing.wait_all_tasks_blocked()
            pending.set_result(11)

        failed = host.create_future()
        failed.set_exception(OSError("native failure"))
        with pytest.raises(OSError, match="native failure"):
            await failed

    async def _collect(future, results):
        results.append(await future)

    trio.run(main)


def test_trio_native_file_operations(tmp_path):
    path = tmp_path / "native.bin"

    async def main():
        with trio.fail_after(10):
            async with ayafileio.aopen(path, "w+b") as handle:
                assert await handle.write(b"0123456789") == 10
                assert await handle.write_many([(0, b"ab"), (4, b"XY")]) == [2, 2]
                assert await handle.read_many([(0, 2), (4, 2)]) == [b"ab", b"XY"]
                assert await handle.read_at(6, 4) == b"6789"
                await handle.seek(0)
                buffer = bytearray(4)
                assert await handle.readinto(buffer) == 4
                assert buffer == b"ab23"
                assert await handle.tell() == 4
                await handle.flush()

            async with ayafileio.open(path, "rb") as handle:
                assert await handle.read() == b"ab23XY6789"

            assert await ayafileio.read_bytes(path) == b"ab23XY6789"
            assert await ayafileio.write_bytes(path, b"whole") == 5
            assert await ayafileio.read_bytes(path) == b"whole"

    trio.run(main)


def test_trio_text_and_multi_file_helpers(tmp_path):
    async def main():
        with trio.fail_after(10):
            text_path = tmp_path / "text.txt"
            assert await ayafileio.write_text(text_path, "你好\nTrio", "utf-8") == 7
            assert await ayafileio.read_text(text_path, "utf-8") == "你好\nTrio"

            paths = [tmp_path / f"{index}.bin" for index in range(8)]
            counts = await ayafileio.write_bytes_many(
                (path, bytes([index])) for index, path in enumerate(paths)
            )
            assert counts == [1] * len(paths)
            assert await ayafileio.read_bytes_many(paths) == [
                bytes([index]) for index in range(len(paths))
            ]

            texts = [tmp_path / f"{index}.txt" for index in range(4)]
            assert await ayafileio.write_text_many(
                ((path, f"文本{index}") for index, path in enumerate(texts)),
                "utf-8",
            ) == [3, 3, 3, 3]
            assert await ayafileio.read_text_many(texts, "utf-8") == [
                f"文本{index}" for index in range(len(texts))
            ]

    trio.run(main)


def test_trio_pipelined_copy(tmp_path, monkeypatch):
    monkeypatch.setattr("ayafileio._copy._os_copy", None)
    source = tmp_path / "source.bin"
    target = tmp_path / "target.bin"
    payload = bytes(range(256)) * 4096
    source.write_bytes(payload)

    async def main():
        with trio.fail_after(15):
            assert await ayafileio.acopy(
                source, target, chunk_size=4096, concurrency=8, copy_stat=True
            ) == len(payload)

    trio.run(main)
    assert target.read_bytes() == payload


def test_trio_cancelled_waiter_does_not_break_file(tmp_path):
    path = tmp_path / "cancel.bin"
    payload = b"x" * (1024 * 1024)
    path.write_bytes(payload)

    async def main():
        with trio.fail_after(15):
            async with ayafileio.aopen(path, "rb") as handle:
                for _ in range(20):
                    started = trio.Event()

                    async def pending_read():
                        started.set()
                        await handle.read_at(0, len(payload))

                    async with trio.open_nursery() as nursery:
                        nursery.start_soon(pending_read)
                        await started.wait()
                        await trio.testing.wait_all_tasks_blocked()
                        nursery.cancel_scope.cancel()
                    assert await handle.read_at(0, 4) == b"xxxx"

    trio.run(main)


def test_trio_hosts_are_released_across_runs(tmp_path):
    path = tmp_path / "hosts.bin"
    path.write_bytes(b"host")
    references = []

    for _ in range(8):
        async def one_run():
            with trio.fail_after(5):
                async with ayafileio.aopen(path, "rb") as handle:
                    assert await handle.read() == b"host"
                references.append(weakref.ref(_host._current_trio_host()))

        trio.run(one_run)

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        gc.collect()
        if all(reference() is None for reference in references):
            break
        time.sleep(0.01)
    assert all(reference() is None for reference in references)
    assert not _host._trio_hosts


def test_native_operation_outside_supported_loop_has_clear_error(tmp_path):
    path = tmp_path / "outside.bin"
    path.write_bytes(b"outside")
    with pytest.raises(RuntimeError, match="asyncio or Trio"):
        ayafileio._ayafileio.read_whole_async(str(path))
