import asyncio

import pytest
import trio

import ayafileio


def test_asyncio_file_rejects_another_loop_but_close_succeeds(tmp_path):
    path = tmp_path / "asyncio-affinity.bin"
    path.write_bytes(b"host")
    handle = ayafileio.open(path, "rb")

    async def first_loop():
        assert await handle.read(1) == b"h"

    async def second_loop():
        with pytest.raises(RuntimeError, match="different event loop host"):
            await handle.read(1)
        await handle.close()

    asyncio.run(first_loop())
    asyncio.run(second_loop())
    assert handle.closed


def test_trio_file_rejects_another_run_but_close_succeeds(tmp_path):
    path = tmp_path / "trio-affinity.bin"
    path.write_bytes(b"host")
    handle = ayafileio.open(path, "rb")

    async def first_run():
        assert await handle.read(1) == b"h"

    async def second_run():
        with trio.fail_after(5):
            with pytest.raises(RuntimeError, match="different event loop host"):
                await handle.read(1)
            await handle.close()

    trio.run(first_run)
    trio.run(second_run)
    assert handle.closed


def test_file_can_close_in_another_framework(tmp_path):
    path = tmp_path / "framework-affinity.bin"
    path.write_bytes(b"host")
    handle = ayafileio.open(path, "rb")

    async def asyncio_read():
        assert await handle.read(1) == b"h"

    async def trio_use():
        with trio.fail_after(5):
            with pytest.raises(RuntimeError, match="different event loop host"):
                await handle.read(1)
            await handle.close()

    asyncio.run(asyncio_read())
    trio.run(trio_use)
    assert handle.closed
