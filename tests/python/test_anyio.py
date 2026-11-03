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
