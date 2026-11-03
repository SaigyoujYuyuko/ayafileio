"""原生 Awaitable 的批量聚合。"""

from ._host import _gather


async def _gather_io(requests):
    """Submit native awaitables directly; drain submitted I/O before reporting errors."""
    pending = []
    try:
        for request in requests:
            pending.append(request)
    except BaseException:
        if pending:
            await _gather(pending, return_exceptions=True)
        raise
    results = await _gather(pending, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    return results
