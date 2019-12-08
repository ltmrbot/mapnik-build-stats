import asyncio
from collections import deque
from sys import intern


class SubprocessFilterProtocol(asyncio.streams.FlowControlMixin,
                               asyncio.SubprocessProtocol):
    """
    Like SubprocessStreamProtocol, but instead of creating
    StreamReaders to feed pipe output into, use compatible
    data sinks provided to constructor.

    """

    def __init__(self, *, stdout=None, stderr=None, loop=None):
        super().__init__(loop=loop)
        self.stdin = None
        self.stdout = stdout
        self.stderr = stderr
        self._transport = None
        self._process_exited = False
        self._pipe_fds = 0

    def __repr__(self):
        info = [self.__class__.__name__]
        if self.stdin is not None:
            info.append(F"stdin={self.stdin!r}")
        if self.stdout is not None:
            info.append(F"stdout={self.stdout!r}")
        if self.stderr is not None:
            info.append(F"stderr={self.stderr!r}")
        return F"<{' '.join(info)}>"

    def connection_made(self, transport):
        self._transport = transport

        stdout_transport = transport.get_pipe_transport(1)
        if stdout_transport is not None:
            self.stdout.set_transport(stdout_transport)
            self._pipe_fds |= 1

        stderr_transport = transport.get_pipe_transport(2)
        if stderr_transport is not None:
            self.stderr.set_transport(stderr_transport)
            self._pipe_fds |= 2

        stdin_transport = transport.get_pipe_transport(0)
        if stdin_transport is not None:
            self.stdin = streams.StreamWriter(stdin_transport,
                                              protocol=self,
                                              reader=None,
                                              loop=self._loop)

    def pipe_data_received(self, fd, data):
        if fd == 1:
            reader = self.stdout
        elif fd == 2:
            reader = self.stderr
        else:
            reader = None
        if reader is not None:
            reader.feed_data(data)

    def pipe_connection_lost(self, fd, exc):
        if fd == 0:
            pipe = self.stdin
            if pipe is not None:
                pipe.close()
            self.connection_lost(exc)
            return
        if fd == 1:
            reader = self.stdout
        elif fd == 2:
            reader = self.stderr
        else:
            reader = None
        if reader is not None:
            if exc is None:
                reader.feed_eof()
            else:
                reader.set_exception(exc)
        self._pipe_fds &= ~fd
        self._maybe_close_transport()

    def process_exited(self):
        self._process_exited = True
        self._maybe_close_transport()

    def _maybe_close_transport(self):
        if self._pipe_fds == 0 and self._process_exited:
            self._transport.close()
            self._transport = None

#endclass


class StreamHasher:

    def __init__(self, hashfunc, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._eof = False
        self._waiter = loop.create_future()
        self._exception = None
        self._transport = None
        self._hasher = hashfunc()

    def __repr__(self):
        info = [self.__class__.__name__, F"{self._hasher.name}"]
        if self._waiter:
            info.append(F"w={self._waiter!r}")
        if self._exception:
            info.append(F"e={self._exception!r}")
        if self._transport:
            info.append(F"t={self._transport!r}")
        return F"<{' '.join(info)}>"

    def feed_data(self, data):
        assert not self._eof, "feed_data after feed_eof"
        if data:
            self._hasher.update(data)

    def feed_eof(self):
        self._eof = True
        if not self._waiter.cancelled():
            digest = intern(self._hasher.hexdigest())
            self._waiter.set_result(digest)

    def exception(self):
        return self._exception

    def set_exception(self, exc):
        self._exception = exc
        if not self._waiter.cancelled():
            self._waiter.set_exception(exc)

    def set_transport(self, transport):
        assert self._transport is None, "transport already set"
        self._transport = transport

    async def hexdigest(self):
        return await self._waiter

#endclass


async def async_batch(aiterable, *, max_concurrent=1, loop=None):
    """
    """
    if max_concurrent <= 0:
        raise ValueError("max_concurrent must be >= 1")

    if loop is None:
        loop = asyncio.get_event_loop()

    futures = deque()

    def _task_completed(task):
        try:
            fut_completed = futures[0]
            fut_completed.set_result(task)
        except:
            fut_completed = loop.create_future()
            fut_completed.set_result(task)
            futures.append(fut_completed)

    async def _whos_on_first():
        try:
            fut_completed = futures[0]
        except:
            fut_completed = loop.create_future()
            futures.append(fut_completed)
        task = await fut_completed
        futures.popleft()
        return task.result()

    num_pending = 0

    async for awaitable in aiterable:
        if num_pending < max_concurrent:
            num_pending += 1
        else:
            yield await _whos_on_first()
        task = asyncio.ensure_future(awaitable, loop=loop)
        task.add_done_callback(_task_completed)

    while num_pending > 0:
        yield await _whos_on_first()
        num_pending -= 1

#enddef


## SELF TEST

if __name__ == "__main__":
    async def sleepers(delays):
        for t in delays:
            yield asyncio.sleep(t, t)
    async def test_delays(concur, *delays):
        start = loop.time()
        tasks = sleepers(delays)
        async for y in async_batch(tasks, max_concurrent=concur):
            print(F'finished sleep({y}) after {loop.time() - start:.3f}')
    async def test_uniform(concur, total, delay):
        print(F'starting {total}x sleep({delay}) with max_concurrent={concur}')
        start = loop.time()
        tasks = sleepers(delay for _ in range(total))
        fin = 0
        async for y in async_batch(tasks, max_concurrent=concur):
            fin += 1
            if fin % concur == 0:
                print(F'finished {fin}x sleep({y}) after {loop.time() - start:.3f}')
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(test_delays(3, 2.2, 0.5, 0.3, 1.7, 0.9, 1.1))
        loop.run_until_complete(test_uniform(15, 90, 0.25))
    finally:
        loop.close()
