
import asyncio


class FifoQueue(asyncio.Queue):

    async def put(self, item):
        if self.maxsize > 0 and self.qsize() >= self.maxsize:
            await super().get()
        await super().put(item)

    def put_nowait(self, item):
        if self.maxsize > 0 and self.qsize() >= self.maxsize:
            super().get_nowait()
        return super().put_nowait(item)
