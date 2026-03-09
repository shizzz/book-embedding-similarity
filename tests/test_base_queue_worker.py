import asyncio
import unittest

from app.workers.base.baseQueueWorker import BaseQueueWorker
from app.workers.stats.null_stats import NullStats
from app.infrastructure.models.channel import Channel
from app.infrastructure.models.task import Task


class DummyWorker(BaseQueueWorker[int]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_batches: list[list[int]] = []

    async def process(self, batch: list[Task], wid: int) -> list[Task]:
        self.processed_batches.append([t.entity for t in batch])
        return batch

    async def dispatch(self, result: Task):
        # No-op to avoid touching real channels in tests
        return


class TestBaseQueueWorker(unittest.IsolatedAsyncioTestCase):
    async def test_start_with_more_items_than_batch_finishes_and_shuts_down(self):
        batch_size = 2
        total_items = 5  # more than one batch

        queue: asyncio.Queue[Task[int]] = asyncio.Queue()
        producer_done = asyncio.Event()
        channel = Channel(downstream="next", queue=queue)

        worker = DummyWorker(
            input_channel=channel,
            output_channels=[],
            stats=NullStats(),
            batch_size=batch_size,
            name="TestStage",
            producer_done=producer_done,
            workers=1,
        )

        # Pre-fill the input queue with more items than a single batch.
        for i in range(total_items):
            await queue.put(Task(id=i, name=f"t{i}", entity=i))

        # Signal that producer is done (no more items will arrive).
        producer_done.set()

        await worker.start()
        await worker.wait()

        # Worker should have processed all items in batches and then shut down.
        self.assertTrue(worker.done.is_set())
        flattened = [item for batch in worker.processed_batches for item in batch]
        self.assertEqual(sorted(flattened), list(range(total_items)))


if __name__ == "__main__":
    unittest.main()

