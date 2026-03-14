from app.infrastructure.db import Migrator
from app.infrastructure.db.repositories import ModelRepository
from app.workers.stages import DbWorker
from .pipeline import Pipeline

class DbPipeline(Pipeline):
    def __init__(
        self,
        model_uid: str = None,
        model_name: str = None,
        threads: int = 1,
        batch_size: int = 256,
        *args, 
        **kwargs
    ):
        super().__init__(name="DB", *args, **kwargs)

        if model_uid:
            model_uids = [model_uid]
            ModelRepository(self._router).get_or_create(model_uid, model_name)
        else:
            model_uids = ModelRepository(self._router).get_uids()

        Migrator(self._router).migrate_all(model_uids)
        
        self._threads = threads
        self._batch_size = batch_size

    async def setup_stages(self) -> None:
        db_stage = DbWorker(
            router=self._router,
            registry=self._registry,
            input_channel=self._input_channel,
            output_channels=self._output_channels,
            stats=self._stats,
            batch_size=self._batch_size,
            workers=self._threads,
            logger=self._logger, 
        )
        self.pool.append(db_stage)