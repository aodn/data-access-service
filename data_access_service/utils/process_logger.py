from dask.callbacks import Callback


class ProcessLogger(Callback):
    def __init__(self, logger):
        self.logger = logger
        self.total_tasks = 0
        self.completed_tasks = 0

    def _start(self, dsk):
        self.total_tasks = len(dsk)
        self.completed_tasks = 0
        self.logger.info("Dask computation started.")

    def _posttask(self, key, result, dsk, state, id):
        self.completed_tasks += 1
        percent = (self.completed_tasks / self.total_tasks) * 100
        self.logger.info(
            f"Progress: {percent:.2f}% ({self.completed_tasks}/{self.total_tasks})"
        )

    def _finish(self, dsk, state, errored):
        self.logger.info("Dask computation finished.")
