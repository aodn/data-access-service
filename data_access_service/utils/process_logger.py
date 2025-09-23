from dask.callbacks import Callback


class ProcessLogger(Callback):
    def __init__(self, logger, task_name: str = "Dask Task"):
        self.logger = logger
        self.total_tasks = 0
        self.completed_tasks = 0
        self.task_name = task_name
        self.milestones = [
            1,
            2,
            3,
            4,
            5,
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
        ]

    def _start(self, dsk):
        self.total_tasks = len(dsk)
        self.completed_tasks = 0
        self.logger.info(f"{self.task_name} started.")

    def _posttask(self, key, result, dsk, state, id):
        self.completed_tasks += 1
        percent = int((self.completed_tasks / self.total_tasks) * 100)
        if percent > self.milestones[0]:
            self.logger.info(
                f"{self.task_name} progress: {percent}% ({self.completed_tasks}/{self.total_tasks})"
            )
            self.milestones.pop(0)

    def _finish(self, dsk, state, errored):
        self.logger.info(f" {self.task_name} finished.")
