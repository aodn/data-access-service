from dask.callbacks import Callback


class ProcessLogger(Callback):
    def __init__(self, logger, task_name: str = "Dask Task"):
        self.logger = logger
        self.total_tasks = 0
        self.completed_tasks = 0
        self.task_name = task_name
        self.milestones = {
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
        }
        self.logged_milestones = set()

    def _start(self, dsk):
        self.total_tasks = len(dsk)
        self.completed_tasks = 0
        self.logged_milestones.clear()
        self.logger.info(f"{self.task_name} started.")

    def _posttask(self, key, result, dsk, state, id):
        self.completed_tasks += 1
        percent = int((self.completed_tasks / self.total_tasks) * 100)
        if percent in self.milestones and percent not in self.logged_milestones:
            self.logger.info(
                f"{self.task_name} progress: {percent}% ({self.completed_tasks}/{self.total_tasks})"
            )
            self.logged_milestones.add(percent)

    def _finish(self, dsk, state, errored):
        self.logger.info(f" {self.task_name} finished.")
