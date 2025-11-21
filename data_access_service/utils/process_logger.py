from dask.callbacks import Callback
import psutil
import os
import resource


class ProcessLogger(Callback):
    def __init__(self, logger, task_name: str = "Dask Task"):
        self.logger = logger
        self.total_tasks = 0
        self.completed_tasks = 0
        self.task_name = task_name
        self.process = psutil.Process(os.getpid())
        self.start_maxrss = 0  # Track starting maxrss to calculate delta
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

    def _get_peak_memory_gb(self):
        """Get the true peak memory from system (maxrss)."""
        # resource.getrusage() returns maxrss in KB on Linux, bytes on macOS
        # We'll assume Linux since that's what the user is running
        maxrss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return maxrss_kb / (1024**2)  # Convert KB to GB

    def _start(self, dsk):
        self.total_tasks = len(dsk)
        self.completed_tasks = 0
        mem_gb = self.process.memory_info().rss / (1024**3)
        self.start_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_gb = self._get_peak_memory_gb()
        self.logger.info(
            f"{self.task_name} started. Memory: {mem_gb:.2f} GB (Peak so far: {peak_gb:.2f} GB)"
        )

    def _posttask(self, key, result, dsk, state, id):
        self.completed_tasks += 1
        percent = int((self.completed_tasks / self.total_tasks) * 100)
        # Check if milestones list is not empty before accessing
        if self.milestones and percent >= self.milestones[0]:
            mem_gb = self.process.memory_info().rss / (1024**3)
            peak_gb = self._get_peak_memory_gb()
            self.logger.info(
                f"{self.task_name} progress: {percent}% ({self.completed_tasks}/{self.total_tasks}) - Memory: {mem_gb:.2f} GB (Peak: {peak_gb:.2f} GB)"
            )
            # Remove all milestones that have been reached
            while self.milestones and percent >= self.milestones[0]:
                self.milestones.pop(0)

    def _finish(self, dsk, state, errored):
        mem_gb = self.process.memory_info().rss / (1024**3)
        peak_gb = self._get_peak_memory_gb()
        self.logger.info(
            f"{self.task_name} finished. Memory: {mem_gb:.2f} GB - Peak Memory: {peak_gb:.2f} GB"
        )
