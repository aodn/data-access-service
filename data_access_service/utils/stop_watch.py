import time

class StopWatch:
    def __init__(self):
        self.start_time = None
        self.task_name = None

    def start(self, task_name: str):
        self.start_time = time.time()
        self.task_name = task_name

    def stop(self):
        if self.start_time is None:
            raise ValueError("Stopwatch has not been started.")
        elapsed_time = time.time() - self.start_time
        print(f"{self.task_name} took {elapsed_time:.2f} seconds")
        self.start_time = None
        self.task_name = None