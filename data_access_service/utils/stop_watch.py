import time

class StopWatch:
    """
    A simple stopwatch utility to measure the time taken for tasks. It is useful for performance monitoring and debugging.
    """
    def __init__(self):
        self.__start_time = None
        self.__task_name = None

    def start(self, task_name: str):
        self.__start_time = time.time()
        self.__task_name = task_name

    def stop(self):
        if self.__start_time is None:
            raise ValueError("Stopwatch has not been started.")
        elapsed_time = time.time() - self.__start_time
        print(f"Timer result: {self.__task_name} took {elapsed_time:.2f} seconds")
        self.__start_time = None
        self.__task_name = None