"""Class for tracking memory usage in a separate thread.

Adapted from the Joblib documentation:
https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html#memorymonitor-helper
"""

import time
from threading import Thread

import psutil


class PerformanceMonitor(Thread):
    """Monitor the memory usage in MB in a separate thread.

    Note that this class is good enough to highlight the memory profile of
    Parallel in this example, but is not a general purpose profiler fit for
    all cases.
    """

    memory_buffer: list[int]
    stop: bool
    start_time: float
    end_time: float

    def __init__(self) -> None:
        """Create a new instance."""
        super().__init__()
        self.stop = False
        self.memory_buffer: list[int] = []
        self.start_time = time.time()
        self.start()

    def get_memory(self) -> int:
        """Get memory of a process and its children."""
        p = psutil.Process()
        memory: int = p.memory_info().rss
        for c in p.children():
            memory += c.memory_info().rss
        return memory

    def get_runtime(self) -> int:
        """Get the runtime of the thread in seconds."""
        return int(self.end_time - self.start_time)

    def run(self) -> None:
        """Run the thread."""
        memory_start = self.get_memory()
        while not self.stop:
            self.memory_buffer.append(self.get_memory() - memory_start)
            time.sleep(0.2)

    def join(self, timeout: int | None = None) -> None:  # type: ignore
        """Stop the thread."""
        self.stop = True
        self.end_time = time.time()
        super().join(timeout=timeout)
