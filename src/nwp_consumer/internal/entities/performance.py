"""Class for tracking memory usage in a separate thread.

Adapted from the Joblib documentation:
https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html#memorymonitor-helper
"""

import time
from threading import Thread
from types import TracebackType

import psutil


class PerformanceMonitor(Thread):
    """Monitor the memory usage in MB in a separate thread.

    Note that this class is good enough to highlight the memory profile of
    Parallel in this example, but is not a general purpose profiler fit for
    all cases.
    """

    thread: Thread
    memory_buffer: list[int]
    cpu_buffer: list[float]
    start_time: float
    end_time: float | None
    stop: bool = True

    def __enter__(self) -> None:
        """Start the monitor."""
        super().__init__()
        self.stop = False
        self.memory_buffer: list[int] = []
        self.cpu_buffer: list[float] = []
        self.start_time = time.time()
        self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Stop the performance monitor, saving the results."""
        self.stop = True
        self.end_time = time.time()
        super().join(timeout=30)

    def get_usage(self) -> tuple[int, float]:
        """Get usage of a process and its children."""
        p = psutil.Process()
        # CPU usage of process and its children
        cpu: float = p.cpu_percent()
        # Memory usage does not reflect child processes
        # * Manually add the memory usage of child processes
        memory: int = p.memory_info().rss
        for c in p.children():
            memory += c.memory_info().rss
        return memory, cpu

    def get_runtime(self) -> int:
        """Get the runtime of the thread in seconds."""
        if self.end_time is None:
            return int(time.time() - self.start_time)
        return int(self.end_time - self.start_time)

    def run(self) -> None:
        """Run the thread."""
        memory_start, cpu_start = self.get_usage()
        while not self.stop:
            new_memory, new_cpu = self.get_usage()
            # Memory is just a total, so get the delta
            self.memory_buffer.append(new_memory - memory_start)
            # CPU is calculated by psutil against the base CPU,
            # so no need to get a delta
            self.cpu_buffer.append(new_cpu)
            time.sleep(0.2)

    def max_memory_mb(self) -> float:
        """Get the maximum memory usage during the thread's runtime."""
        return max(self.memory_buffer) / 1e6

    def max_cpu_percent(self) -> float:
        """Get the maximum CPU usage during the thread's runtime."""
        return max(self.cpu_buffer)
