import asyncio
import dataclasses
import datetime
import os
import platform
import time
import pytest

from typing import List, Tuple, AsyncGenerator

import ezmsg.core as ez

# We expect this test to generate LOTS of backpressure warnings
# PERF_LOGLEVEL = os.environ.get("EZMSG_LOGLEVEL", "ERROR")
# ez.logger.setLevel(PERF_LOGLEVEL)

PLATFORM = {
    "Darwin": "mac",
    "Linux": "linux",
    "Windows": "win",
}[platform.system()]
SAMPLE_SUMMARY_DATASET_PREFIX = "sample_summary"
COUNT_DATASET_NAME = "count"


try:
    import numpy as np
except ImportError:
    ez.logger.error("This test requires Numpy to run.")
    raise


def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


class LoadTestSettings(ez.Settings):
    duration: float = 30.0
    dynamic_size: int = 8
    buffers: int = 32


@dataclasses.dataclass
class LoadTestSample:
    _timestamp: float
    counter: int
    dynamic_data: np.ndarray


class LoadTestPublisher(ez.Unit):
    OUTPUT = ez.OutputStream(LoadTestSample)
    SETTINGS: LoadTestSettings

    def initialize(self) -> None:
        self.running = True
        self.counter = 0
        self.OUTPUT.num_buffers = self.SETTINGS.buffers

    @ez.publisher(OUTPUT)
    async def publish(self) -> AsyncGenerator:
        ez.logger.info(f"Load test publisher started. (PID: {os.getpid()})")
        start_time = time.time()
        while self.running:
            current_time = time.time()
            if current_time - start_time >= self.SETTINGS.duration:
                break

            yield self.OUTPUT, LoadTestSample(
                _timestamp=time.perf_counter(),
                counter=self.counter,
                dynamic_data=np.zeros(
                    int(self.SETTINGS.dynamic_size // 8), dtype=np.float32
                ),
            )
            self.counter += 1
        ez.logger.info("Exiting publish")
        raise ez.Complete

    def shutdown(self) -> None:
        self.running = False
        ez.logger.info(f"Samples sent: {self.counter}")


class LoadTestSubscriberState(ez.State):
    # Tuples of sent timestamp, received timestamp, counter, dynamic size
    received_data: List[Tuple[float, float, int]] = dataclasses.field(
        default_factory=list
    )
    counter: int = -1


class LoadTestSubscriber(ez.Unit):
    INPUT = ez.InputStream(LoadTestSample)
    SETTINGS: LoadTestSettings
    STATE: LoadTestSubscriberState

    @ez.subscriber(INPUT, zero_copy=True)
    async def receive(self, sample: LoadTestSample) -> None:
        if sample.counter != self.STATE.counter + 1:
            ez.logger.warning(
                f"{sample.counter - self.STATE.counter-1} samples skipped!"
            )
        self.STATE.received_data.append(
            (sample._timestamp, time.perf_counter(), sample.counter)
        )
        self.STATE.counter = sample.counter

    @ez.task
    async def log_result(self) -> None:
        ez.logger.info(f"Load test subscriber started. (PID: {os.getpid()})")

        # Wait for the duration of the load test
        await asyncio.sleep(self.SETTINGS.duration)
        # logger.info(f"STATE: {self.STATE.received_data}")

        # Log some useful summary statistics
        min_timestamp = min(timestamp for timestamp, _, _ in self.STATE.received_data)
        max_timestamp = max(timestamp for timestamp, _, _ in self.STATE.received_data)
        total_latency = abs(
            sum(
                receive_timestamp - send_timestamp
                for send_timestamp, receive_timestamp, _ in self.STATE.received_data
            )
        )

        counters = list(sorted(t[2] for t in self.STATE.received_data))
        dropped_samples = sum(
            [(x1 - x0) - 1 for x1, x0 in zip(counters[1:], counters[:-1])]
        )

        num_samples = len(self.STATE.received_data)
        ez.logger.info(f"Samples received: {num_samples}")
        ez.logger.info(
            f"Sample rate: {num_samples / (max_timestamp - min_timestamp)} Hz"
        )
        ez.logger.info(f"Mean latency: {total_latency / num_samples} s")
        ez.logger.info(f"Total latency: {total_latency} s")

        total_data = num_samples * self.SETTINGS.dynamic_size
        ez.logger.info(
            f"Data rate: {total_data / (max_timestamp - min_timestamp) * 1e-6} MB/s"
        )
        ez.logger.info(
            f"Dropped samples: {dropped_samples} ({dropped_samples / (dropped_samples + num_samples)}%)",
        )

        raise ez.NormalTermination


class LoadTest(ez.Collection):
    SETTINGS: LoadTestSettings

    PUBLISHER = LoadTestPublisher()
    SUBSCRIBER = LoadTestSubscriber()

    def configure(self) -> None:
        self.PUBLISHER.apply_settings(self.SETTINGS)
        self.SUBSCRIBER.apply_settings(self.SETTINGS)

    def network(self) -> ez.NetworkDefinition:
        return ((self.PUBLISHER.OUTPUT, self.SUBSCRIBER.INPUT),)

    def process_components(self):
        return (
            self.PUBLISHER,
            self.SUBSCRIBER,
        )


def get_time() -> float:
    # time.perf_counter() isn't system-wide on Windows Python 3.6:
    # https://bugs.python.org/issue37205
    return time.time() if PLATFORM == "win" else time.perf_counter()


@pytest.mark.parametrize("duration", [2])
@pytest.mark.parametrize("buffers", [2, 32])
@pytest.mark.parametrize("size", [2**i for i in range(5, 22, 4)])
def test_performance(duration, size, buffers) -> None:
    ez.logger.info(f"Running load test for dynamic size: {size} bytes")
    system = LoadTest(
        LoadTestSettings(
            dynamic_size=int(size),
            duration=duration,
            buffers=buffers
        )
    )
    ez.run(SYSTEM = system)


def run_many_dynamic_sizes(duration, buffers) -> None:
    for exp in range(5, 22, 4):
        test_performance(duration, 2**exp, buffers)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--many-dynamic-sizes",
        action="store_true",
        help="Run load test for many dynamic sizes",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=2,
        help="How long to run the load test (seconds)",
    )
    parser.add_argument(
        "--num-buffers", type=int, default=32, help="Shared memory buffers"
    )

    class Args:
        many_dynamic_sizes: bool
        duration: int
        num_buffers: int

    args = parser.parse_args(namespace=Args)

    if args.many_dynamic_sizes:
        run_many_dynamic_sizes(args.duration, args.num_buffers)
    else:
        test_performance(args.duration, 8, args.num_buffers)
