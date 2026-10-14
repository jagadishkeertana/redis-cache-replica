"""Load test script and benchmark: throughput and latency under concurrent load."""

import argparse
import concurrent.futures
import os
import random
import socket
import sys
import time
import unittest
from typing import List, Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server.node import NodeServer
from client.client import MiniRedisClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def run_benchmark(
    host: str,
    port: int,
    concurrency: int = 10,
    total_requests: int = 1000,
    read_ratio: float = 0.7,
) -> Dict[str, Any]:
    """
    Execute concurrent load against Mini-Redis instance and collect performance metrics.
    """
    reqs_per_worker = total_requests // concurrency
    latencies: List[float] = []
    errors: List[str] = []

    # Pre-populate keys for read workload
    seed_client = MiniRedisClient(host=host, port=port)
    with seed_client:
        for i in range(100):
            seed_client.set(f"load_seed_{i}", f"val_{i}")

    def worker(worker_id: int):
        worker_latencies = []
        worker_errors = []
        client = MiniRedisClient(host=host, port=port, timeout=5.0)
        try:
            with client:
                for req_i in range(reqs_per_worker):
                    t0 = time.perf_counter()
                    try:
                        if random.random() < read_ratio:
                            key = f"load_seed_{random.randint(0, 99)}"
                            client.get(key)
                        else:
                            key = f"load_w{worker_id}_k{req_i}"
                            val = f"bench_val_{req_i}"
                            client.set(key, val)
                    except Exception as ex:
                        worker_errors.append(str(ex))

                    t1 = time.perf_counter()
                    worker_latencies.append((t1 - t0) * 1000.0)  # in milliseconds
        except Exception as e:
            worker_errors.append(f"Worker {worker_id} connection error: {e}")

        return worker_latencies, worker_errors

    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, w) for w in range(concurrency)]
        for f in concurrent.futures.as_completed(futures):
            wl, we = f.result()
            latencies.extend(wl)
            errors.extend(we)

    total_time = time.perf_counter() - start_time
    total_completed = len(latencies)

    latencies.sort()
    ops_per_sec = total_completed / total_time if total_time > 0 else 0
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    min_lat = latencies[0] if latencies else 0
    max_lat = latencies[-1] if latencies else 0
    avg_lat = sum(latencies) / len(latencies) if latencies else 0

    return {
        "concurrency": concurrency,
        "total_requests": total_completed,
        "total_time_sec": total_time,
        "ops_per_sec": ops_per_sec,
        "error_count": len(errors),
        "min_ms": min_lat,
        "avg_ms": avg_lat,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "max_ms": max_lat,
        "errors": errors[:5],
    }


def print_report(res: Dict[str, Any]) -> None:
    print("\n" + "=" * 50)
    print("      MINI-REDIS LOAD & LATENCY BENCHMARK")
    print("=" * 50)
    print(f"Concurrency level:       {res['concurrency']} clients")
    print(f"Completed requests:      {res['total_requests']}")
    print(f"Total duration:          {res['total_time_sec']:.3f} s")
    print(f"Throughput:              {res['ops_per_sec']:.2f} ops/sec")
    print(f"Errors:                  {res['error_count']}")
    print("-" * 50)
    print("Latency Distribution (ms):")
    print(f"  Min:                   {res['min_ms']:.2f} ms")
    print(f"  Avg:                   {res['avg_ms']:.2f} ms")
    print(f"  Median (p50):          {res['p50_ms']:.2f} ms")
    print(f"  p95:                   {res['p95_ms']:.2f} ms")
    print(f"  p99:                   {res['p99_ms']:.2f} ms")
    print(f"  Max:                   {res['max_ms']:.2f} ms")
    print("=" * 50 + "\n")


class TestLoadBenchmark(unittest.TestCase):
    """Unittest wrapper for CI/automated testing."""

    @classmethod
    def setUpClass(cls):
        cls.port = find_free_port()
        cls.wal = f"test_load_{cls.port}.wal"
        cls.node = NodeServer(host="127.0.0.1", port=cls.port, wal_path=cls.wal)
        cls.node.start(background=True)
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.node.stop()
        if os.path.exists(cls.wal):
            try:
                os.remove(cls.wal)
            except OSError:
                pass

    def test_load_and_latency(self):
        res = run_benchmark(
            host="127.0.0.1",
            port=self.port,
            concurrency=8,
            total_requests=400,
            read_ratio=0.7,
        )
        print_report(res)
        self.assertEqual(res["error_count"], 0)
        self.assertGreater(res["ops_per_sec"], 50)


def main():
    parser = argparse.ArgumentParser(description="Mini-Redis Load Tester")
    parser.add_argument("--host", default="127.0.0.1", help="Target host")
    parser.add_argument("--port", type=int, default=6379, help="Target port")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument("--requests", "-n", type=int, default=2000, help="Total number of requests")
    parser.add_argument("--read-ratio", type=float, default=0.7, help="Fraction of reads (0.0 to 1.0)")
    args = parser.parse_args()

    results = run_benchmark(
        host=args.host,
        port=args.port,
        concurrency=args.concurrency,
        total_requests=args.requests,
        read_ratio=args.read_ratio,
    )
    print_report(results)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] not in ("-v", "-q"):
        main()
    else:
        unittest.main()
