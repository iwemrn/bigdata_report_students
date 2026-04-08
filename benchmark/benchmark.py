import argparse
import csv
import json
import math
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import matplotlib.pyplot as plt
import requests


DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_RESULTS_DIR = "results"
DEFAULT_THREAD_COUNTS = [1, 2, 4, 8, 16]
DEFAULT_TOTAL_REQUESTS = 100
DEFAULT_HTTP_TIMEOUT = 120
DEFAULT_WARMUP_REQUESTS = 10
SPECIALTY_IDS = [1, 2, 3, 4]

SCENARIOS = [
    "read_only",
    "create_only",
    "update_only",
    "report_only",
    "read_update_mix",
    "update_report_mix",
]

session_local = threading.local()
students_lock = threading.Lock()
known_student_ids: list[str] = []

created_counter_lock = threading.Lock()
created_counter = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark for LR7: distributed student system"
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL of app service")
    parser.add_argument(
        "--threads",
        default="1,2,4,8,16",
        help="Comma-separated thread counts, e.g. 1,2,4,8,16"
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=DEFAULT_TOTAL_REQUESTS,
        help="Total requests per scenario"
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=DEFAULT_WARMUP_REQUESTS,
        help="Warmup requests before main benchmark"
    )
    parser.add_argument(
        "--results-dir",
        default=DEFAULT_RESULTS_DIR,
        help="Directory for benchmark outputs"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_HTTP_TIMEOUT,
        help="Timeout per HTTP request in seconds"
    )
    parser.add_argument(
        "--scenarios",
        default=",".join(SCENARIOS),
        help="Comma-separated scenarios to run"
    )
    return parser.parse_args()


def parse_thread_counts(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_scenarios(value: str) -> list[str]:
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    for scenario in parsed:
        if scenario not in SCENARIOS:
            raise ValueError(f"Unknown scenario: {scenario}")
    return parsed


def get_session() -> requests.Session:
    if not hasattr(session_local, "session"):
        session_local.session = requests.Session()
    return session_local.session


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0

    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return sorted_vals[0]

    rank = (p / 100) * (len(sorted_vals) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)

    if low == high:
        return sorted_vals[low]

    weight = rank - low
    return sorted_vals[low] * (1 - weight) + sorted_vals[high] * weight


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def build_unique_student_name() -> str:
    global created_counter
    with created_counter_lock:
        created_counter += 1
        idx = created_counter
    return f"Benchmark Student {idx} {int(time.time() * 1000)}"


def load_initial_students(base_url: str, timeout: int) -> None:
    global known_student_ids

    session = requests.Session()
    response = session.get(f"{base_url}/", timeout=timeout)
    response.raise_for_status()

    data = response.json()
    ids = [item["id"] for item in data if "id" in item]

    with students_lock:
        known_student_ids = ids.copy()


def get_random_student_id() -> str | None:
    with students_lock:
        if not known_student_ids:
            return None
        return random.choice(known_student_ids)


def add_student_id(student_id: str) -> None:
    with students_lock:
        if student_id not in known_student_ids:
            known_student_ids.append(student_id)


def choose_new_status(current_status: str | None = None) -> str:
    statuses = ["учится", "отчислен"]
    if current_status in statuses and len(statuses) > 1:
        statuses = [s for s in statuses if s != current_status]
    return random.choice(statuses)


def read_students_operation(base_url: str, timeout: int) -> dict[str, Any]:
    session = get_session()
    start = time.perf_counter()
    response = session.get(f"{base_url}/", timeout=timeout)
    elapsed = time.perf_counter() - start

    return {
        "operation": "read",
        "status_code": response.status_code,
        "ok": response.ok,
        "elapsed": elapsed,
        "error": "" if response.ok else response.text[:500],
    }


def create_student_operation(base_url: str, timeout: int) -> dict[str, Any]:
    session = get_session()
    payload = {
        "name": build_unique_student_name(),
        "status": random.choice(["учится", "отчислен"]),
        "specialty_id": random.choice(SPECIALTY_IDS),
    }

    start = time.perf_counter()
    response = session.post(f"{base_url}/", json=payload, timeout=timeout)
    elapsed = time.perf_counter() - start

    error = ""
    if response.ok:
        try:
            data = response.json()
            if "id" in data:
                add_student_id(data["id"])
        except Exception as e:
            error = f"JSON parse error after create: {e}"
    else:
        error = response.text[:500]

    return {
        "operation": "create",
        "status_code": response.status_code,
        "ok": response.ok,
        "elapsed": elapsed,
        "error": error,
    }


def update_student_operation(base_url: str, timeout: int) -> dict[str, Any]:
    session = get_session()
    student_id = get_random_student_id()

    if student_id is None:
        return {
            "operation": "update",
            "status_code": 0,
            "ok": False,
            "elapsed": 0.0,
            "error": "No student ids available for update",
        }

    get_resp = session.get(f"{base_url}/{student_id}", timeout=timeout)
    if not get_resp.ok:
        return {
            "operation": "update",
            "status_code": get_resp.status_code,
            "ok": False,
            "elapsed": 0.0,
            "error": f"Could not fetch student before update: {get_resp.text[:300]}",
        }

    student = get_resp.json()
    current_name = student.get("name", "Updated Student")
    current_status = student.get("status", "учится")
    current_specialty_id = student.get("specialty_id", 1)

    updated_payload = {
        "name": f"{current_name} [upd]",
        "status": choose_new_status(current_status),
        "specialty_id": random.choice(
            [sid for sid in SPECIALTY_IDS if sid != current_specialty_id] or SPECIALTY_IDS
        ),
    }

    start = time.perf_counter()
    response = session.put(f"{base_url}/{student_id}", json=updated_payload, timeout=timeout)
    elapsed = time.perf_counter() - start

    return {
        "operation": "update",
        "status_code": response.status_code,
        "ok": response.ok,
        "elapsed": elapsed,
        "error": "" if response.ok else response.text[:500],
    }


def report_operation(base_url: str, timeout: int) -> dict[str, Any]:
    session = get_session()
    start = time.perf_counter()
    response = session.get(f"{base_url}/report/json", timeout=timeout)
    elapsed = time.perf_counter() - start

    return {
        "operation": "report",
        "status_code": response.status_code,
        "ok": response.ok,
        "elapsed": elapsed,
        "error": "" if response.ok else response.text[:500],
    }


def execute_operation(operation_name: str, base_url: str, timeout: int) -> dict[str, Any]:
    if operation_name == "read":
        return read_students_operation(base_url, timeout)
    if operation_name == "create":
        return create_student_operation(base_url, timeout)
    if operation_name == "update":
        return update_student_operation(base_url, timeout)
    if operation_name == "report":
        return report_operation(base_url, timeout)

    return {
        "operation": operation_name,
        "status_code": 0,
        "ok": False,
        "elapsed": 0.0,
        "error": f"Unknown operation: {operation_name}",
    }


def scenario_to_operation(index: int, scenario_name: str) -> str:
    if scenario_name == "read_only":
        return "read"
    if scenario_name == "create_only":
        return "create"
    if scenario_name == "update_only":
        return "update"
    if scenario_name == "report_only":
        return "report"
    if scenario_name == "read_update_mix":
        return "read" if index % 2 == 0 else "update"
    if scenario_name == "update_report_mix":
        return "update" if index % 2 == 0 else "report"

    raise ValueError(f"Unknown scenario: {scenario_name}")


def run_single_request(index: int, scenario_name: str, base_url: str, timeout: int) -> dict[str, Any]:
    operation_name = scenario_to_operation(index, scenario_name)
    result = execute_operation(operation_name, base_url, timeout)
    result["scenario"] = scenario_name
    result["request_index"] = index
    return result


def compute_metrics(results: list[dict[str, Any]], total_test_time: float) -> dict[str, Any]:
    latencies = [r["elapsed"] for r in results]
    success_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - success_count
    error_rate = (fail_count / len(results) * 100) if results else 0.0

    avg = statistics.mean(latencies) if latencies else 0.0
    median = statistics.median(latencies) if latencies else 0.0
    stdev = statistics.stdev(latencies) if len(latencies) > 1 else 0.0
    min_latency = min(latencies) if latencies else 0.0
    max_latency = max(latencies) if latencies else 0.0
    p90 = percentile(latencies, 90)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    throughput = (len(results) / total_test_time) if total_test_time > 0 else 0.0

    return {
        "total_requests": len(results),
        "success_count": success_count,
        "fail_count": fail_count,
        "error_rate": error_rate,
        "total_test_time": total_test_time,
        "average_response_time": avg,
        "median_response_time": median,
        "stdev_response_time": stdev,
        "min_response_time": min_latency,
        "max_response_time": max_latency,
        "p90_response_time": p90,
        "p95_response_time": p95,
        "p99_response_time": p99,
        "throughput": throughput,
    }


def save_raw_results_csv(all_request_results: list[dict[str, Any]], results_dir: str, timestamp: str) -> str:
    path = os.path.join(results_dir, f"benchmark_raw_{timestamp}.csv")

    fieldnames = [
        "scenario",
        "request_index",
        "operation",
        "status_code",
        "ok",
        "elapsed",
        "error",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_request_results:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    return path


def save_summary_csv(summary_rows: list[dict[str, Any]], results_dir: str, timestamp: str) -> str:
    path = os.path.join(results_dir, f"benchmark_summary_{timestamp}.csv")

    fieldnames = [
        "scenario",
        "threads",
        "total_requests",
        "success_count",
        "fail_count",
        "error_rate",
        "total_test_time",
        "average_response_time",
        "median_response_time",
        "stdev_response_time",
        "min_response_time",
        "max_response_time",
        "p90_response_time",
        "p95_response_time",
        "p99_response_time",
        "throughput",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    return path


def save_summary_json(summary_rows: list[dict[str, Any]], results_dir: str, timestamp: str) -> str:
    path = os.path.join(results_dir, f"benchmark_summary_{timestamp}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary_rows, f, ensure_ascii=False, indent=2)
    return path


def save_text_report(summary_rows: list[dict[str, Any]], results_dir: str, timestamp: str) -> str:
    path = os.path.join(results_dir, f"benchmark_report_{timestamp}.txt")

    with open(path, "w", encoding="utf-8") as f:
        f.write("ОТЧЕТ ПО ЛАБОРАТОРНОЙ РАБОТЕ 7. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ДАННЫХ\n")
        f.write("Система: учет студентов по специальностям\n")
        f.write(f"Дата формирования: {timestamp}\n")
        f.write("=" * 100 + "\n\n")

        for row in summary_rows:
            f.write(f"Тип запросов: {row['scenario']}\n")
            f.write(f"Количество потоков: {row['threads']}\n")
            f.write(f"Общее кол-во запросов: {row['total_requests']}\n")
            f.write(f"Успешных запросов: {row['success_count']}\n")
            f.write(f"Ошибок: {row['fail_count']}\n")
            f.write(f"Процент ошибок: {row['error_rate']:.2f}%\n")
            f.write(f"Суммарное время теста: {row['total_test_time']:.4f} сек.\n")
            f.write(f"Среднее время ответа: {row['average_response_time']:.4f} сек.\n")
            f.write(f"Медиана: {row['median_response_time']:.4f} сек.\n")
            f.write(f"Стандартное отклонение: {row['stdev_response_time']:.4f} сек.\n")
            f.write(f"Минимум: {row['min_response_time']:.4f} сек.\n")
            f.write(f"Максимум: {row['max_response_time']:.4f} сек.\n")
            f.write(f"90% квантиль: {row['p90_response_time']:.4f} сек.\n")
            f.write(f"95% квантиль: {row['p95_response_time']:.4f} сек.\n")
            f.write(f"99% квантиль: {row['p99_response_time']:.4f} сек.\n")
            f.write(f"Пропускная способность: {row['throughput']:.4f} req/s\n")
            f.write("-" * 100 + "\n")

    return path


def plot_metric(summary_rows: list[dict[str, Any]], scenario_names: list[str], metric_key: str, title: str, ylabel: str, results_dir: str, timestamp: str) -> str:
    plt.figure(figsize=(10, 6))

    for scenario_name in scenario_names:
        rows = [r for r in summary_rows if r["scenario"] == scenario_name]
        rows.sort(key=lambda x: x["threads"])

        x = [row["threads"] for row in rows]
        y = [row[metric_key] for row in rows]

        plt.plot(x, y, marker="o", label=scenario_name)

    plt.title(title)
    plt.xlabel("Количество потоков")
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.legend()

    output_path = os.path.join(results_dir, f"{metric_key}_{timestamp}.png")
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
    return output_path


def save_plots(summary_rows: list[dict[str, Any]], scenario_names: list[str], results_dir: str, timestamp: str) -> list[str]:
    paths = []
    paths.append(
        plot_metric(
            summary_rows, scenario_names,
            "average_response_time",
            "Среднее время ответа от числа потоков",
            "Среднее время ответа, сек.",
            results_dir, timestamp
        )
    )
    paths.append(
        plot_metric(
            summary_rows, scenario_names,
            "p95_response_time",
            "95% квантиль от числа потоков",
            "P95, сек.",
            results_dir, timestamp
        )
    )
    paths.append(
        plot_metric(
            summary_rows, scenario_names,
            "throughput",
            "Пропускная способность от числа потоков",
            "Throughput, req/s",
            results_dir, timestamp
        )
    )
    paths.append(
        plot_metric(
            summary_rows, scenario_names,
            "error_rate",
            "Процент ошибок от числа потоков",
            "Error rate, %",
            results_dir, timestamp
        )
    )
    return paths


def run_scenario(scenario_name: str, threads: int, total_requests: int, base_url: str, timeout: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    print(f"\n[START] scenario={scenario_name}, threads={threads}, total_requests={total_requests}")

    started_at = time.perf_counter()
    results: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(run_single_request, i, scenario_name, base_url, timeout)
            for i in range(total_requests)
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                result = {
                    "scenario": scenario_name,
                    "request_index": -1,
                    "operation": "unknown",
                    "status_code": 0,
                    "ok": False,
                    "elapsed": 0.0,
                    "error": f"Unhandled exception: {e}",
                }
            results.append(result)

    total_test_time = time.perf_counter() - started_at
    metrics = compute_metrics(results, total_test_time)
    metrics["scenario"] = scenario_name
    metrics["threads"] = threads

    print(
        f"[DONE] scenario={scenario_name}, threads={threads}, "
        f"avg={metrics['average_response_time']:.4f}s, "
        f"p95={metrics['p95_response_time']:.4f}s, "
        f"throughput={metrics['throughput']:.2f} req/s, "
        f"errors={metrics['fail_count']}"
    )

    return results, metrics


def warmup(base_url: str, timeout: int, warmup_requests: int) -> None:
    if warmup_requests <= 0:
        return

    print(f"Выполняется прогрев системы ({warmup_requests} запросов)...")
    for _ in range(warmup_requests):
        try:
            read_students_operation(base_url, timeout)
        except Exception:
            pass
    print("Прогрев завершён.\n")


def main() -> None:
    args = parse_args()

    base_url = args.base_url.rstrip("/")
    thread_counts = parse_thread_counts(args.threads)
    scenarios = parse_scenarios(args.scenarios)
    total_requests = args.requests
    warmup_requests = args.warmup
    timeout = args.timeout
    results_dir = args.results_dir

    ensure_dir(results_dir)

    print("Проверка доступности системы...")
    load_initial_students(base_url, timeout)
    print(f"Найдено студентов для update-сценариев: {len(known_student_ids)}")

    warmup(base_url, timeout, warmup_requests)

    timestamp = now_str()
    all_request_results: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    for scenario_name in scenarios:
        for threads in thread_counts:
            results, metrics = run_scenario(
                scenario_name=scenario_name,
                threads=threads,
                total_requests=total_requests,
                base_url=base_url,
                timeout=timeout,
            )
            all_request_results.extend(results)
            summary_rows.append(metrics)

    raw_csv = save_raw_results_csv(all_request_results, results_dir, timestamp)
    summary_csv = save_summary_csv(summary_rows, results_dir, timestamp)
    summary_json = save_summary_json(summary_rows, results_dir, timestamp)
    text_report = save_text_report(summary_rows, results_dir, timestamp)
    plot_paths = save_plots(summary_rows, scenarios, results_dir, timestamp)

    print("\nТестирование завершено.")
    print(f"RAW CSV: {raw_csv}")
    print(f"SUMMARY CSV: {summary_csv}")
    print(f"SUMMARY JSON: {summary_json}")
    print(f"TEXT REPORT: {text_report}")
    for plot_path in plot_paths:
        print(f"PLOT: {plot_path}")


if __name__ == "__main__":
    main()