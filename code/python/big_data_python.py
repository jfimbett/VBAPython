r"""Entry-point script to run all Big Data benchmarks.

Installation (in your conda env):

    pip install pandas dask[complete] polars[rtcompat] pyspark

On macOS you may need to escape the brackets in some shells (in zsh/bash you usually don't need quotes in the requirements file):

    pip install pandas dask\[complete] polars[rtcompat] pyspark
"""

import argparse
import functools
import math
import statistics
import timeit

from big_data_utils import generate_dataset
from bench_pandas import run_pandas
from bench_spark import run_spark
from bench_dask import run_dask
from bench_polars import run_polars
# %%
def timeit_ci(_func=None, *, repeat=7):
    """
    Decorator that emulates IPython %timeit (auto loops + repeats)
    but reports a 95% confidence interval on the mean per-loop time.

    Usage:
        @timeit_ci
        def f(...):
            ...

        or

        @timeit_ci(repeat=10)
        def g(...):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Capture the last result of the function so we can return it
            result_holder = [None]

            def stmt():
                result_holder[0] = func(*args, **kwargs)

            timer = timeit.Timer(stmt)

            # Auto-determine number of loops so that total time >= ~0.2s
            # (this uses the same logic as timeit’s CLI: 1, 2, 5, 10, 20, 50, …)
            number, _ = timer.autorange()

            # Repeat timing `repeat` times
            all_runs = timer.repeat(repeat=repeat, number=number)

            # Convert to per-loop times
            per_loop = [t / number for t in all_runs]
            mean = statistics.mean(per_loop)

            if len(per_loop) > 1:
                # Sample standard deviation
                stdev = statistics.stdev(per_loop)
                # 95% CI half-width using normal approximation
                ci_half = 1.96 * stdev / math.sqrt(len(per_loop))
            else:
                stdev = float('nan')
                ci_half = float('nan')

            # Choose appropriate time unit (like %timeit)
            def scale(seconds: float):
                if seconds < 1e-9:
                    return seconds * 1e12, "ps"
                elif seconds < 1e-6:
                    return seconds * 1e9, "ns"
                elif seconds < 1e-3:
                    return seconds * 1e6, "µs"
                elif seconds < 1:
                    return seconds * 1e3, "ms"
                else:
                    return seconds, "s"

            mean_val, unit = scale(mean)
            ci_val, _ = scale(ci_half)

            def fmt(x: float) -> str:
                if math.isnan(x):
                    return "nan"
                # 3 significant digits, like %timeit
                return f"{x:.3g}"

            loops_str = f"{number:,}"

            # Output format modeled after %timeit
            # e.g. "764 µs ± 36.3 µs per loop (mean ± 95% CI of 7 runs, 1,000 loops each)"
            print(
                f"{fmt(mean_val)} {unit} ± {fmt(ci_val)} {unit} per loop "
                f"(mean ± 95% CI of {len(per_loop)} runs, {loops_str} loops each)"
            )

            return result_holder[0]

        return wrapper

    # Support both @timeit_ci and @timeit_ci(...)
    if _func is None:
        return decorator
    else:
        return decorator(_func)



if __name__ == "__main__":

    # run as python  big_data_python.py --n-companies 10000 --time-periods 1000

    parser = argparse.ArgumentParser(description="Big Data Processing with Python")
    parser.add_argument("--generate-data", action="store_true", help="Only generate the dataset and exit")
    parser.add_argument("--n-companies", type=int, default=10000, help="Number of companies")
    parser.add_argument("--time-periods", type=int, default=1000, help="Number of time periods")
    args = parser.parse_args()

    n_companies = args.n_companies
    time_periods = args.time_periods

    # Ensure data exists once
    generate_dataset(n_companies, time_periods)

    if args.generate_data:
        # Only data generation was requested
        raise SystemExit(0)

    print("\n=== Running pandas benchmark ===")
    result_pandas = run_pandas(n_companies, time_periods)

    print("\n=== Running Spark benchmark ===")
    result_spark = run_spark(n_companies, time_periods)

    print("\n=== Running Dask benchmark ===")
    result_dask = run_dask(n_companies, time_periods)

    print("\n=== Running Polars benchmark ===")
    result_polars = run_polars(n_companies, time_periods)

    print("\nDone. Benchmarks finished for all backends.")