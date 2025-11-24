"""Dask benchmark for the Big Data dataset.

Requires:
    pip install dask[complete]
"""

from big_data_utils import get_data_path

import functools
import math
import statistics
import timeit

import dask.dataframe as dd
import numpy as np


def timeit_ci(_func=None, *, repeat=7):
    """Local copy of the timing decorator to avoid circular imports."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result_holder = [None]

            def stmt():
                result_holder[0] = func(*args, **kwargs)

            timer = timeit.Timer(stmt)
            number, _ = timer.autorange()
            all_runs = timer.repeat(repeat=repeat, number=number)
            per_loop = [t / number for t in all_runs]
            mean = statistics.mean(per_loop)

            if len(per_loop) > 1:
                stdev = statistics.stdev(per_loop)
                ci_half = 1.96 * stdev / math.sqrt(len(per_loop))
            else:
                ci_half = float("nan")

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
                return f"{x:.3g}"

            loops_str = f"{number:,}"
            print(
                f"{fmt(mean_val)} {unit} ± {fmt(ci_val)} {unit} per loop "
                f"(mean ± 95% CI of {len(per_loop)} runs, {loops_str} loops each)"
            )

            return result_holder[0]

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


@timeit_ci
def run_dask(n_companies: int, time_periods: int):
    path_to_data = get_data_path(n_companies, time_periods)
    ddf = dd.read_csv(path_to_data, assume_missing=True)
    ddf["Date"] = dd.to_datetime(ddf["Date"])

    expected_returns = ddf.groupby("Ticker")["Return"].mean().compute()

    def _cumulative(group):
        group = group.sort_values("Date")
        # numerically stable cumulative return using log-sum-exp style
        # clip extreme negatives so 1 + r stays in (0, +inf) for log1p
        r = group["Return"].clip(lower=-0.999999, upper=1e6)
        group["log_1p"] = np.log1p(r)
        group["log_cum"] = group.groupby("Ticker")["log_1p"].cumsum()
        group["Cumulative_Return"] = np.expm1(group["log_cum"])
        group["HWM"] = group["Cumulative_Return"].cummax()
        group["Drawdown"] = group["Cumulative_Return"] / group["HWM"] - 1
        return group

    ddf_cs = ddf.map_partitions(_cumulative)
    max_drawdown = ddf_cs.groupby("Ticker")["Drawdown"].min().compute()

    std_returns = ddf.groupby("Ticker")["Return"].std().compute()
    sharpe_ratio = expected_returns / std_returns

    return {
        "expected_returns": expected_returns,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
    }
