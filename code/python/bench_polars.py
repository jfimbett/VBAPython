"""Polars benchmark for the Big Data dataset.

Requires:
    pip install "polars[rtcompat]"
"""

from big_data_utils import get_data_path

import functools
import math
import statistics
import timeit

import polars as pl


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
def run_polars(n_companies: int, time_periods: int):
    path_to_data = get_data_path(n_companies, time_periods)
    df = pl.read_csv(path_to_data, try_parse_dates=True)

    expected_returns = (
        df.group_by("Ticker")
        .agg(pl.col("Return").mean().alias("Expected_Return"))
        .to_pandas().set_index("Ticker")["Expected_Return"]
    )

    df = df.sort(["Ticker", "Date"])
    df = df.with_columns([
        ((1 + pl.col("Return")).cum_prod()).over("Ticker").alias("Cumulative_Return"),
    ])
    df = df.with_columns([
        pl.col("Cumulative_Return").cum_max().over("Ticker").alias("HWM"),
    ])
    df = df.with_columns([
        (pl.col("Cumulative_Return") / pl.col("HWM") - 1).alias("Drawdown"),
    ])

    max_drawdown = (
        df.group_by("Ticker")
        .agg(pl.col("Drawdown").min().alias("Max_Drawdown"))
        .to_pandas().set_index("Ticker")["Max_Drawdown"]
    )

    std_returns = (
        df.group_by("Ticker")
        .agg(pl.col("Return").std().alias("Std_Return"))
        .to_pandas().set_index("Ticker")["Std_Return"]
    )

    sharpe_ratio = expected_returns / std_returns

    return {
        "expected_returns": expected_returns,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
    }
