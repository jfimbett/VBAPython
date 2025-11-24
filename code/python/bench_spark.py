"""PySpark benchmark for the Big Data dataset.

Requires:
    pip install pyspark
    # and Java (e.g. conda install -c conda-forge openjdk=17)
"""

from big_data_utils import get_data_path

import functools
import math
import statistics
import timeit

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


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
def run_spark(n_companies: int, time_periods: int):
    path_to_data = get_data_path(n_companies, time_periods)
    spark = SparkSession.builder.appName("BigDataFinance").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(path_to_data, header=True, inferSchema=True)

    expected_returns = df.groupBy("Ticker").agg(F.mean("Return").alias("Expected_Return"))

    window_spec = (
        Window.partitionBy("Ticker")
        .orderBy("Date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    df = df.withColumn(
        "Cumulative_Return",
        F.exp(F.sum(F.log(1 + df["Return"])).over(window_spec)) - 1,
    )
    df = df.withColumn("HWM", F.max("Cumulative_Return").over(window_spec))
    df = df.withColumn("Drawdown", df["Cumulative_Return"] / df["HWM"] - 1)

    max_drawdown = df.groupBy("Ticker").agg(F.min("Drawdown").alias("Max_Drawdown"))

    stddev_returns = df.groupBy("Ticker").agg(F.stddev("Return").alias("StdDev_Return"))
    sharpe_ratio = expected_returns.join(
        stddev_returns, "Ticker"
    ).withColumn(
        "Sharpe_Ratio",
        expected_returns["Expected_Return"] / stddev_returns["StdDev_Return"],
    )

    spark.stop()

    return {
        "expected_returns": expected_returns,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
    }
