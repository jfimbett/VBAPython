"""Shared utilities for Big Data benchmarks (data generation, paths)."""

import os
from typing import Tuple

import numpy as np
import pandas as pd


def get_data_path(n_companies: int, time_periods: int) -> str:
    return f"../../data/large_financial_data_{n_companies}x{time_periods}.csv"


def generate_dataset(n_companies: int, time_periods: int) -> str:
    """Generate the dataset if it does not exist and return its path."""
    path_to_data = get_data_path(n_companies, time_periods)

    if not os.path.exists(path_to_data):
        print(f"Creating dataset: {path_to_data}")

        import string

        def generate_random_tickers(n: int, length: int = 4, seed: int = 0):
            rng = np.random.default_rng(seed)
            letters = np.array(list(string.ascii_uppercase))
            codes = rng.integers(0, 26, size=(n, length), dtype=np.uint8)
            bases = ["".join(letters[row]) for row in codes]
            suffixes = np.arange(n).astype(str)
            tickers_np = np.char.add(np.array(bases, dtype=object), np.char.add("_", suffixes))
            return tickers_np.tolist()

        tickers = generate_random_tickers(n_companies, length=4, seed=0)

        np.random.seed(0)
        random_matrix = np.random.rand(n_companies, n_companies)
        cov_matrix = np.dot(random_matrix, random_matrix.T)

        assert np.all(np.linalg.eigvals(cov_matrix) >= 0), "Covariance matrix is not positive semi-definite"

        cross_sectional_mu = 0.001  # mean return across companies
        time_series_sigma = 0.05  # std dev of returns over time

        scaled_cov_matrix = cov_matrix * (time_series_sigma ** 2)

        returns = np.random.multivariate_normal(
            mean=[cross_sectional_mu] * n_companies,
            cov=scaled_cov_matrix,
            size=time_periods,
        )

        date_range = pd.date_range(start="2020-01-01", periods=time_periods, freq="D")
        df_panel = (
            pd.DataFrame(returns, index=date_range, columns=tickers)
            .rename(columns=lambda c: f"TICKER_{c}")
            .stack()
            .reset_index(name="Return")
            .rename(columns={"level_0": "Date", "level_1": "Ticker"})
        )
        df_panel["Ticker"] = df_panel["Ticker"].astype("category")
        df_panel.to_csv(path_to_data, index=False)
        print("Dataset created successfully!")
    else:
        print(f"Dataset already exists: {path_to_data}")

    return path_to_data
