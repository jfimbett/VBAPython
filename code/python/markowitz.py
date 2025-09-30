import numpy as np
from scipy import optimize as opt


def solve_markowitz(mu: np.ndarray, Sigma: np.ndarray, short=False):
    """Minimize variance with weights summing to 1; no-short by default."""
    n = len(mu)
    def var(w):
        return w @ Sigma @ w
    cons = ({'type': 'eq', 'fun': lambda w: np.sum(w)-1},)
    bounds = None if short else [(0, 1)]*n
    w0 = np.ones(n)/n
    res = opt.minimize(var, w0, constraints=cons, bounds=bounds)
    res.check = (res.success, res.message)
    return res


if __name__ == "__main__":
    mu = np.array([0.1, 0.12, 0.08])
    Sigma = np.array([[0.04,0.01,0.00],[0.01,0.05,0.02],[0.00,0.02,0.09]])
    print(solve_markowitz(mu, Sigma).x)
