#%%
import math
from scipy import optimize as opt

f = lambda x: x**3 - 1 
root = opt.brentq(f, 0, 2)
# %%
# plot f 
import numpy as np
import matplotlib.pyplot as plt
x = np.linspace(-2, 4, 100)
y = np.array([f(xi) for xi in x]    )
plt.plot(x, y)
plt.axhline(0, color='black', lw=0.5)
plt.axvline(root, color='red', ls='--', label=f'Root at x={root:.2f}')
plt.legend()
plt.title('Finding root of f(x) = x*cos(x) - 1')
# %%
import numpy as np
from scipy import optimize as opt

def F(v):
  x, y = v
  return [x**2 + y**2 - 1, x - y]
sol = opt.root(F, [0.5, 0.1])
# %%
from scipy import optimize as opt
quad = lambda x: (x-3)**2
res = opt.minimize(lambda v: quad(v[0]), x0=[0.0])
# %%
# Create a tiny two‑ticker daily prices CSV for demonstration
import pandas as pd, numpy as np
from scipy import optimize as opt

# use returns from prices-top20.csv to estimate mu and Sigma
# px = pd.read_csv('data/prices-top20.csv', parse_dates=['date']).sort_values(['symbol','date'])



ret = px.groupby('symbol')['close'].pct_change()
R = px.assign(ret=ret).pivot(index='date', columns='symbol', values='ret').dropna()
mu = R.mean().values
Sigma = np.cov(R.values, rowvar=False)

# min variance portfolio with sum(w)=1 and w>=0
N = len(mu)
var = lambda w: w @ Sigma @ w
cons = ({'type': 'eq', 'fun': lambda w: np.sum(w)-1}, {'type':'ineq','fun': lambda w: w})
res = opt.minimize(var, x0=np.ones(N)/N, constraints=cons, bounds=[(0,1)]*N)
weights = pd.Series(res.x, index=R.columns).sort_values(ascending=False)
weights.head()