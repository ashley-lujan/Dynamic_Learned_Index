"""
This file includes an implementation of a multi-level Recursive Model Index.
Initial drafts were produced with the assistance of ChatGPT (OpenAI), with
subsequent modifications, validation, and testing performed by the author.

AI use rationale:
- The algorithm derives entirely from published research and public open-source
  RMI implementations.
- AI assistance was appropriate for accelerating boilerplate and structural code.
- All logic has been checked for correctness and adapted to this project.
"""

import numpy as np
from sklearn.linear_model import LinearRegression
from bisect import bisect_left

class SimpleModel:
    """
    Wrapper around a regression model. Exposes fit/predict.
    By default uses sklearn LinearRegression (fast). You can replace with MLPRegressor, etc.
    """
    def __init__(self, model=None):
        self.model = model or LinearRegression()

    def fit(self, X, y):
        # X: 1D array-like of keys -> reshape to (-1,1)
        X = np.asarray(X).reshape(-1, 1)
        y = np.asarray(y).reshape(-1, 1)
        if len(X) == 0:
            # nothing to fit; keep default weights (predict returns 0)
            self.empty = True
        else:
            self.empty = False
            self.model.fit(X, y)

    def predict(self, x):
        x = np.asarray([x]).reshape(-1, 1)
        if getattr(self, "empty", False):
            return 0.0
        return float(self.model.predict(x)[0, 0])


class MultiLevelRMI:
    """
    Multi-level Recursive Model Index (RMI).
    levels: list of ints describing number of models per level, e.g. [1, 32, 256]
      - the first element should be 1 (single root model).
    """
    def __init__(self, levels, model_factory=SimpleModel):
        assert len(levels) >= 1 and levels[0] == 1, "levels must start with [1, ...]"
        self.levels = levels
        self.model_factory = model_factory
        # data structure: models[level][i] = model instance
        self.models = [[model_factory() for _ in range(n)] for n in levels]
        self.trained = False

    def _pos_to_target(self, pos, n):
        # normalize position into [0, n) so that parent prediction can be used to
        # route to next model by rounding.
        # We'll keep targets as absolute positions for regression, but use relative mapping to choose model index.
        return pos

    def fit(self, keys):
        """
        Fit RMI on sorted unique keys and implicit positions [0..N-1].
        keys: 1D sorted list/array of keys (must be sorted ascending).
        """
        keys = np.asarray(keys)
        assert keys.ndim == 1
        N = len(keys)
        positions = np.arange(N)

        # top-level fit
        # fit model[0][0] => map key -> position (absolute)
        self.N = N
        self.keys = keys  # keep copy for final local verification/search
        self.models[0][0].fit(keys, positions)

        # building children assignments for subsequent levels
        # For l from 1..L-1:
        # - For each key find which parent model routed it to
        # - For each child model, fit with keys assigned to it
        prev_assignments = np.zeros(N, dtype=int)  # start: everything assigned to model 0 at level 0
        for l in range(1, len(self.levels)):
            n_models = self.levels[l]
            child_models = self.models[l]
            # compute estimated position from parent-level model to route to one of n_models
            # use parent-level predictions to compute which child model index to route to.
            parent_models = self.models[l - 1]
            # For each key, get parent's prediction (as approx position)
            parent_preds = np.zeros(N, dtype=float)
            # If parent level has multiple models, use prev_assignments to choose the right parent model for each key
            for pm_i, pm in enumerate(parent_models):
                mask = (prev_assignments == pm_i)
                if mask.any():
                    parent_preds[mask] = np.array([pm.predict(k) for k in keys[mask]])
            # We map the parent's predicted absolute position to a child index between 0..n_models-1
            # Simple mapping: child_idx = floor(pred / N * n_models)
            # clamp to valid range
            child_idx = np.floor(parent_preds / max(1, (self.N - 1)) * n_models).astype(int)
            child_idx = np.clip(child_idx, 0, n_models - 1)

            # Now fit each child model on its assigned keys
            new_assignments = child_idx.copy()
            for cid in range(n_models):
                mask = (new_assignments == cid)
                assigned_keys = keys[mask]
                assigned_pos = positions[mask]
                child_models[cid].fit(assigned_keys, assigned_pos)
            prev_assignments = new_assignments

        self.trained = True

    def _predict_pos_and_error(self, key, level_route=None):
        """
        Predict position by walking the models down the hierarchy.
        Returns predicted_pos, and rough error bound (we'll use a small multiple of sqrt(N) if unknown).
        level_route (optional) collects model indices chosen at each level.
        """
        if not self.trained:
            raise RuntimeError("RMI not trained. Call fit(keys) first.")
        N = self.N
        pred = None
        assign = 0
        if level_route is None:
            level_route = []
        # level 0:
        pred = self.models[0][0].predict(key)
        level_route.append(0)
        # for levels >=1, route using mapping used in fit
        for l in range(1, len(self.levels)):
            n_models = self.levels[l]
            # map pred -> index
            idx = int(np.floor(pred / max(1, (N - 1)) * n_models))
            idx = max(0, min(n_models - 1, idx))
            level_route.append(idx)
            pred = self.models[l][idx].predict(key)
        # conservative error bound: you can also compute residuals from training to get tighter bound
        # we'll use sqrt(N) heuristic and also clip to N
        err = max(1, int(np.sqrt(max(1, N))))
        return int(round(pred)), err, level_route

    def lookup(self, key):
        """
        Lookup a key: predict position then search locally around predicted position.
        Returns index if found, else -1.
        """
        pred_pos, err, route = self._predict_pos_and_error(key)
        # define window:
        lo = max(0, pred_pos - err)
        hi = min(self.N - 1, pred_pos + err)
        # use bisect on the stored keys to find exact location but limited to window:
        # Python bisect works on entire array; we'll narrow by slicing indices to window
        # find leftmost index in [lo, hi] with key >= target
        # use numpy searchsorted for speed
        i = np.searchsorted(self.keys[lo:hi+1], key, side='left')
        if i < 0 or i > (hi - lo):
            return -1
        found_index = lo + i
        if found_index <= hi and found_index < self.N and self.keys[found_index] == key:
            return found_index
        return -1

# Example / quick test:
if __name__ == "__main__":
    # generate an example skewed dataset
    N = 10000
    # keys drawn from skewed distribution (simulate real-world)
    keys = np.sort((np.random.exponential(1.0, size=N) * 1e6).astype(np.int64))
    # unique and sorted
    keys = np.unique(keys)
    rmi = MultiLevelRMI(levels=[1, 16, 128])  # small example
    rmi.fit(keys)
    # test lookups
    import random
    for t in range(10):
        k = random.choice(keys)
        idx = rmi.lookup(k)
        assert idx != -1 and keys[idx] == k
    print("Sanity checks passed; example lookups succeeded.")
