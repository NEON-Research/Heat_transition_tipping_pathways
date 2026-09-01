#!/usr/bin/env python3
"""Shared GLUE weighting primitives.

Kept in its own module so that overview_figures.py and glue_weighting.py use ONE implementation
without importing each other (which would be circular). See glue_weighting.py for the rationale
behind the likelihood measure and the choice of N.

    L_i = max(0, 1 - MAD_i / MAD_ref) ** N        w_i = L_i / sum(L)

MAD_ref is the MAD scored by the original AnyLogic default weights, so L=1 is a perfect fit and
L=0 is "no better than the uncalibrated model". N is chosen by the analyst, not estimated.
"""
import glob, json, os
import numpy as np

DEFAULT_N = 1.0


def likelihood(mad, mad_ref, n):
    """Informal GLUE likelihood. n=0 returns equal weights."""
    mad = np.asarray(mad, float)
    if n == 0:
        return np.ones_like(mad)
    return np.clip(1.0 - mad / mad_ref, 0.0, None) ** n


def ess(w):
    """Kish effective sample size: how many EQUALLY weighted sets the ensemble is worth."""
    w = np.asarray(w, float)
    return float(w.sum() ** 2 / (w ** 2).sum())


def wquantile(v, w, q):
    """Weighted quantile, Hazen plotting position (i-0.5)/n -- quantile "type 5".

    Under equal weights this equals np.percentile(..., method="hazen"), NOT the numpy default
    "linear" (type 7). With n=21 the two differ only in the tails: type 7 pins the 5th percentile
    to the second-smallest value, type 5 interpolates past it. Type 5 is used throughout so that
    weighted and equal-weighted bands are computed by the same estimator and are comparable.
    """
    v = np.asarray(v, float); w = np.asarray(w, float)
    i = np.argsort(v); v, w = v[i], w[i]
    c = (np.cumsum(w) - 0.5 * w) / w.sum()
    return np.interp(q, c, v)


def wmean(v, w):
    v = np.asarray(v, float); w = np.asarray(w, float)
    return float((v * w).sum() / w.sum())


def set_weights(root, scope_tag, n=DEFAULT_N, paths=None):
    """-> (weights, info) aligned to the sorted set*/ run folders under results/<scope>/calib.

    Returns equal weights and an explanatory note if the run folders cannot be matched to the
    calibration search (e.g. when the caller globbed representative folders such as best_fit/,
    which have no MAD and are not part of the retained ensemble).
    """
    if paths is None:
        paths = sorted(glob.glob(os.path.join(root, "results", scope_tag, "calib", "set*",
                                              "simulation_results.csv")))
    equal = np.ones(len(paths)) / max(len(paths), 1)
    sj = os.path.join(root, "results", "calib", "calibration_search.json")
    if not os.path.exists(sj):
        return equal, {"n": 0, "ess": len(paths), "note": "no calibration_search.json; equal weights"}
    d = json.load(open(sj))
    ret = sorted(d["retained"], key=lambda s: s["mad"])
    if len(paths) != len(ret):
        return equal, {"n": 0, "ess": len(paths),
                       "note": f"{len(paths)} run folders vs {len(ret)} retained sets; equal weights"}
    mad = []
    for p, r in zip(paths, ret):
        tag = os.path.basename(os.path.dirname(p))
        if "mad" not in tag or abs(float(tag.split("mad")[1]) - r["mad"]) > 0.006:
            return equal, {"n": 0, "ess": len(paths),
                           "note": f"{tag} does not match retained MAD; equal weights"}
        mad.append(r["mad"])
    mad = np.array(mad)
    L = likelihood(mad, d["default_mad"], n)
    if L.sum() <= 0:
        return equal, {"n": 0, "ess": len(paths), "note": "all likelihoods zero; equal weights"}
    w = L / L.sum()
    return w, {"n": n, "ess": ess(w), "mad": mad, "mad_ref": d["default_mad"],
               "note": (f"GLUE N={n:g}, ESS {ess(w):.1f}/{len(w)}" if n else
                        f"equal weights ({len(w)} sets)")}


def _selftest():
    rng = np.random.default_rng(0)
    for n in (5, 21, 100):
        v = rng.normal(size=n); w = np.ones(n)
        for q in (0.05, 0.25, 0.5, 0.75, 0.95):
            assert abs(wquantile(v, w, q) - np.percentile(v, 100 * q, method="hazen")) < 1e-9
    v = rng.normal(size=21)
    assert abs(wquantile(v, np.ones(21), .3) - wquantile(v, np.full(21, 7.0), .3)) < 1e-12
    ww = rng.uniform(0.01, 1, 21)
    qs = [wquantile(v, ww, x) for x in np.linspace(0.01, 0.99, 40)]
    assert all(b >= a - 1e-12 for a, b in zip(qs, qs[1:]))
    assert v.min() - 1e-12 <= min(qs) and max(qs) <= v.max() + 1e-12
    w1 = np.zeros(21); w1[7] = 1.0
    assert abs(wquantile(v, w1, .5) - v[7]) < 1e-12
    assert abs(ess(np.ones(21) / 21) - 21) < 1e-9
    assert abs(wmean(v, np.ones(21)) - v.mean()) < 1e-12
    return "glue selftest OK (hazen match, scale-free, monotone, bounded, degenerate, ESS, wmean)"


if __name__ == "__main__":
    print(_selftest())
