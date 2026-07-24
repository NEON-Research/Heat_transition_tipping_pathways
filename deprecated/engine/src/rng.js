// rng.js — deterministic, seeded random number generator for reproducible runs.
//
// NOTE ON FIDELITY: the original AnyLogic model used Java's Random (seed=1) for
// beta-distributed attitudes AND java Math.random() (unseeded!) for the Gumbel
// choice. We deliberately route BOTH through one seeded stream here so stochastic
// runs are fully reproducible — an intended improvement over the original. This
// changes exact numbers but not distributions. Use the same seed to reproduce a run.

export class RNG {
  constructor(seed = 1) {
    // mulberry32 — small, fast, good enough for ABM sampling.
    this._s = (seed >>> 0) || 1;
  }
  // Uniform [0,1)
  next() {
    let t = (this._s += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }
  // Uniform integer in [lo, hi)
  int(lo, hi) { return lo + Math.floor(this.next() * (hi - lo)); }

  // Gamma(shape>=1, 1) via Marsaglia-Tsang; shape<1 handled by boosting.
  gamma(shape) {
    if (shape < 1) {
      const u = this.next();
      return this.gamma(shape + 1) * Math.pow(u, 1 / shape);
    }
    const d = shape - 1 / 3;
    const c = 1 / Math.sqrt(9 * d);
    for (;;) {
      let x, v;
      do {
        x = this._normal();
        v = 1 + c * x;
      } while (v <= 0);
      v = v * v * v;
      const u = this.next();
      if (u < 1 - 0.0331 * x * x * x * x) return d * v;
      if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v;
    }
  }
  _normal() {
    // Box-Muller
    let u = 0, v = 0;
    while (u === 0) u = this.next();
    while (v === 0) v = this.next();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  }
  // Beta(alpha, beta) on [min,max]. Model uses beta(5,2,0,1) for attitudes.
  beta(alpha, betaParam, min = 0, max = 1) {
    const x = this.gamma(alpha);
    const y = this.gamma(betaParam);
    return min + (max - min) * (x / (x + y));
  }
  // Gumbel noise -log(-log(U)) — for RUM stochastic choice.
  gumbel() { return -Math.log(-Math.log(this.next())); }
}
