package heattransition;

/** Deterministic seeded RNG for reproducible runs (mirrors the JS engine's rng.js).
 *  NOTE: the original AnyLogic model used Java's Random (seed=1) for beta attitudes and
 *  the UNSEEDED Math.random() for the Gumbel choice. We route both through one seeded
 *  stream here so stochastic runs are fully reproducible — an intended improvement.
 *  We use the SAME mulberry32 algorithm as the JS engine, so the two produce
 *  statistically equivalent streams (not bit-identical: JS accumulates state as a
 *  double, Java wraps at 2^32). The golden-master tests are statistical, so this is fine. */
public final class Rng {
    private int state;

    public Rng(long seed) {
        this.state = (int) (seed & 0xffffffffL);
        if (this.state == 0) this.state = 1;
    }

    /** Uniform [0,1) — mulberry32. */
    public double next() {
        state += 0x6d2b79f5;
        int t = state;
        t = (t ^ (t >>> 15)) * (t | 1);
        t ^= t + (t ^ (t >>> 7)) * (t | 61);
        long u = (t ^ (t >>> 14)) & 0xffffffffL;
        return u / 4294967296.0;
    }

    /** Uniform integer in [lo, hi). */
    public int nextInt(int lo, int hi) { return lo + (int) Math.floor(next() * (hi - lo)); }

    private double normal() { // Box-Muller
        double u = 0, v = 0;
        while (u == 0) u = next();
        while (v == 0) v = next();
        return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
    }

    /** Gamma(shape, 1) — Marsaglia-Tsang; shape<1 boosted. */
    public double gamma(double shape) {
        if (shape < 1) {
            double u = next();
            return gamma(shape + 1) * Math.pow(u, 1.0 / shape);
        }
        double d = shape - 1.0 / 3.0;
        double c = 1.0 / Math.sqrt(9 * d);
        while (true) {
            double x, v;
            do { x = normal(); v = 1 + c * x; } while (v <= 0);
            v = v * v * v;
            double u = next();
            if (u < 1 - 0.0331 * x * x * x * x) return d * v;
            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v;
        }
    }

    /** Beta(alpha, beta) on [min,max]. Attitudes use beta(5,2,0,1). */
    public double beta(double alpha, double betaParam, double min, double max) {
        double x = gamma(alpha), y = gamma(betaParam);
        return min + (max - min) * (x / (x + y));
    }

    /** Gumbel noise -log(-log(U)) for the RUM choice. */
    public double gumbel() { return -Math.log(-Math.log(next())); }

    /** Standard normal via Box-Muller. */
    public double gaussian() {
        double u = Math.max(1e-12, next()), v = next();
        return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
    }
    /** Equipment lifetime ~ round(N(base, sd)) clamped to [base-maxDev, base+maxDev].
     *  sd<=0 or maxDev<=0 -> deterministic base (AnyLogic-faithful). */
    public int jitteredLifetime(int base, double sd, int maxDev) {
        if (sd <= 0 || maxDev <= 0) return base;
        long v = Math.round(base + sd * gaussian());
        return (int) Math.max(base - maxDev, Math.min(base + maxDev, v));
    }
}
