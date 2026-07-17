"""
test_heat_model_spec.py
-----------------------
Executable specification of the heat-transition decision core. Every expected
value is hand-computed from the extracted Java formulas (see heat_model_ref.py
for the source citation of each). These tests are language-agnostic intent: the
new engine's equivalent functions must reproduce the same numbers.

Run standalone (no dependencies):
    python test_heat_model_spec.py
Or with pytest:
    pytest test_heat_model_spec.py
"""
import math
import heat_model_ref as M

TOL = 1e-9


def approx(a, b, tol=TOL):
    assert abs(a - b) <= tol, f"expected {b}, got {a} (|Δ|={abs(a-b):.3e} > {tol:.0e})"


# ----------------------------------------------------------------------------
# Attitude
# ----------------------------------------------------------------------------
def test_attitude_value():
    # 1 - |0.7 - 0.9| = 0.8
    approx(M.attitude_value(0.7, 0.9), 0.8)
    # perfect match -> 1.0
    approx(M.attitude_value(0.42, 0.42), 1.0)
    # opposite ends -> 0.0
    approx(M.attitude_value(0.0, 1.0), 0.0)


# ----------------------------------------------------------------------------
# Effort
# ----------------------------------------------------------------------------
def test_effort():
    approx(M.effort(type_is_current=True, requires_low_temp=True, dwelling_has_low_temp=False), 0.2)
    approx(M.effort(type_is_current=False, requires_low_temp=True, dwelling_has_low_temp=False), 0.8)
    approx(M.effort(type_is_current=False, requires_low_temp=True, dwelling_has_low_temp=True), 0.5)
    approx(M.effort(type_is_current=False, requires_low_temp=False, dwelling_has_low_temp=False), 0.5)


# ----------------------------------------------------------------------------
# Normalisation
# ----------------------------------------------------------------------------
def test_normalized_value():
    approx(M.normalized_value(1500, 1000, 2000), 0.5)
    approx(M.normalized_value(1000, 1000, 2000), 0.0)
    approx(M.normalized_value(2000, 1000, 2000), 1.0)


# ----------------------------------------------------------------------------
# PBC  (weights: affordability=0.5, effort=0.2)
# ----------------------------------------------------------------------------
def test_pbc():
    # eac_norm=0.25, effort=0.5
    # eac_rev=0.75, effort_rev=0.5
    # (0.75*0.5 + 0.5*0.2) / (0.5+0.2) = (0.375+0.1)/0.7 = 0.6785714285714286
    approx(M.pbc(0.25, 0.5), 0.475 / 0.7)
    # cheapest + lowest effort -> best PBC = 1.0
    approx(M.pbc(0.0, 0.0), 1.0)
    # most expensive + highest effort -> 0.0
    approx(M.pbc(1.0, 1.0), 0.0)


# ----------------------------------------------------------------------------
# Subjective norm (salience-amplified peer share, capped at 1.0)
# ----------------------------------------------------------------------------
def test_subjective_norm():
    # 40% peers, salience 0.5 -> 0.4*1.5 = 0.6
    approx(M.subjective_norm(4, 10, 0.5), 0.6)
    # cap at 1.0: 80% peers, salience 1.0 -> 1.6 -> capped
    approx(M.subjective_norm(8, 10, 1.0), 1.0)
    # zero salience -> raw peer share
    approx(M.subjective_norm(3, 10, 0.0), 0.3)


# ----------------------------------------------------------------------------
# Intention  (weights att=0.5, SN_base=0.5, PBC->intention=0.5)
# ----------------------------------------------------------------------------
def test_intention_medium_slf():
    # att=0.8, SN=0.6, PBC=0.6785714..., SLF(medium)=1.0, per-tech rate=0.3
    # weight_SN = 1.0*0.5 = 0.5
    # num = 0.8*0.5 + 0.6*0.5*0.3 + 0.6785714*0.5 = 0.4 + 0.09 + 0.3392857 = 0.8292857
    # den = 0.5+0.5+0.5 = 1.5
    pbc_val = 0.475 / 0.7
    expected = (0.8 * 0.5 + 0.6 * 0.5 * 0.3 + pbc_val * 0.5) / 1.5
    approx(M.intention(0.8, 0.6, pbc_val, social_learning_factor=1.0,
                       per_tech_social_learning_rate=0.3), expected)


def test_intention_high_slf_raises_weight_and_denominator():
    # SLF high (2.0): weight_SN = 2.0*0.5 = 1.0; denominator grows to 2.0
    pbc_val = 0.5
    num = 0.8 * 0.5 + 0.6 * 1.0 * 0.3 + pbc_val * 0.5
    den = 0.5 + 1.0 + 0.5
    approx(M.intention(0.8, 0.6, pbc_val, social_learning_factor=2.0,
                       per_tech_social_learning_rate=0.3), num / den)


# ----------------------------------------------------------------------------
# Perceived utility  (weights intention=0.5, PBC->behavior=0.5 -> simple mean)
# ----------------------------------------------------------------------------
def test_perceived_utility():
    approx(M.perceived_utility(0.5528571, 0.6785714),
           (0.5528571 + 0.6785714) / 2)
    approx(M.perceived_utility(0.4, 0.8), 0.6)


# ----------------------------------------------------------------------------
# Salience factor  (k=10, threshold=0.3, steepness=30)
# ----------------------------------------------------------------------------
def test_salience_rising_low_share():
    # current=0.1, prev=0.05
    novelty = 1.0 / (1.0 + math.exp(10 * (0.1 - 0.3)))
    decay = 0.1
    momentum = 1.0 / (1.0 + math.exp(-30 * 0.05))
    expected = momentum * novelty + (1 - momentum) * decay
    approx(M.salience_factor(0.1, 0.05), expected)


def test_salience_flat_is_half_blend():
    # delta=0 -> momentum=0.5 exactly
    novelty = 1.0 / (1.0 + math.exp(10 * (0.2 - 0.3)))
    expected = 0.5 * novelty + 0.5 * 0.2
    approx(M.salience_factor(0.2, 0.2), expected)


def test_salience_high_share_low_novelty():
    # well above threshold -> novelty small, factor -> ~ decay (share)
    s = M.salience_factor(0.9, 0.9)
    assert 0.0 <= s <= 1.0
    # at flat 0.9: novelty ~ 1/(1+e^6) ~ 0.0025; factor = 0.5*0.0025 + 0.5*0.9
    approx(s, 0.5 * (1 / (1 + math.exp(6))) + 0.5 * 0.9)


# ----------------------------------------------------------------------------
# Learning curve  capex factor = (1 - rate*policy) ^ doublings
# ----------------------------------------------------------------------------
def test_learning_curve_two_doublings():
    # 1000 -> 4000 = 2 doublings, rate 0.1, policy medium(1.0) -> 0.9^2 = 0.81
    approx(M.capex_learning_factor(4000, 1000, 0.1, 1.0), 0.81)


def test_learning_curve_policy_multiplier_high():
    # policy HIGH(2.0): effective rate 0.2 -> 0.8^2 = 0.64
    approx(M.capex_learning_factor(4000, 1000, 0.1, 2.0), 0.64)


def test_learning_curve_no_growth_is_unity():
    approx(M.capex_learning_factor(1000, 1000, 0.1, 1.0), 1.0)
    approx(M.capex_learning_factor(500, 1000, 0.1, 1.0), 1.0)


def test_learning_multiplier_map():
    assert M.LEARNING_MULTIPLIER == {"LOW": 0.5, "MEDIUM": 1.0, "HIGH": 2.0}


# ----------------------------------------------------------------------------
# Opportunity trigger  -- CRITICAL Java integer-division semantics
# ----------------------------------------------------------------------------
def test_opportunity_trigger_corrected_float_division():
    # CORRECTED (default): fires once past 75% of lifetime.
    lifetime = 15
    assert M.has_opportunity_trigger(11, lifetime) is False   # 11/15 = 0.733 < 0.75
    assert M.has_opportunity_trigger(12, lifetime) is True    # 12/15 = 0.80  > 0.75
    assert M.has_opportunity_trigger(14, lifetime) is True
    assert M.has_opportunity_trigger(15, lifetime) is True


def test_opportunity_trigger_legacy_int_division():
    # LEGACY (original AnyLogic bug): integer division -> only fires at age >= lifetime.
    lifetime = 15
    assert M.has_opportunity_trigger(11, lifetime, legacy_int_division=True) is False
    assert M.has_opportunity_trigger(14, lifetime, legacy_int_division=True) is False  # 93% but still False!
    assert M.has_opportunity_trigger(15, lifetime, legacy_int_division=True) is True
    assert M.has_opportunity_trigger(20, lifetime, legacy_int_division=True) is True


def test_end_of_life_trigger():
    assert M.has_end_of_life_trigger(14, 15) is False
    assert M.has_end_of_life_trigger(15, 15) is True


# ----------------------------------------------------------------------------
# Gumbel / RUM stochastic choice
# ----------------------------------------------------------------------------
def test_gumbel_noise_median_is_zero_ish():
    # median of Gumbel(0,1) = -ln(ln2) ~ 0.3665; at U=e^{-1} noise=0
    approx(M.gumbel_noise(math.exp(-1)), 0.0)


def test_rum_score_small_scale_preserves_ranking():
    # with scale 0.02, a 0.1 utility gap almost always dominates gumbel noise
    # deterministic check: equal U -> higher utility keeps higher score
    u = 0.5
    s_hi = M.rum_score(0.70, u)
    s_lo = M.rum_score(0.60, u)
    assert s_hi > s_lo


def test_rum_choice_distribution_is_logit_like():
    # Statistical: over many draws, P(choose A over B) increases with utility gap.
    import random
    rng = random.Random(12345)
    scale = M.CONST["gumbelScaleUtil"]

    def prob_a(ua, ub, n=20000):
        wins = 0
        for _ in range(n):
            sa = ua + scale * M.gumbel_noise(rng.random())
            sb = ub + scale * M.gumbel_noise(rng.random())
            wins += sa > sb
        return wins / n

    p_small = prob_a(0.61, 0.60)   # tiny gap ~ near 0.5..high depending on scale
    p_big = prob_a(0.80, 0.60)     # big gap -> near 1.0
    assert 0.5 <= p_small <= 1.0
    assert p_big > p_small
    assert p_big > 0.99            # 0.2 gap >> 0.02 scale -> almost deterministic


# ----------------------------------------------------------------------------
# End-to-end: single household utility for one option (composition test)
# ----------------------------------------------------------------------------
def test_full_utility_pipeline():
    # A concrete option evaluation, composing every step.
    sustainability_attitude = 0.7
    score_norm = 0.9
    eac_norm = 0.25
    eff = M.effort(False, False, False)          # 0.5
    att = M.attitude_value(sustainability_attitude, score_norm)   # 0.8
    p = M.pbc(eac_norm, eff)                      # 0.6785714...
    sn = M.subjective_norm(4, 10, 0.5)            # 0.6
    intent = M.intention(att, sn, p, social_learning_factor=1.0,
                         per_tech_social_learning_rate=0.3)
    util = M.perceived_utility(intent, p)
    # hand-computed final value
    expected_intent = (0.8 * 0.5 + 0.6 * 0.5 * 0.3 + (0.475 / 0.7) * 0.5) / 1.5
    expected_util = (expected_intent + 0.475 / 0.7) / 2
    approx(util, expected_util)
    assert 0.0 <= util <= 1.0


# ----------------------------------------------------------------------------
# Minimal runner
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn(); passed += 1
            print(f"  PASS  {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL  {name}: {e}")
    print(f"\n{passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
