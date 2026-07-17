"""
heat_model_ref.py
-----------------
Executable specification of the core decision formulas, transcribed verbatim
from the AnyLogic Java source (J_HomeOwner, J_HeatingSystemOptionsGlobal, Main).

This is the SINGLE SOURCE OF TRUTH for the maths. The new engine (Java or JS)
must produce the same numbers as these functions. `test_heat_model_spec.py`
locks them with hand-verified values; keep both in sync with the Java.

Every function cites the Java method + file it came from. Constants that live in
the model are gathered in `CONST` with the line of code that sets them.
"""
from __future__ import annotations
import math

# --- Calibration constants (defaults from the AnyLogic model) -----------------
CONST = dict(
    # Startup_agent.f_setDefaultWeights()
    weight_affordabilityToPBC=0.5,
    weight_effortToPBC=0.2,
    weight_attitudeToIntention=0.5,
    weight_PBCToIntention=0.5,
    weight_socialNormToIntention=0.5,
    weight_intentionToBehavior=0.5,
    weight_PBCToBehavior=0.5,
    # Startup_agent.f_initializeMain()
    gumbelScaleUtil=0.02,
    gumbelScaleEAC=20.0,
    # J_HeatingSystemOptionsGlobal salience S-curve constants
    salience_k=10.0,          # steepness of novelty s-curve
    salience_threshold=0.3,   # share at which novelty is half strength
    salience_steepness=30.0,  # rising/falling distinction sharpness
)

# f_learningFactorToMultiplier() : LOW/MEDIUM/HIGH -> multiplier
LEARNING_MULTIPLIER = {"LOW": 0.5, "MEDIUM": 1.0, "HIGH": 2.0}


# --- Normalisation (Main.f_getNormalizedValue) --------------------------------
def normalized_value(value: float, mn: float, mx: float) -> float:
    """Main.f_getNormalizedValue -> (value - min) / (max - min).
    NOTE: no guard against max==min in the original; caller ensures min<max."""
    return (value - mn) / (mx - mn)


# --- Attitude (J_HomeOwner.f_getAttitudeValue) --------------------------------
def attitude_value(sustainability_attitude: float, sustainability_score_norm: float) -> float:
    """1 - |household_attitude - technology_sustainability_score_norm|."""
    return 1.0 - abs(sustainability_attitude - sustainability_score_norm)


# --- Effort (J_HomeOwner.f_getEffort) -----------------------------------------
def effort(type_is_current: bool, requires_low_temp: bool, dwelling_has_low_temp: bool) -> float:
    if type_is_current:
        return 0.2   # standard replacement, minimal effort
    if requires_low_temp and not dwelling_has_low_temp:
        return 0.8   # also needs in-house distribution adaptation
    return 0.5


# --- PBC (J_HomeOwner.f_getPBC) -----------------------------------------------
def pbc(eac_norm: float, effort_val: float,
        weight_eac: float = None, weight_effort: float = None) -> float:
    weight_eac = CONST["weight_affordabilityToPBC"] if weight_eac is None else weight_eac
    weight_effort = CONST["weight_effortToPBC"] if weight_effort is None else weight_effort
    eac_norm_reversed = 1.0 - eac_norm       # cheapest -> best
    effort_reversed = 1.0 - effort_val
    return (eac_norm_reversed * weight_eac + effort_reversed * weight_effort) / (weight_eac + weight_effort)


# --- Subjective norm (J_HomeOwner.f_getSubjectiveNorm) ------------------------
def subjective_norm(peer_count_for_type: int, network_size: int, salience_factor: float) -> float:
    share_peers = peer_count_for_type / network_size
    return min(1.0, share_peers * (1.0 + salience_factor))


# --- Intention (J_HomeOwner.f_getIntention) -----------------------------------
def intention(attitude_val: float, subjective_norm_val: float, pbc_val: float,
              social_learning_factor: float, per_tech_social_learning_rate: float,
              weight_att: float = None, weight_sn_base: float = None,
              weight_pbc_to_intention: float = None) -> float:
    """
    weight_SN passed into Java's f_getIntention already = socialLearningFactor * weight_socialNormToIntention.
    Numerator SN term additionally multiplies by the per-technology socialLearningRate;
    denominator uses weight_SN (NOT times the per-tech rate).
    """
    weight_att = CONST["weight_attitudeToIntention"] if weight_att is None else weight_att
    weight_sn_base = CONST["weight_socialNormToIntention"] if weight_sn_base is None else weight_sn_base
    weight_pbc_to_intention = CONST["weight_PBCToIntention"] if weight_pbc_to_intention is None else weight_pbc_to_intention
    weight_sn = social_learning_factor * weight_sn_base
    num = (attitude_val * weight_att
           + subjective_norm_val * weight_sn * per_tech_social_learning_rate
           + pbc_val * weight_pbc_to_intention)
    den = weight_att + weight_sn + weight_pbc_to_intention
    return num / den


# --- Perceived utility (J_HomeOwner.f_getPerceivedUtility) --------------------
def perceived_utility(intention_val: float, pbc_val: float,
                      weight_intention_to_behavior: float = None,
                      weight_pbc_to_behavior: float = None) -> float:
    wi = CONST["weight_intentionToBehavior"] if weight_intention_to_behavior is None else weight_intention_to_behavior
    wp = CONST["weight_PBCToBehavior"] if weight_pbc_to_behavior is None else weight_pbc_to_behavior
    return (intention_val * wi + pbc_val * wp) / (wi + wp)


# --- Salience factor (J_HeatingSystemOptionsGlobal.setSalienceFactor) ---------
def salience_factor(current_share: float, previous_share: float,
                    k: float = None, threshold: float = None, steepness: float = None) -> float:
    k = CONST["salience_k"] if k is None else k
    threshold = CONST["salience_threshold"] if threshold is None else threshold
    steepness = CONST["salience_steepness"] if steepness is None else steepness
    novelty = 1.0 / (1.0 + math.exp(k * (current_share - threshold)))
    decay = current_share
    delta = current_share - previous_share
    momentum = 1.0 / (1.0 + math.exp(-steepness * delta))
    return momentum * novelty + (1.0 - momentum) * decay


# --- Learning-curve capex (J_HeatingSystemOptionsGlobal.f_updateCapexFromLearningCurve)
def capex_learning_factor(units_installed: float, initial_units_installed: float,
                          economic_learning_rate: float, policy_multiplier: float) -> float:
    """Multiplier applied to initial capex. 1.0 when installs have not grown."""
    if initial_units_installed == 0:
        initial_units_installed = 1  # avoids NaN, matches Java guard
    if units_installed > initial_units_installed:
        updated_rate = economic_learning_rate * policy_multiplier
        doublings = math.log(units_installed / initial_units_installed) / math.log(2)
        return (1.0 - updated_rate) ** doublings
    return 1.0


# --- Triggers (J_HomeOwner) ---------------------------------------------------
def has_opportunity_trigger(age_years: int, lifetime_years: int, legacy_int_division: bool = False) -> bool:
    """
    Opportunity replacement trigger: heating system is late in life.

    CORRECT behaviour (default): `age / lifetime > 0.75` with real division ->
    fires once the system passes 75% of its lifetime, as the model intends.

    The ORIGINAL AnyLogic code used `age / HSLifetime` with BOTH operands `int`,
    i.e. Java integer division: 0 for age<lifetime, 1 for lifetime<=age<2*lifetime.
    That made the trigger fire only at age >= lifetime (a bug). Set
    `legacy_int_division=True` to reproduce the old model exactly (e.g. to
    validate a faithful port against the pre-fix golden metrics).
    """
    if legacy_int_division:
        return (age_years // lifetime_years) > 0.75      # original (buggy) behaviour
    return (age_years / lifetime_years) > 0.75           # corrected


def has_end_of_life_trigger(age_years: int, lifetime_years: int) -> bool:
    """End-of-life: heating system has reached its lifetime. (age >= lifetime)."""
    return age_years >= lifetime_years


# --- Gumbel / RUM stochastic choice (J_HomeOwner.getStochasticChoiceHO) -------
def gumbel_noise(u: float) -> float:
    """-log(-log(U)), U ~ Uniform(0,1). Java uses Math.random()."""
    return -math.log(-math.log(u))


def rum_score(perceived_utility_val: float, u: float, scale: float = None) -> float:
    scale = CONST["gumbelScaleUtil"] if scale is None else scale
    return perceived_utility_val + scale * gumbel_noise(u)
