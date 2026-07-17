// decision.js — pure decision functions, transcribed from J_HomeOwner /
// J_HeatingSystemOptionsGlobal. Mirrors ../../tests/heat_model_ref.py exactly.
// No side effects, no state — trivially unit-testable.

import { WEIGHTS, GUMBEL_SCALE_UTIL, SALIENCE } from './constants.js';

export const normalizedValue = (v, mn, mx) => (v - mn) / (mx - mn);

// attitude = 1 - |householdAttitude - technologySustainabilityScoreNorm|
export const attitudeValue = (att, scoreNorm) => 1 - Math.abs(att - scoreNorm);

// effort: 0.2 same system | 0.8 needs low-temp retrofit | 0.5 otherwise
export function effort(typeIsCurrent, requiresLowTemp, dwellingHasLowTemp) {
  if (typeIsCurrent) return 0.2;
  if (requiresLowTemp && !dwellingHasLowTemp) return 0.8;
  return 0.5;
}

// PBC = weighted mean of (1-EAC_norm) and (1-effort)
export function pbc(eacNorm, effortVal,
                    wEac = WEIGHTS.affordabilityToPBC, wEffort = WEIGHTS.effortToPBC) {
  return ((1 - eacNorm) * wEac + (1 - effortVal) * wEffort) / (wEac + wEffort);
}

// subjectiveNorm = min(1, peerShare * (1 + salience))
export const subjectiveNorm = (peerCount, networkSize, salience) =>
  Math.min(1, (peerCount / networkSize) * (1 + salience));

// intention; weightSN = socialLearningFactor * weight_socialNormToIntention (in denominator);
// per-technology socialLearningRate multiplies only the numerator SN term.
export function intention(att, sn, pbcVal, socialLearningFactor, perTechRate,
                          wAtt = WEIGHTS.attitudeToIntention,
                          wSnBase = WEIGHTS.socialNormToIntention,
                          wPbcToInt = WEIGHTS.PBCToIntention) {
  const wSn = socialLearningFactor * wSnBase;
  const num = att * wAtt + sn * wSn * perTechRate + pbcVal * wPbcToInt;
  return num / (wAtt + wSn + wPbcToInt);
}

// perceived utility = weighted mean of intention and PBC
export function perceivedUtility(intentionVal, pbcVal,
                                 wInt = WEIGHTS.intentionToBehavior,
                                 wPbc = WEIGHTS.PBCToBehavior) {
  return (intentionVal * wInt + pbcVal * wPbc) / (wInt + wPbc);
}

// salience factor: novelty S-curve blended with decay by momentum
export function salienceFactor(currentShare, previousShare,
                               k = SALIENCE.k, threshold = SALIENCE.threshold,
                               steepness = SALIENCE.steepness) {
  const novelty = 1 / (1 + Math.exp(k * (currentShare - threshold)));
  const momentum = 1 / (1 + Math.exp(-steepness * (currentShare - previousShare)));
  return momentum * novelty + (1 - momentum) * currentShare;
}

// learning-curve capex multiplier = (1 - rate*policy)^doublings, 1.0 if not growing
export function capexLearningFactor(units, initialUnits, learningRate, policyMultiplier) {
  if (initialUnits === 0) initialUnits = 1;
  if (units > initialUnits) {
    const rate = learningRate * policyMultiplier;
    const doublings = Math.log(units / initialUnits) / Math.log(2);
    return Math.pow(1 - rate, doublings);
  }
  return 1.0;
}

// --- Triggers ---------------------------------------------------------------
export const hasEndOfLifeTrigger = (age, lifetime) => age >= lifetime;

// CORRECTED opportunity trigger: real division -> fires past 75% of life.
// legacyIntDivision=true reproduces the original AnyLogic integer-division bug
// (only fired at age>=lifetime) for validating a faithful port vs the old golden.
export function hasOpportunityTrigger(age, lifetime, legacyIntDivision = false) {
  if (legacyIntDivision) return Math.trunc(age / lifetime) > 0.75;
  return age / lifetime > 0.75;
}

// RUM score = utility + scale * Gumbel noise (choose argmax over feasible options)
export const rumScore = (utility, gumbelNoise, scale = GUMBEL_SCALE_UTIL) =>
  utility + scale * gumbelNoise;
