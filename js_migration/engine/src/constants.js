// constants.js — calibration constants + enums, extracted verbatim from the AnyLogic model.
// Sources cited inline. Keep in sync with ../../tests/heat_model_ref.py (the shared spec).

// OL_HeatingSystem — ordinal order matches the AnyLogic enum / DB insert order.
export const HEATING_SYSTEMS = [
  'NATURAL_GAS_BOILER',
  'NATURAL_GAS_BLOCK',
  'HYBRID_HEAT_PUMP',
  'ELECTRIC_HEAT_PUMP',
  'DISTRICT_HEATING',
];

export const OWNERSHIPS = [
  'PRIVATELY_OWNED',
  'PRIVATELY_RENTED',
  'SOCIAL_HOUSING',
  'HOME_OWNER_ASSOCIATION',
];

// Decision weights — Startup_agent.f_setDefaultWeights()
export const WEIGHTS = {
  affordabilityToPBC: 0.5,
  effortToPBC: 0.2,
  attitudeToIntention: 0.5,
  PBCToIntention: 0.5,
  socialNormToIntention: 0.5,
  intentionToBehavior: 0.5,
  PBCToBehavior: 0.5,
};

// Startup_agent.f_initializeMain()
export const GUMBEL_SCALE_UTIL = 0.02;
export const GUMBEL_SCALE_EAC = 20;

// J_HeatingSystemOptionsGlobal salience S-curve constants
export const SALIENCE = { k: 10.0, threshold: 0.3, steepness: 30.0 };

// f_learningFactorToMultiplier(): LOW/MEDIUM/HIGH -> multiplier
export const LEARNING_MULTIPLIER = { LOW: 0.5, MEDIUM: 1.0, HIGH: 2.0 };

// Simulation experiment defaults
export const DEFAULT_START_YEAR = 2024;
export const DEFAULT_END_YEAR = 2050;
export const DEFAULT_SEED = 1;

// Energy-label letter -> number (A best). Used for insulation-requirement checks.
// (J_Dwelling.f_insulationLabelLetterToNumber — A=7 .. G=1)
export const LABEL_TO_NUMBER = { a: 7, b: 6, c: 5, d: 4, e: 3, f: 2, g: 1 };
