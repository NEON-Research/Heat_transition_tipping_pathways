// heating_systems.js — REAL values exported from the AnyLogic HEATING_SYSTEM_DATA
// and ENERGY_SOURCE_DATA tables (database/db.script). Costs in EUR, demand in kWh.
//
// requiresLowTemp = (REQUIRED_DISTRIBUTION_SYSTEM === 'LT').
// energy source domestic costs (EUR/kWh): NATURAL_GAS 0.14, ELECTRICITY 0.32, HEAT 0.14.

const E = { NATURAL_GAS: 0.14, ELECTRICITY: 0.32, HEAT: 0.14, NOT_APPLICABLE: 0 };

export const HEATING_SYSTEM_DATA = {
  NATURAL_GAS_BOILER: {
    type: 'NATURAL_GAS_BOILER',
    investSmall: 1500, investMedium: 2250, investLarge: 3000,
    distributionSystemCost: 0, requiresLowTemp: false, requiredLabel: 'no',
    maintenance: 0, lifetime: 12, discountRate: 0.02, economicLearningRate: 0.01,
    primarySource: 'NATURAL_GAS', secondarySource: 'NOT_APPLICABLE',
    efficiencyPrimary: 0.99, efficiencySecondary: 0, fractionPrimary: 1.0, fractionSecondary: 0,
    subsidy: 0, sustainabilityScore: 1, socialLearningRate: 0.5,
    primaryCostPerKWh: E.NATURAL_GAS, secondaryCostPerKWh: 0,
  },
  NATURAL_GAS_BLOCK: {
    type: 'NATURAL_GAS_BLOCK',
    investSmall: 1500, investMedium: 2250, investLarge: 3000,
    distributionSystemCost: 0, requiresLowTemp: false, requiredLabel: 'no',
    maintenance: 0, lifetime: 12, discountRate: 0.02, economicLearningRate: 0.01,
    primarySource: 'NATURAL_GAS', secondarySource: 'NOT_APPLICABLE',
    efficiencyPrimary: 0.99, efficiencySecondary: 0, fractionPrimary: 1.0, fractionSecondary: 0,
    subsidy: 0, sustainabilityScore: 1, socialLearningRate: 0.5,
    primaryCostPerKWh: E.NATURAL_GAS, secondaryCostPerKWh: 0,
  },
  HYBRID_HEAT_PUMP: {
    type: 'HYBRID_HEAT_PUMP',
    investSmall: 5000, investMedium: 6000, investLarge: 7000,
    distributionSystemCost: 5000, requiresLowTemp: true, requiredLabel: 'c',
    maintenance: 0, lifetime: 15, discountRate: 0.02, economicLearningRate: 0.05,
    primarySource: 'ELECTRICITY', secondarySource: 'NATURAL_GAS',
    efficiencyPrimary: 3.0, efficiencySecondary: 0.99, fractionPrimary: 0.8, fractionSecondary: 0.2,
    subsidy: 4000, sustainabilityScore: 3, socialLearningRate: 1.0,
    primaryCostPerKWh: E.ELECTRICITY, secondaryCostPerKWh: E.NATURAL_GAS,
  },
  ELECTRIC_HEAT_PUMP: {
    type: 'ELECTRIC_HEAT_PUMP',
    investSmall: 7500, investMedium: 9000, investLarge: 12000,
    distributionSystemCost: 5000, requiresLowTemp: true, requiredLabel: 'b',
    maintenance: 0, lifetime: 15, discountRate: 0.02, economicLearningRate: 0.1,
    primarySource: 'ELECTRICITY', secondarySource: 'NOT_APPLICABLE',
    efficiencyPrimary: 3.0, efficiencySecondary: 0, fractionPrimary: 1.0, fractionSecondary: 0,
    subsidy: 4000, sustainabilityScore: 5, socialLearningRate: 1.0,
    primaryCostPerKWh: E.ELECTRICITY, secondaryCostPerKWh: 0,
  },
  DISTRICT_HEATING: {
    type: 'DISTRICT_HEATING',
    investSmall: 5250.24, investMedium: 5250.24, investLarge: 5250.24,
    distributionSystemCost: 0, requiresLowTemp: false, requiredLabel: 'no',
    maintenance: 0, lifetime: 30, discountRate: 0.03, economicLearningRate: 0.05,
    primarySource: 'HEAT', secondarySource: 'NOT_APPLICABLE',
    efficiencyPrimary: 0.8, efficiencySecondary: 0, fractionPrimary: 1.0, fractionSecondary: 0,
    subsidy: 3775, sustainabilityScore: 3, socialLearningRate: 0.5,
    primaryCostPerKWh: E.HEAT, secondaryCostPerKWh: 0,
  },
};

// Max sustainability score across systems (=5) -> normalisation for attitude value.
export const MAX_SUSTAINABILITY_SCORE = 5;
