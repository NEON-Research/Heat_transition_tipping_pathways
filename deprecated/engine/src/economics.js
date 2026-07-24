// economics.js — Equivalent Annual Cost (EAC) of a heating system for a dwelling.
// Faithful transcription of J_Household.f_getEAC (TCO discounted over lifetime,
// annualised via the annuity factor).
//
// EAC = round(TCO / annuityFactor)
//   TCO           = (investment - subsidy) at t=0  +  Σ_t (maintenance + energy)/(1+r)^t
//   annuityFactor = (1 - (1+r)^-lifetime) / r
//
// investment = supply-system cost (sized by heat demand)
//              + heat-distribution retrofit cost   (if system needs low-temp and dwelling lacks it)
//              + insulation cost to reach required label (if system requires a better label)


export function investmentCostsSupply(hs, heatDemandKWh) {
  if (heatDemandKWh < 6000) return hs.investSmall;
  if (heatDemandKWh > 10000) return hs.investLarge;
  return hs.investMedium;
}

export function computeEAC(hs, dwelling, insulationCostFn) {
  const invSupply = investmentCostsSupply(hs, dwelling.heatDemandKWh);
  let investment = invSupply;

  // low-temperature in-house distribution retrofit
  if (hs.requiresLowTemp && !dwelling.hasLowTemp) {
    investment += hs.distributionSystemCost;
  }

  // insulation upgrade to the system's required label (on-the-fly from current label;
  // the function returns 0 when no upgrade is needed) — matches J_Household.f_getEAC
  if (hs.requiredLabel && hs.requiredLabel !== 'no') {
    investment += insulationCostFn(dwelling, hs.requiredLabel);
  }

  // annual energy cost
  let energyPerYear = hs.primaryCostPerKWh * dwelling.heatDemandKWh
                      * hs.fractionPrimary / hs.efficiencyPrimary;
  if (hs.secondarySource && hs.secondarySource !== 'NOT_APPLICABLE') {
    energyPerYear += hs.secondaryCostPerKWh * dwelling.heatDemandKWh
                     * hs.fractionSecondary / hs.efficiencySecondary;
  }

  // discounted total cost of ownership
  const r = hs.discountRate;
  let tco = 0;
  for (let t = 0; t < hs.lifetime; t++) {
    if (t === 0) tco += investment - hs.subsidy;
    tco += (hs.maintenance + energyPerYear) / Math.pow(1 + r, t);
  }
  const annuityFactor = (1 - Math.pow(1 + r, -hs.lifetime)) / r;
  return Math.round(tco / annuityFactor);
}
