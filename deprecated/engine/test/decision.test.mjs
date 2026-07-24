// decision.test.mjs — mirrors ../../tests/test_heat_model_spec.py against the JS engine.
// Run: node --test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as D from '../src/decision.js';
import { RNG } from '../src/rng.js';
import { computeEAC } from '../src/economics.js';
import { HEATING_SYSTEM_DATA } from '../data/heating_systems.js';

const approx = (a, b, t = 1e-9) => assert.ok(Math.abs(a - b) <= t, `${a} !== ${b}`);

test('attitude value', () => {
  approx(D.attitudeValue(0.7, 0.9), 0.8);
  approx(D.attitudeValue(0.42, 0.42), 1.0);
  approx(D.attitudeValue(0.0, 1.0), 0.0);
});

test('effort tiers', () => {
  approx(D.effort(true, true, false), 0.2);
  approx(D.effort(false, true, false), 0.8);
  approx(D.effort(false, true, true), 0.5);
  approx(D.effort(false, false, false), 0.5);
});

test('normalized value', () => {
  approx(D.normalizedValue(1500, 1000, 2000), 0.5);
  approx(D.normalizedValue(1000, 1000, 2000), 0.0);
});

test('PBC', () => {
  approx(D.pbc(0.25, 0.5), 0.475 / 0.7);
  approx(D.pbc(0.0, 0.0), 1.0);
  approx(D.pbc(1.0, 1.0), 0.0);
});

test('subjective norm capped', () => {
  approx(D.subjectiveNorm(4, 10, 0.5), 0.6);
  approx(D.subjectiveNorm(8, 10, 1.0), 1.0);
  approx(D.subjectiveNorm(3, 10, 0.0), 0.3);
});

test('intention medium SLF', () => {
  const p = 0.475 / 0.7;
  const expected = (0.8 * 0.5 + 0.6 * 0.5 * 0.3 + p * 0.5) / 1.5;
  approx(D.intention(0.8, 0.6, p, 1.0, 0.3), expected);
});

test('intention high SLF grows weight and denominator', () => {
  const p = 0.5;
  const expected = (0.8 * 0.5 + 0.6 * 1.0 * 0.3 + p * 0.5) / (0.5 + 1.0 + 0.5);
  approx(D.intention(0.8, 0.6, p, 2.0, 0.3), expected);
});

test('perceived utility is mean of intention and PBC', () => {
  approx(D.perceivedUtility(0.4, 0.8), 0.6);
});

test('salience flat = half blend', () => {
  const novelty = 1 / (1 + Math.exp(10 * (0.2 - 0.3)));
  approx(D.salienceFactor(0.2, 0.2), 0.5 * novelty + 0.5 * 0.2);
});

test('salience bounded 0..1', () => {
  const s = D.salienceFactor(0.9, 0.9);
  assert.ok(s >= 0 && s <= 1);
});

test('learning curve two doublings', () => {
  approx(D.capexLearningFactor(4000, 1000, 0.1, 1.0), 0.81);
  approx(D.capexLearningFactor(4000, 1000, 0.1, 2.0), 0.64);
  approx(D.capexLearningFactor(1000, 1000, 0.1, 1.0), 1.0);
});

test('opportunity trigger CORRECTED fires at 75%', () => {
  assert.equal(D.hasOpportunityTrigger(11, 15), false); // 0.733
  assert.equal(D.hasOpportunityTrigger(12, 15), true);  // 0.80
  assert.equal(D.hasOpportunityTrigger(14, 15), true);
});

test('opportunity trigger LEGACY reproduces int-division bug', () => {
  assert.equal(D.hasOpportunityTrigger(14, 15, true), false); // 93% but false
  assert.equal(D.hasOpportunityTrigger(15, 15, true), true);
});

test('end of life trigger', () => {
  assert.equal(D.hasEndOfLifeTrigger(14, 15), false);
  assert.equal(D.hasEndOfLifeTrigger(15, 15), true);
});

test('RUM small scale preserves ranking at equal noise', () => {
  approx(D.rumScore(0.7, 0), 0.7);
  assert.ok(D.rumScore(0.7, 1.0) > D.rumScore(0.6, 1.0));
});

test('EAC is finite and positive on real data', () => {
  const d = { heatDemandKWh: 9000, energyLabel: 'd', livingAreaM2: 100, hasLowTemp: false };
  const eac = computeEAC(HEATING_SYSTEM_DATA.NATURAL_GAS_BOILER, d, () => 0);
  assert.ok(Number.isFinite(eac) && eac > 0);
});

test('RNG beta(5,2) mean ~ 5/7 and deterministic', () => {
  const r = new RNG(1); let s = 0, n = 50000;
  for (let i = 0; i < n; i++) s += r.beta(5, 2, 0, 1);
  approx(s / n, 5 / 7, 0.01);
  assert.equal(new RNG(7).next(), new RNG(7).next());
});
