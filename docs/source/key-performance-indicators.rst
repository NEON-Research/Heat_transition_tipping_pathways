Key-peformance indicators
==========================

Description of key-performance indicators


Key to the financial performance indicators is the concetp of *Levelised cost of energy* (*LCOE*). To calculate the costs per kWh
for the heating system, electricity system, insulation measures and overall system this method has been applied. Note the
actual costs that households have to pay can slighly differ as LCOE includes discount rates to average out costs over the
system lifetime. To better understand the concept see the equation below.

:math:`LCOE = \frac { \text{sum of costs over lifetime} } { \text{sum of electrical energy produced over lifetime} } = \frac{\sum_{t=1}^{n} \frac{ I_t + M_t + F_t}{\left({1+r}\right)^t} }{\sum_{t=1}^{n} \frac{E_t}{\left({1+r}\right)^{t}} }`

* It	:	investment expenditures in the year t
* Mt	:	operations and maintenance expenditures in the year t
* Ft	:	fuel expenditures in the year t
* Et	:	energy generated in the year t
* r	    :	discount rate
* n 	:	expected lifetime of system

Furthermore it is important to define which costs are in system scope. For the exact description see the table below.

.. list-table:: **Input settings**
   :widths: 40 40 100
   :header-rows: 1

   * - Indicator
     - Unit
     - Description
   * - **Total**
     -
     -
   * - Average energy costs 
     - €/kWh
     - Levelized cost of energy (LCOE). Combination of the levelised costs of electricity, heating and mobility multiplied with their respective demands and than divided by the total energy demand.
   * - Total annual costs
     - €/year
     - LCOE multiplied by annual energy demand
   * - Total lifetime costs
     - €
     - Annual costs multiplied by average energy system lifetime
   * - Annual energy demand
     - kWh
     - Primary energy consumption per year, includes heat losses in case of district heating on the societal level.
   * - **Heating solution**
     -
     -
   * - Average energy costs 
     - €/kWh
     - Levelized cost of heat (LCOH). Investment costs plus annual costs multiplied by discount rate devided by heating demand multiplied by discount rate
   * - Total annual costs
     - €/year
     - LCOH multiplied by annual heating demand
   * - Total lifetime costs
     - €
     - Annual costs multiplied by heating system lifetime
   * - Annual heating demand
     - kWh
     - Primary energy consumption per year, includes heat losses in case of district heating on the societal level.
   * - **Insulation**
     -
     -
   * - Average energy costs 
     - €/kWh
     - Levelized cost of insulation (LCOI), or euro per saved kWh of energy. Insulation costs multiplied by discount rate devided by energy demand reduction multiplied by discount rate
   * - Total annual costs
     - €/year
     - LCOI multiplied by annual energy savings
   * - Total lifetime costs
     - €
     - Annual costs multiplied by insulation system lifetime
   * - Annual energy savings
     - kWh
     - Energy saved by insulation measure (assumption from Vesta MAIS).
   * - **Electricity**
     -
     -
   * - Average electricity costs 
     - €/kWh
     - Also **LCOE** (Levelised cost of electricity). Adding up investment costs for PV and home batteries if a household has those and the grid electricity costs per kWh. Multiplying this over the lifetime with the discount rate.
   * - Total annual costs
     - €/year
     - LCOE multiplied by annual household electricity demand (*Note: this excludes public EV charging demand*).
   * - Total lifetime costs
     - €
     - Annual costs multiplied by electricity system lifetime (lifetime of PV and home battery systems)
   * - Annual electricity consumption
     - kWh
     - Household electricity consumption + public charging electricity consumption (*Note: This is actual demand so not grid demand, PV generated energy is not substracted from this figure*).
