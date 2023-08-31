Neighborhood model
==================

.. _neighborhood-model-structure:

Model structure
----------------

The model consists of bottom-up modeled energy assets and agents, such as buildings, cars, and households. Heterogeneous instances of these objects or agents are created based on data from, for example, households, cars, people, and transformers. The neighborhood model contains data from three different neighborhoods: a rural village and urban districts from the 60s with and without an operational district heating grid. In this paper, the focus is on the urban neighborhood without district heating. The other neighborhoods were created to let the local policymakers interact with their location-specific characteristics in the participatory process. The neighborhoods are based on data from the central bureau of statistics (CBS, 2022), basic registration of buildings and addresses (BAG) (Kadaster, 2022), and the grid operator (Enexis Netbeheer, 2022). They contain between 1201 and 2904 residence objects connected to 7 to 23 transformers.

.. _energy-calculations:

Energy Calculations
--------------------

Energy demand and supply are calculated hourly at a household level. Energy demand is subdivided into room heating, domestic hot water, mobility, electricity, and cooking demand. The model is activity-based, which ensures that any change in appliances (e.g., gas stove to induction stove, conventional car to electric car) is a shift in energy carrier while maintaining behavioral characteristics. 
Local demand is always matched by a combination of local supply (PV generation and renewable heat sources) and imported supply (natural gas, gasoline, and electricity). The energy sources are included in the costs. However, it is up to the user to create viable scenarios where the required resources (e.g. residual heat) do not exceed the potential.
Flexibility and storage are covered by the curtailment of PV systems, smart charging algorithm for electric vehicles, and home battery systems. PV systems will curtail if the aggregated feed-in power exceeds the connected low-voltage transformer limits on nominal capacity. Smart charging is implemented according to an algorithm aimed at alleviating grid congestion. If transformers exceed the maximum capacity, the charging session of connected EVs is delayed until the maximum capacity is not exceeded anymore. This system is not yet possible but was modeled to show the potential of smart charging. Home batteries are implemented with the scope of minimizing homeowners’ electricity bills. This means charging based on excess energy and discharging to alleviate grid congestion. This is not an optimized charging schedule or aimed at maximizing homeowners’ profits. Further exploring the effects of different charging strategies is up for future research.

.. _scenario-settings-in-GUI:

Scenario settings in GUI
------------------------
When using the model in the participatory process, the user can select a range of scenario settings. The settings are presented as the number of households with a specific application or technology. Changing this setting means an algorithm will select which household gets this technology. For example, if more electric heat pumps are selected, households with higher energy labels will get the heat pumps first. This aligns with existing examples of renewable energy technology adoption and ensures realistic costs and energy loss assumptions are considered.


