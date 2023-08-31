Data
====
This page inidactes all data sources used. Primarly data from publicaly available open-sources have been used. The only exception on this is the low voltage transformer capacity, which has been distributed under NDA.

The public data can be downloaded, directly from its sources, or the actual data used in the model from the github repositories.
The list below is a general overview of main data sources used.

* `Get data neighborhood model <https://github.com/ZEnMo/Brabant-buurt-serious-game/tree/main/data/>`_ 
* `Get data province model <https://github.com/ZEnMo/Brabant-systeem-integratie-model/tree/main/Data/>`_ 

.. _List-of-model-data:

Overview of data sources
-------------------------



.. list-table:: **Data sources**
   :widths: 60 30 30 30
   :header-rows: 1
   
   * - Description
     - Source
     - Year
     - Accesability
   * - **Neighborhood characteristics**
     -
     - 
     - 
   * - Building characteristics
     - `BAG <https://www.pdok.nl/introductie/-/article/basisregistratie-adressen-en-gebouwen-ba-1>`_
     - 2021
     - Open data
   * - Building insulation levels
     - `RVO EP-Online <https://www.ep-online.nl/>`_
     - 2021
     - Open data
   * - Demographic characteristics 
     - `PDOK <https://www.pdok.nl/geo-services/-/article/cbs-wijken-en-buurten>`_
     - 2021
     - Open data
   * - Grid topology
     - `Enexis <https://www.enexis.nl/over-ons/open-data>`_
     - 2021
     - Open data
   * - Low-voltage transformer capacity
     - Enexis
     - 2022
     - Under NDA
   * - Building locations
     - `BAG <https://www.pdok.nl/introductie/-/article/basisregistratie-adressen-en-gebouwen-ba-1>`_
     - 2021
     - Open data
   * - **Province characteristics**
     -
     - 
     - 
   * - Neighborhood demarcation
     - `PDOK <https://www.pdok.nl/geo-services/-/article/cbs-wijken-en-buurten>`_
     - 2021
     - Open data
   * - Grid topology
     - `Enexis <https://www.enexis.nl/over-ons/open-data>`_
     - 2021
     - Open data
   * - Renewable generation sources
     - `RVO SDE++ <https://www.rvo.nl/subsidies-financiering/sde/aanvragen/feiten-en-cijfers>`_
     - 2022
     - Open data          
   * - Heat sources
     - `Startanalyse Expertise Centrum Warmte <https://www.warmteatlas.nl/viewer/app/Warmteatlas/v2?debug=false>`_
     - 2021
     - Open data     
   * - **Energy supply and demand**
     - 
     - 
     - 
   * - Annual household energy demand (DHW, room heating, electricity)
     - `PBL Vesta MAIS <https://www.pbl.nl/sites/default/files/downloads/pbl-2021-functioneel-ontwerp-vesta-mais-5.0-4583.pdf>`_
     - 2021
     - Open-source model
   * - Annual neighborhood energy demand per strategy
     - `Startanalyse PBL Vesta MAIS <https://themasites.pbl.nl/leidraad-warmte/2020/>`_
     - 2020
     - Open-source model     
   * - DHW hourly profile
     - `DHWCalc <https://www.uni-kassel.de/maschinenbau/institute/thermische-energietechnik/fachgebiete/solar-und-anlagentechnik/downloads>`_
     - 2005
     - Open-source model     
   * - Travel behavior patterns
     - Albatross
     - 2022
     - Closed scientific model     
   * - Electricity hourly demand profile
     - `NEDU <https://www.mffbas.nl/documenten/>`_
     - 2021
     - Open data     
   * - Gas hourly demand profile
     - `NEDU <https://www.mffbas.nl/documenten/>`_
     - 2021
     - Open data
   * - Weather data
     - `KNMI <https://www.knmi.nl/nederland-nu/klimatologie/uurgegevens>`_
     - 2019
     - Open data 
   * - **Costs**
     - 
     -   
     -
   * - Cost assumptions on insulation, district heating and inhouse heating solutions
     - `PBL Vesta MAIS <https://www.pbl.nl/sites/default/files/downloads/pbl-2021-functioneel-ontwerp-vesta-mais-5.0-4583.pdf>`_
     - 2021
     - Open-source model
   * - **Emissions**
     - 
     -   
     -
   * - Emission factors natural gas, green gas, residual heat, and fuels
     - `Lijst emissiefactoren <https://www.co2emissiefactoren.nl/lijst-emissiefactoren/>`_
     - 2022
     - Open data

Subtitute non-public data
~~~~~~~~~~~~~~~~~~~~~~~~~~
To substitute this data for public usage of the model the transformer capacity can be seen as a funtion from the connected households, averiging a capacity of 1.5kW per household. 

