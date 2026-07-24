import pandas as pd
import os

# Load combined JSON data
df = pd.read_json('./data/combined.json')

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# Drop the existing table if it exists
cursor.execute('DROP TABLE IF EXISTS households')

# Create a new table
cursor.execute('''
CREATE TABLE IF NOT EXISTS households (
    numid INTEGER PRIMARY KEY,
    pid INTEGER,
    vid INTEGER,
    lid TEXT,
    sid TEXT,
    postcode TEXT,
    straat TEXT,
    woonplaatsnaam TEXT,
    huisnummer INTEGER,
    huisletter TEXT,
    huisnummertoevoeging TEXT,
    oppervlakte INTEGER,
    woningequivalent INTEGER,
    gebruiksdoelen TEXT,
    pand_bouwjaar INTEGER,
    pc6_gemiddelde_woz_waarde_woning INTEGER,
    gemiddelde_gemeente_woz INTEGER,
    pc6_eigendomssituatie_perc_koop INTEGER,
    pc6_eigendomssituatie_perc_huur INTEGER,
    pc6_eigendomssituatie_aantal_woningen_corporaties INTEGER,
    netbeheerder TEXT,
    energieklasse TEXT,
    woning_type TEXT,
    sbicode TEXT,
    gas_ean_count INTEGER,
    p6_grondbeslag_m2 INTEGER,
    p6_gasm3_2023 INTEGER,
    p6_gas_aansluitingen_2023 INTEGER,
    p6_kwh_2023 INTEGER,
    p6_kwh_productie_2023 INTEGER,
    point TEXT,
    buurtcode TEXT,
    buurtnaam TEXT,
    wijkcode TEXT,
    wijknaam TEXT,
    gemeentecode TEXT,
    gemeentenaam TEXT,
    provincienaam TEXT,
    provinciecode TEXT
)
''')

# Insert data into the table
df.to_sql('households', conn, if_exists='append', index=False)

# Commit changes and close the connection
conn.commit()
conn.close()