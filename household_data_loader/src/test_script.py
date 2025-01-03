import sqlite3

# Create a new SQLite database (or connect to an existing one)
conn = sqlite3.connect('households.db')
cursor = conn.cursor()

# Sum a column (e.g., p6_gasm3_2023)
cursor.execute("SELECT SUM(p6_gasm3_2023) FROM households")
total_gas = cursor.fetchone()[0]
print("Total gas consumption in 2023:", total_gas)

# Check for missing values in a column (e.g., energieklasse)
cursor.execute("SELECT COUNT(*) FROM households WHERE energieklasse IS NULL")
missing_energieklasse = cursor.fetchone()[0]
print("Number of missing values in energieklasse:", missing_energieklasse)

# Check for missing values in all columns
cursor.execute("""
SELECT 
    SUM(CASE WHEN numid IS NULL THEN 1 ELSE 0 END) AS numid_missing,
    SUM(CASE WHEN pid IS NULL THEN 1 ELSE 0 END) AS pid_missing,
    SUM(CASE WHEN vid IS NULL THEN 1 ELSE 0 END) AS vid_missing,
    SUM(CASE WHEN lid IS NULL THEN 1 ELSE 0 END) AS lid_missing,
    SUM(CASE WHEN sid IS NULL THEN 1 ELSE 0 END) AS sid_missing,
    SUM(CASE WHEN postcode IS NULL THEN 1 ELSE 0 END) AS postcode_missing,
    SUM(CASE WHEN straat IS NULL THEN 1 ELSE 0 END) AS straat_missing,
    SUM(CASE WHEN woonplaatsnaam IS NULL THEN 1 ELSE 0 END) AS woonplaatsnaam_missing,
    SUM(CASE WHEN huisnummer IS NULL THEN 1 ELSE 0 END) AS huisnummer_missing,
    SUM(CASE WHEN huisletter IS NULL THEN 1 ELSE 0 END) AS huisletter_missing,
    SUM(CASE WHEN huisnummertoevoeging IS NULL THEN 1 ELSE 0 END) AS huisnummertoevoeging_missing,
    SUM(CASE WHEN oppervlakte IS NULL THEN 1 ELSE 0 END) AS oppervlakte_missing,
    SUM(CASE WHEN woningequivalent IS NULL THEN 1 ELSE 0 END) AS woningequivalent_missing,
    SUM(CASE WHEN gebruiksdoelen IS NULL THEN 1 ELSE 0 END) AS gebruiksdoelen_missing,
    SUM(CASE WHEN pand_bouwjaar IS NULL THEN 1 ELSE 0 END) AS pand_bouwjaar_missing,
    SUM(CASE WHEN pc6_gemiddelde_woz_waarde_woning IS NULL THEN 1 ELSE 0 END) AS pc6_gemiddelde_woz_waarde_woning_missing,
    SUM(CASE WHEN gemiddelde_gemeente_woz IS NULL THEN 1 ELSE 0 END) AS gemiddelde_gemeente_woz_missing,
    SUM(CASE WHEN pc6_eigendomssituatie_perc_koop IS NULL THEN 1 ELSE 0 END) AS pc6_eigendomssituatie_perc_koop_missing,
    SUM(CASE WHEN pc6_eigendomssituatie_perc_huur IS NULL THEN 1 ELSE 0 END) AS pc6_eigendomssituatie_perc_huur_missing,
    SUM(CASE WHEN pc6_eigendomssituatie_aantal_woningen_corporaties IS NULL THEN 1 ELSE 0 END) AS pc6_eigendomssituatie_aantal_woningen_corporaties_missing,
    SUM(CASE WHEN netbeheerder IS NULL THEN 1 ELSE 0 END) AS netbeheerder_missing,
    SUM(CASE WHEN energieklasse IS NULL THEN 1 ELSE 0 END) AS energieklasse_missing,
    SUM(CASE WHEN woning_type IS NULL THEN 1 ELSE 0 END) AS woning_type_missing,
    SUM(CASE WHEN sbicode IS NULL THEN 1 ELSE 0 END) AS sbicode_missing,
    SUM(CASE WHEN gas_ean_count IS NULL THEN 1 ELSE 0 END) AS gas_ean_count_missing,
    SUM(CASE WHEN p6_grondbeslag_m2 IS NULL THEN 1 ELSE 0 END) AS p6_grondbeslag_m2_missing,
    SUM(CASE WHEN p6_gasm3_2023 IS NULL THEN 1 ELSE 0 END) AS p6_gasm3_2023_missing,
    SUM(CASE WHEN p6_gas_aansluitingen_2023 IS NULL THEN 1 ELSE 0 END) AS p6_gas_aansluitingen_2023_missing,
    SUM(CASE WHEN p6_kwh_2023 IS NULL THEN 1 ELSE 0 END) AS p6_kwh_2023_missing,
    SUM(CASE WHEN p6_kwh_productie_2023 IS NULL THEN 1 ELSE 0 END) AS p6_kwh_productie_2023_missing,
    SUM(CASE WHEN point IS NULL THEN 1 ELSE 0 END) AS point_missing,
    SUM(CASE WHEN buurtcode IS NULL THEN 1 ELSE 0 END) AS buurtcode_missing,
    SUM(CASE WHEN buurtnaam IS NULL THEN 1 ELSE 0 END) AS buurtnaam_missing,
    SUM(CASE WHEN wijkcode IS NULL THEN 1 ELSE 0 END) AS wijkcode_missing,
    SUM(CASE WHEN wijknaam IS NULL THEN 1 ELSE 0 END) AS wijknaam_missing,
    SUM(CASE WHEN gemeentecode IS NULL THEN 1 ELSE 0 END) AS gemeentecode_missing,
    SUM(CASE WHEN gemeentenaam IS NULL THEN 1 ELSE 0 END) AS gemeentenaam_missing,
    SUM(CASE WHEN provincienaam IS NULL THEN 1 ELSE 0 END) AS provincienaam_missing,
    SUM(CASE WHEN provinciecode IS NULL THEN 1 ELSE 0 END) AS provinciecode_missing
FROM households
""")
missing_values = cursor.fetchall()
print("Missing values in each column:", missing_values)

# Close the connection
conn.close()