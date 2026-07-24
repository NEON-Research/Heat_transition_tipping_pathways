import requests
import csv
import sqlite3

# Function to get municipality codes from a CSV file
def read_municipality_codes(filepath):
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row['GemeentecodeGM'] for row in reader]
    
#Function to iterate over municipality codes and fetch data
def fetch_all_data(codes):
    all_data = []
    for code in codes:
        data = get_municipality_data(code)
        all_data.append(data)
    return all_data

# Function for API call to collect data for a single municipality
def get_municipality_data(municipality_code):
    url = f"https://ds.vboenergie.commondatafactory.nl/list/?match-gemeentecode={municipality_code}"
    response = requests.get(url)
    return response.json()

# Function to create a table in the database
def create_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS households')
    cursor.execute('''
    CREATE TABLE households (
        numid INTEGER,
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

def insert_data(cursor, all_data):
    # Insert data into the table
    for data in all_data:
        for row in data:
            cursor.execute('''
            INSERT INTO households (
                numid, pid, vid, lid, sid, postcode, straat, woonplaatsnaam, huisnummer, huisletter, huisnummertoevoeging, 
                oppervlakte, woningequivalent, gebruiksdoelen, pand_bouwjaar, pc6_gemiddelde_woz_waarde_woning, 
                gemiddelde_gemeente_woz, pc6_eigendomssituatie_perc_koop, pc6_eigendomssituatie_perc_huur, 
                pc6_eigendomssituatie_aantal_woningen_corporaties, netbeheerder, energieklasse, woning_type, sbicode, 
                gas_ean_count, p6_grondbeslag_m2, p6_gasm3_2023, p6_gas_aansluitingen_2023, p6_kwh_2023, p6_kwh_productie_2023, 
                point, buurtcode, buurtnaam, wijkcode, wijknaam, gemeentecode, gemeentenaam, provincienaam, provinciecode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['numid'], row['pid'], row['vid'], row['lid'], row['sid'], row['postcode'], row['straat'], row['woonplaatsnaam'], 
                row['huisnummer'], row['huisletter'], row['huisnummertoevoeging'], row['oppervlakte'], row['woningequivalent'], 
                row['gebruiksdoelen'], row['pand_bouwjaar'], row['pc6_gemiddelde_woz_waarde_woning'], row['gemiddelde_gemeente_woz'], 
                row['pc6_eigendomssituatie_perc_koop'], row['pc6_eigendomssituatie_perc_huur'], row['pc6_eigendomssituatie_aantal_woningen_corporaties'], 
                row['netbeheerder'], row['energieklasse'], row['woning_type'], row['sbicode'], row['gas_ean_count'], row['p6_grondbeslag_m2'], 
                row['p6_gasm3_2023'], row['p6_gas_aansluitingen_2023'], row['p6_kwh_2023'], row['p6_kwh_productie_2023'], row['point'], 
                row['buurtcode'], row['buurtnaam'], row['wijkcode'], row['wijknaam'], row['gemeentecode'], row['gemeentenaam'], 
                row['provincienaam'], row['provinciecode']
            ))

def main():
    municipality_codes = read_municipality_codes('./data/municipality_codes.csv')
    # print(municipality_codes)

    # Select just the first two municipality codes
    #selected_codes = municipality_codes[:2]

    all_data = fetch_all_data(municipality_codes)
    # print(all_data)

    conn = sqlite3.connect('households.db')
    cursor = conn.cursor()

    create_table(cursor)
    insert_data(cursor, all_data)

    conn.commit()

    # Inspect the results in the table
    cursor.execute("SELECT * FROM households LIMIT 5")  # Fetch the first 5 rows for inspection
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()



