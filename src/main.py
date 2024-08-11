from geoplace_swa_codes.create_table_name import get_table_name
from geoplace_swa_codes.fetch_swa_codes import get_link, fetch_swa_codes, validate_data_model
from geoplace_swa_codes.load_swa_codes import create_table_swa_codes_motherduck, load_swa_code_data_motherduck

from general_functions.create_motherduck_connection import connect_to_motherduck, MotherDuckConnector
from general_functions.creds import secret_name
from general_functions.get_credentials import get_secrets

def main():
    
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = "geoplace_swa_codes"
    link = get_link()
    table_name = get_table_name(link)
    
    with MotherDuckConnector(token, database) as conn:
        conn.execute_query(f"""CREATE OR REPLACE TABLE "{schema}"."{table_name}" (
                    "SWA Code" INT,
                    "Account Name" VARCHAR,
                    "Prefix" VARCHAR,
                    "Account Type" VARCHAR,
                    "Registered for Street Manager" VARCHAR,
                    "Account Status" VARCHAR,
                    "Companies House Number" VARCHAR,
                    "Previous Company Names" VARCHAR,
                    "Linked/Parent Company" VARCHAR,
                    "Website" VARCHAR,
                    "Plant Enquiries" VARCHAR,
                    "Ofgem Electricity Licence" VARCHAR,
                    "Ofgem Gas Licence" VARCHAR,
                    "Ofcom Licence" VARCHAR,
                    "Ofwat Licence" VARCHAR,
                    "Company Subsumed By" VARCHAR,
                    "SWA Code of New Company" INT
                );""")

if __name__ == "__main__":
    main()
