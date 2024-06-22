from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_creds import get_secrets
from general_functions.creds import secret_name
from os_open_usrn_functions.get_redirect_url import fetch_redirect_url
from os_open_usrn_functions.os_open_usrns_processor import load_geopackage_open_usrns
from general_functions.creds import open_usrn_schema
from os_open_usrn_functions.create_motherduck_table import create_table

def main():
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    
    conn = connect_to_motherduck(token, database)
    
    schema = open_usrn_schema
    create_table(conn, schema)
    
    url = fetch_redirect_url()
    load_geopackage_open_usrns(url, conn)

if __name__=="__main__":
    main()
