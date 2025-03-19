from data_sources.street_manager import StreetManager
from database.motherduck import MotherDuckManager
from data_processors.street_manager import process_data

def main():
    # MotherDuck credentials
    token = ""
    database = ""

    # Create all the configurations
    street_manager_config_latest = StreetManager.create_default_latest()

    # Create all the tables and schemas
    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(street_manager_config_latest)
        process_data(street_manager_config_latest.download_links[0], 150000, motherduck_manager.connection, street_manager_config_latest.schema_name, street_manager_config_latest.table_names[0])

if __name__ == "__main__":
    main()  
