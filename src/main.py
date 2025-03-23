from database.motherduck import MotherDuckManager

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.os_usrn_uprn import OsUsrnUprn
from data_sources.os_open_usrn import OsOpenUsrn
from data_sources.geoplace_swa import GeoplaceSwa
from data_sources.street_manager import StreetManager

from data_processors.os_usrn_uprn import process_data as process_os_usrn_uprn_data
from data_processors.os_open_usrn import process_data as process_os_open_usrn_data
from data_processors.geoplace_swa import process_data as process_geoplace_swa_data
from data_processors.street_manager import process_data as process_street_manager_data

def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit_test"

    # Create Data Source Configs
    street_manager_config = StreetManager.create_default_latest()
    geoplace_swa_config = GeoplaceSwa.create_default_latest()
    os_open_usrn_config = OsOpenUsrn.create_default_latest()
    os_usrn_uprn_config = OsUsrnUprn.create_default_latest()
    
    # Process Data
    with MotherDuckManager(token, database) as motherduck_manager_street_manager:
        motherduck_manager_street_manager.setup_for_data_source(street_manager_config)
        process_street_manager_data(
            url=street_manager_config.download_links[0],
            batch_size=street_manager_config.batch_limit or 150000,
            conn=motherduck_manager_street_manager.connection,
            schema_name=street_manager_config.schema_name,
            table_name=street_manager_config.table_names[0],
        )

    with MotherDuckManager(token, database) as motherduck_manager_geoplace_swa:
        motherduck_manager_geoplace_swa.setup_for_data_source(geoplace_swa_config)
        process_geoplace_swa_data(
            url=geoplace_swa_config.download_links[0],
            conn=motherduck_manager_geoplace_swa.connection,
            schema_name=geoplace_swa_config.schema_name,
            table_name=geoplace_swa_config.table_names[0],
        )

    with MotherDuckManager(token, database) as motherduck_manager_os_open_usrn:
        motherduck_manager_os_open_usrn.setup_for_data_source(os_open_usrn_config)
        process_os_open_usrn_data(
            url=os_open_usrn_config.download_links[0],
            conn=motherduck_manager_os_open_usrn.connection,
            batch_size=os_open_usrn_config.batch_limit or 150000,
            schema_name=os_open_usrn_config.schema_name,
            table_name=os_open_usrn_config.table_names[0],
        )

    with MotherDuckManager(token, database) as motherduck_manager_os_usrn_uprn:
        process_os_usrn_uprn_data(
            url=os_usrn_uprn_config.download_links[0],
            conn=motherduck_manager_os_usrn_uprn.connection,
            batch_limit=os_usrn_uprn_config.batch_limit or 150000,
            schema_name=os_usrn_uprn_config.schema_name,
            table_name=os_usrn_uprn_config.table_names[0],
        )


if __name__ == "__main__":
    main()
