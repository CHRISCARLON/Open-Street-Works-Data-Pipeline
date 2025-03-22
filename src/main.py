from database.motherduck import MotherDuckManager

from data_sources.os_open_usrn import OsOpenUsrn
from data_processors.os_open_usrn import process_data as process_os_open_usrn_data

from auth.get_credentials import get_secrets
from auth.creds import secret_name


def main():
    # MotherDuck credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit_test"

    # Create all the configurations
    os_open_usrn_config = OsOpenUsrn.create_default_latest()

    # Create all the tables and schemas
    with MotherDuckManager(token, database) as motherduck_manager_os_open_usrn:
        motherduck_manager_os_open_usrn.setup_for_data_source(os_open_usrn_config)
        process_os_open_usrn_data(os_open_usrn_config.download_links[0], motherduck_manager_os_open_usrn.connection, os_open_usrn_config.batch_limit or 150000, os_open_usrn_config.schema_name, os_open_usrn_config.table_names[0])

if __name__ == "__main__":
    main()
