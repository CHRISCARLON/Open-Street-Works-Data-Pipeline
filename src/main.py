from database.motherduck import MotherDuckManager

from data_sources.os_usrn_uprn import OsUsrnUprn

from auth.get_credentials import get_secrets
from auth.creds import secret_name


def main():
    # MotherDuck credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit_test"

    # Create all the configurations
    os_usrn_uprn_config = OsUsrnUprn.create_default_latest()

    # Create all the tables and schemas
    with MotherDuckManager(token, database) as motherduck_manager_os_usrn_uprn:
        motherduck_manager_os_usrn_uprn.setup_for_data_source(os_usrn_uprn_config)
        # process_os_usrn_uprn_data(
        #     os_usrn_uprn_config.download_links[0],
        #     motherduck_manager_os_usrn_uprn.connection,
        #     os_usrn_uprn_config.batch_limit or 150000,
        #     os_usrn_uprn_config.schema_name,
        #     os_usrn_uprn_config.table_names[0],
        # )


if __name__ == "__main__":
    main()
