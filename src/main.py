from database.motherduck import MotherDuckManager

from data_sources.street_manager import StreetManager
from data_sources.geoplace_swa import GeoplaceSwa

from data_processors.street_manager import process_data as process_street_manager_data
from data_processors.geoplace_swa import process_data as process_geoplace_swa_data

from auth.get_credentials import get_secrets
from auth.creds import secret_name


def main():
    # MotherDuck credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]

    # Create all the configurations
    street_manager_config_latest = StreetManager.create_default_latest()
    geoplace_swa_config_latest = GeoplaceSwa.create_default_latest()

    # Create all the tables and schemas
    with MotherDuckManager(token, database) as motherduck_manager_street_manager:
        motherduck_manager_street_manager.setup_for_data_source(
            street_manager_config_latest
        )
        process_street_manager_data(
            street_manager_config_latest.download_links[0],
            150000,
            motherduck_manager_street_manager.connection,
            street_manager_config_latest.schema_name,
            street_manager_config_latest.table_names[0],
        )

    with MotherDuckManager(token, database) as motherduck_manager_geoplace:
        motherduck_manager_geoplace.setup_for_data_source(geoplace_swa_config_latest)
        process_geoplace_swa_data(
            geoplace_swa_config_latest.download_links[0],
            motherduck_manager_geoplace.connection,
            geoplace_swa_config_latest.schema_name,
            geoplace_swa_config_latest.table_names[0],
        )


if __name__ == "__main__":
    main()
