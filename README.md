# Open Street Works Data Pipeline 🚧

> [!IMPORTANT]
> This is currently being rewritten from top to bottom to use a more modular and flexible approach.
>
> I wrote this project a while ago and it's time to update the code to be more maintainable and flexible.
>
> I'll update this README properly when the new version is fully ready.

OLD WAY (TOO MUCH CODE AND TOO MESSY)

```python
import psutil
import os

from memory_profiler import profile
from loguru import logger
from general_functions.creds import secret_name

from england_street_manager.generate_dl_link import generate_dl_link
from england_street_manager.schema_name_trigger import get_raw_data_year
from england_street_manager.date_month import date_for_table
from england_street_manager.motherduck_create_table import motherduck_create_table
from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_credentials import get_secrets
from england_street_manager.extract_load_data import process_batch_and_insert_to_motherduck
from england_street_manager.stream_zipped_data import fetch_data

@profile
def main(batch_limit: int):
    """
    Monthly permit main will process the latest Street Manager Permit data.

    If you run this in April then you will generate the D/L link for March's data.

    Args:
        Batch limit - set this to specify the chunk size for processing (e.g. process in chunks of 100,000 files)
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    logger.success("MONTHLY PERMIT DATA STARTED")

    # Credentials for MotherDuck
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = get_raw_data_year()

    # Create MotherDuck table date
    table = date_for_table()

    # Initiate motherduck connection and table
    conn = connect_to_motherduck(token, database)
    motherduck_create_table(conn, schema, table)

    # Start data processing
    link = generate_dl_link()
    data = fetch_data(link)
    process_batch_and_insert_to_motherduck(data, batch_limit, conn, schema, table)

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    logger.success("MONTHLY PERMIT DATA PROCESSED")
```

NEW WAY (IN PROGRESS BUT A LOT CLEANER)

```python
from data_sources.street_manager import StreetManager
from database.motherduck import MotherDuckManager
from data_processors.street_manager import process_data

def main():
    # MotherDuck credentials
    token = ""
    database = ""

    # Create all the configurations
    street_manager_config_latest = StreetManager.create_default_latest()

    # Process the data
    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(street_manager_config_latest)
        process_data(street_manager_config_latest.download_links[0], 150000, motherduck_manager, street_manager_config_latest.schema_name, street_manager_config_latest.table_names[0])

if __name__ == "__main__":
    main()
```
