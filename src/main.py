from pipeline_assets import (
    monthly_permit_main,
    os_open_usrns_main,
    swa_codes_main,
    os_open_roads_main,
)
from src.pipeline_assets import os_open_linked_ids_main


def main():
    monthly_permit_main.main(100000)
    os_open_usrns_main.main(100000)
    swa_codes_main.main()
    os_open_linked_ids_main.main(150000)
    os_open_roads_main.main(100000)


if __name__ == "__main__":
    main()
