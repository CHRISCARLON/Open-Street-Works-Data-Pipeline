from pipeline_launch_scripts import (
    swa_codes_main,
    sm_monthly_permit_main,
    os_open_usrns_main,
    os_open_linked_ids_main,
)

def main():
    swa_codes_main.main()
    sm_monthly_permit_main.main(100000)
    os_open_usrns_main.main(100000)
    os_open_linked_ids_main.main(150000)

if __name__ == "__main__":
    main()
