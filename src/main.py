from src.pipeline_launch.monthly_permit_main import main as monthly_permit_main
from src.pipeline_launch.os_open_usrns_main import main as os_open_usrns_main

def main():
    """
    Call monthly permit nad os open usrns here.
    
    This is the pipeline that I run most frequently.
    """
    
    # Call the main function from monthly_permit_main
    # Include the batch limit argument
    monthly_permit_main(75000)
    
    # Call the main function from os_open_usrns_main
    os_open_usrns_main()

if __name__ == "__main__":
    main()
