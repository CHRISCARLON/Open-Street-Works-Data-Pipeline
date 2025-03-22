from datetime import datetime


def get_raw_data_year():
    current_year = datetime.now().year
    current_month = datetime.now().month

    if current_month == 1:
        return f"raw_data_{current_year - 1}"
    else:
        return f"raw_data_{current_year}"
