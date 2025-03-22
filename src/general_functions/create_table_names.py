from datetime import datetime


def create_table_names(name: str) -> str:
    """
    Create a table name by appending current date in YYYY_MM_DD format.

    Args:
        name (str): Base name for the table

    Returns:
        str: Table name with formatted date appended
    """
    date = datetime.today().strftime("%Y_%m_%d")
    table_name = f"{name}_{date}"
    return table_name
