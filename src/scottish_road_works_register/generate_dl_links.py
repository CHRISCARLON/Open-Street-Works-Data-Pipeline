def dl_link_creator(identifier):
    """
    Idenfitifer could be things such as:

    1. 2020, 2022, etc -> historical year
    2. APR, MAR, FEB, etc -> month from current year
    3. 01, 02, 03, etc -> day from current month

    """

    base_url = f"https://downloads.srwr.scot/export/api/v1/file/{identifier}.zip"

    return base_url
