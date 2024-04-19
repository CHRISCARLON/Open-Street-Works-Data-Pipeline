from datetime import date, timedelta

def last_month() -> list:
    """
    Subtract one day from the first day of the current month to get a day in the previous month
    Should return [2024, 3] if you run it in April 2024

    """
    current_date = date.today()
    first_day_of_current_month = date(current_date.year, current_date.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    year, month = last_day_of_previous_month.year, f"{last_day_of_previous_month.month:02d}"
    return [year, month]

def date_for_table():
    year_month = last_month()
    year = str(year_month[0])
    month_number = str(year_month[1])
    result = f"{month_number}_{year}"
    return result

def month_to_abbrev(month: int) -> str:
    """
    Function to convert a month number to its abbreviation

    """
    month_abbreviations = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    return month_abbreviations.get(month, "InvalidMonth")

if __name__ == "__main__":
    date = date_for_table()
    print(date)
