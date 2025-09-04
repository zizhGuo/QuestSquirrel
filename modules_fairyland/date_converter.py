from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta


def date2str(date, timestamp_format=False):
    if date.month < 10:
        month = f"0{date.month}"
    else:
        month = f"{date.month}"
    if date.day < 10:
        day = f"0{date.day}"
    else:
        day = f"{date.day}"
    if timestamp_format:
        return f"{date.year}-{month}-{day}"
    else:
        return f"{date.year}{month}{day}"

def dt_minus_days(dt, days):
    return date2str(datetime.strptime(dt, "%Y-%m-%d") - timedelta(days=days))
