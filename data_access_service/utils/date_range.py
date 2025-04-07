from datetime import datetime

class DateRange:
    def __init__(self, start_date: str, end_date: str, date_format: str = "%Y-%m-%d"):
        self.start_date = datetime.strptime(start_date, date_format)
        self.end_date = datetime.strptime(end_date, date_format)

    def __str__(self):
        return f"DateRange(start_date={self.start_date}, end_date={self.end_date})"

    def contains(self, date: str, date_format: str = "%Y-%m-%d") -> bool:
        date_obj = datetime.strptime(date, date_format)
        return self.start_date <= date_obj <= self.end_date
