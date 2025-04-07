from datetime import datetime

class DateRange:
    def __init__(self, start_date: datetime, end_date: datetime, date_format: str = "%Y-%m-%d"):
        self.start_date = start_date
        self.end_date = end_date
        self.date_format = date_format

    def __str__(self):
        return f"{self.start_date.strftime(self.date_format)} - {self.end_date.strftime(self.date_format)}"

    def __eq__(self, other):
        if not isinstance(other, DateRange):
            return False
        return self.start_date == other.start_date and self.end_date == other.end_date
