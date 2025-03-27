from datetime import datetime


class DatetimeUtils(object):
    def get_datetime_string_identifier(self) -> str:
        datetime_string_identifier = datetime.now().strftime('%Y_%m_%d_%Hh%Mm')
        return datetime_string_identifier