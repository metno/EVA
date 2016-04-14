"""!
This module provides command-line argument templating through the Jinja2
templating system.
"""

import jinja2
import dateutil.tz
import datetime


def filter_iso8601(value):
    return value.astimezone(dateutil.tz.tzutc()).strftime('%Y-%m-%dT%H:%M:%SZ')


def filter_iso8601_compact(value):
    return value.astimezone(dateutil.tz.tzutc()).strftime('%Y%m%dT%H%M%SZ')


def filter_timedelta(value, **kwargs):
    return value + datetime.timedelta(**kwargs)


def filter_strftime(value, *args, **kwargs):
    return value.strftime(*args, **kwargs)


class Environment(jinja2.Environment):
    def __init__(self, *args, **kwargs):
        """!
        @brief Initialize a Jinja2 template renderer.

        The constructor also adds template filters.
        """
        super(Environment, self).__init__(*args, **kwargs)
        self.filters['iso8601'] = filter_iso8601
        self.filters['iso8601_compact'] = filter_iso8601_compact
        self.filters['timedelta'] = filter_timedelta
        self.filters['strftime'] = filter_strftime
