"""!
@brief This module provides custom logging adapters for specially formatted output.
"""

import logging


class WildcardLogFilter(logging.Filter):
    """
    This log filter injects any kind of data into the log record.
    """
    def __init__(self, **kwargs):
        self.extra = kwargs

    def filter(self, record):
        [setattr(record, key, value) for key, value in self.extra.items()]
        return True


class AdapterLogAdapter(logging.LoggerAdapter):
    """
    @brief This adapter prepends the job adapter ID in brackets to all log messages.
    """
    def process(self, msg, kwargs):
        return u'{%s} %s' % (self.extra['ADAPTER'].config_id, msg), kwargs


class JobLogAdapter(logging.LoggerAdapter):
    """
    @brief This adapter prepends the job ID in brackets to all log messages.
    """
    def process(self, msg, kwargs):
        return u'[%s] %s' % (self.extra['JOB'].id, msg), kwargs
