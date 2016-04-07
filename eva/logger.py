"""!
@brief This module provides custom logging adapters for specially formatted output.
"""

import logging


class MesosLogAdapter(logging.LoggerAdapter):
    """
    @brief This example adapter expects the passed in dict-like object to have
    a 'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '%s %s %s' % (self.extra['MARATHON_APP_ID'], self.extra['MESOS_TASK_ID'], msg), kwargs


class JobLogAdapter(logging.LoggerAdapter):
    """
    @brief This adapter prepends the job ID in brackets to all log messages.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['JOB'].id, msg), kwargs
