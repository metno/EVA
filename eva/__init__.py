import logging
import re
import time
import datetime
import dateutil.tz
import traceback

import eva.exceptions


def retry_n(func, args=(), kwargs={}, interval=5, exceptions=(Exception,), warning=1, error=3, give_up=5, logger=logging):
    """
    Call 'func' and, if it throws anything listed in 'exceptions', catch it and retry again
    up to 'give_up' times. If give_up is <= 0, retry indefinitely.
    Checks that error > warning > 0, and give_up > error or give_up <= 0.
    """
    assert (warning > 0) and (error > warning) and (give_up <= 0 or give_up > error)
    tries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            tries += 1
            if give_up > 0 and tries >= give_up:
                logger.error('Action failed %d times, giving up: %s' % (give_up, e))
                return False
            if tries >= error:
                logfunc = logger.error
            elif tries >= warning:
                logfunc = logger.warning
            else:
                logfunc = logger.info
            logfunc('Action failed, retrying in %d seconds: %s' % (interval, e))
            time.sleep(interval)


def import_module_class(name):
    components = name.split('.')
    modname = ('.').join(components[0:-1])
    mod = __import__(modname)
    for c in components[1:-1]:
        mod = getattr(mod, c)
    return getattr(mod, components[-1])


def format_exception_as_bug(exception):
    """!
    @brief Given an Exception object, return a list of lines that can be
    printed in order to display the backtrace and error.
    """
    lines = []
    lines += ["Fatal error: %s" % exception]
    backtrace = traceback.format_exc().split("\n")
    lines += ["***********************************************************"]
    lines += ["Uncaught exception during program execution. THIS IS A BUG!"]
    lines += ["***********************************************************"]
    for line in backtrace:
        lines += [line]
    return lines


def print_and_mail_exception(exception, logger, mailer):
    lines = format_exception_as_bug(exception)
    for line in lines:
        logger.critical(line)
    template_params = {
        'error_message': lines[0],
        'backtrace': '\n'.join(lines[1:]) + '\n',
    }
    subject = eva.mail.text.CRITICAL_ERROR_SUBJECT % template_params
    text = eva.mail.text.CRITICAL_ERROR_TEXT % template_params
    mailer.send_email(subject, text)


def in_array_or_empty(id, array):
    """!
    @returns true if `id` is found in `array`, or `array` is empty.
    """
    return (array is None) or (len(array) == 0) or (id in array)


def split_comma_separated(string):
    """!
    @brief Given a comma-separated string, return a list of components.
    @returns list
    """
    return [x.strip() for x in string.strip().split(',')]


def url_to_filename(url):
    """!
    @brief Convert a file://... URL to a path name. Raises an exception if
    the URL does not start with file://.
    """
    start = 'file://'
    if not url.startswith(start):
        raise RuntimeError('Expected an URL starting with %s, got %s instead' % (start, url))
    return url[len(start):]


def parse_boolean_string(string):
    """!
    @brief Given a string, return its boolean value.
    @returns True if parsed as true, False if parsed as false, otherwise None.
    """
    if string in ['yes', 'YES', 'true', 'TRUE', 'True', '1']:
        return True
    if string in ['no', 'NO', 'false', 'FALSE', 'False', '0']:
        return False
    return None


def strftime_iso8601(dt, null_string=False):
    """!
    @brief Given a DateTime object, return an ISO8601 formatted string.
    @param null_string Print "NULL" instead of timestamp if not passed a DateTime object
    """
    try:
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    except (AttributeError, ValueError):
        if null_string:
            return 'NULL'
        raise


def coerce_to_utc(dt):
    """!
    @brief Sets the time zone of a DateTime object to UTC.
    """
    return dt.replace(tzinfo=dateutil.tz.tzutc())


def now_with_timezone():
    """!
    @brief Return a timezone-aware UTC datetime object representing the current timestamp.
    """
    return coerce_to_utc(datetime.datetime.utcnow())


def epoch_with_timezone():
    """!
    @brief Return a timezone-aware datetime object with timestamp zero.
    """
    return eva.coerce_to_utc(datetime.datetime.utcfromtimestamp(0))


def netcdf_time_to_timestamp(time_string):
    """!
    @brief Parses "ncdump -v -t time" time string output into a DateTime object.
    """
    if len(time_string.split()) == 2:
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d %H')
    else:
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d')
    return coerce_to_utc(dt)


def zookeeper_group_id(group_id):
    """!
    @brief Clean up a group_id name so that it can be used in a Zookeeper path.
    """
    g = re.sub(r'[/\000]', '.', group_id)
    g = g.strip('.')
    g = g.encode('ascii', 'ignore')
    g = g.lower()
    if g == b'zookeeper':
        raise eva.exceptions.InvalidGroupIdException('The name "zookeeper" is reserved and cannot be used as a Zookeeper node name.')
    if len(g) == 0:
        raise eva.exceptions.InvalidGroupIdException('The group id "%s" translates to an empty string, which cannot be used as a Zookeeper node name.' % group_id)
    return g.decode('ascii')


def convert_to_bytes(value, notation):
    notations = ['B', 'K', 'M', 'G', 'T']
    for exp, suffix in enumerate(notations):
        if notation.upper() != suffix:
            continue
        return int((1024 ** exp) * float(value))
    raise ValueError('Invalid data size notation %s' % notation)
