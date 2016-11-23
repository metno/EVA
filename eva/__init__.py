import logging
import re
import time
import datetime
import dateutil.tz
import traceback

import eva.exceptions


def retry_n(func, args=(), kwargs={}, interval=5, exceptions=(Exception,), warning=1, error=3, give_up=5, logger=logging):
    """
    Call `func(*args, **kwargs)` and, if it throws anything listed in
    `exceptions`, catch it and retry again, up to `give_up` times. If `give_up`
    is `<= 0`, retry indefinitely.

    :param function func: the function to call.
    :param tuple args: positional arguments to send with the function call.
    :param dict kwargs: keyword arguments to send with the function call.
    :param int interval: how long to wait between function calls.
    :param tuple exceptions: tuple of exceptions to catch.
    :param int warning: how many failures before escalating logging output to `WARNING`.
    :param int error: how many failures before escalating logging output to `ERROR`.
    :param int give_up: how many failures to tolerate before giving up.
    :param logging.Logger logger: Logger object where failure messages are sent.

    :raises AssertionError: if either `(error > warning > 0)` or `(give_up > error or give_up <= 0)` are False.
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
    """
    Import a Python module and return it to the caller.

    :param str name: full dotted name to a Python module.
    :rtype: module
    :returns: the imported module.
    """
    components = name.split('.')
    modname = ('.').join(components[0:-1])
    mod = __import__(modname)
    for c in components[1:-1]:
        mod = getattr(mod, c)
    return getattr(mod, components[-1])


def format_exception_as_bug(exception):
    """
    Format an exception backtrace into a list of lines.

    :param Exception exception: the exception to return a backtrace from.
    :rtype: list
    :returns: a list of strings from the exception backtrace.
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
    """
    Print an exception with backtrace to a logging object, and send it as an
    e-mail as well.

    :param Exception exception: the exception instance.
    :param logging.Logger logger: a Logger object used for printing the backtrace.
    :param eva.mail.Mailer mailer: a Mailer object used for sending the backtrace.
    """
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
    """
    Check if `id` is found in `array`, or `array` is empty.

    :param id: the member to check.
    :param list array: the list to check.
    :rtype: bool
    """
    return (array is None) or (len(array) == 0) or (id in array)


def split_comma_separated(string):
    """
    Split a comma-separated string into a list of strings.

    :param str string: string with comma-separated values.
    :rtype: list
    """
    return [x.strip() for x in string.strip().split(',')]


def url_to_filename(url):
    """
    Convert a file:// URL to a path name.
    
    :param str url: URL starting with `file://`.
    :raises RuntimeError: when the URL does not start with `file://`.
    """
    start = 'file://'
    if not url.startswith(start):
        raise RuntimeError('Expected an URL starting with %s, got %s instead' % (start, url))
    return url[len(start):]


def strftime_iso8601(dt, null_string=False):
    """
    Convert a DateTime object into an ISO8601 formatted string.

    :param datetime.datetime.DateTime dt: DateTime object.
    :param bool null_string: if True, print "NULL" instead of timestamp if not passed a DateTime object.
    :rtype: str
    """
    try:
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    except (AttributeError, ValueError):
        if null_string:
            return 'NULL'
        raise


def coerce_to_utc(dt):
    """
    Create a copy of a DateTime object, where the timezone is set to UTC.

    Any existing timezone information is lost in the conversion process.

    :param datetime.datetime.DateTime dt: DateTime object.
    :rtype: datetime.datetime.Datetime
    """
    return dt.replace(tzinfo=dateutil.tz.tzutc())


def now_with_timezone():
    """
    Create a timezone-aware UTC DateTime object with the current time.

    :rtype: datetime.datetime.Datetime
    """
    return coerce_to_utc(datetime.datetime.utcnow())


def epoch_with_timezone():
    """
    Create a timezone-aware DateTime object with timestamp zero.

    :rtype: datetime.datetime.Datetime
    """
    return eva.coerce_to_utc(datetime.datetime.utcfromtimestamp(0))


def netcdf_time_to_timestamp(time_string):
    """
    Parses the "ncdump -v -t time" time string output to a DateTime object.

    :param str time_string: single line of `ncdump` time string output.
    :rtype: datetime.datetime.Datetime
    """
    if len(time_string.split()) == 2:
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d %H')
    else:
        dt = datetime.datetime.strptime(time_string, '%Y-%m-%d')
    return coerce_to_utc(dt)


def zookeeper_group_id(string):
    """
    Clean up a string so that it can be used as a Zookeeper path component.

    :param str string: the string to convert.
    :raises eva.exceptions.InvalidGroupIdException: when the converted string is not suitable for use in ZooKeeper.
    :rtype: str
    """
    g = re.sub(r'[/\000]', '.', string)
    g = g.strip('.')
    g = g.encode('ascii', 'ignore')
    g = g.lower()
    if g == b'zookeeper':
        raise eva.exceptions.InvalidGroupIdException('The name "zookeeper" is reserved and cannot be used as a Zookeeper node name.')
    if len(g) == 0:
        raise eva.exceptions.InvalidGroupIdException('The group id "%s" translates to an empty string, which cannot be used as a Zookeeper node name.' % string)
    return g.decode('ascii')


def convert_to_bytes(value, notation):
    """
    Convert a number into a string, representing a value in bytes, kilobytes,
    megabytes, gigabytes, or terabytes.

    :param int|float value: number to convert.
    :param str notation: size wanted, one of B, K, M, G, T
    :raises ValueError: when the notation parameter is not one of the pre-defined variables.
    :rtype: str
    """
    notations = ['B', 'K', 'M', 'G', 'T']
    for exp, suffix in enumerate(notations):
        if notation.upper() != suffix:
            continue
        return int((1024 ** exp) * float(value))
    raise ValueError('Invalid data size notation %s' % notation)
