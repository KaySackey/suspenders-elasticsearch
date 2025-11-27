# This function is needed because data descriptors must be defined on a class
# object, not an instance, to have any effect.
import pendulum
import time
from pytz import utc


def epoch_seconds(datetime_obj):
    """Helper function.
    Returns the number of seconds from the epoch to date."""
    return int(time.mktime(datetime_obj.timetuple()) * 1000)


def convert_str_to_datetime(obj):
    """
    Uses the same logic as ElasticSearch
    """

    if isinstance(obj, str):
        # All dates from ElasticSearch are UTC
        # Udate will give us a timezone of 0+0 which is equviilent
        obj = pendulum.parse(obj).replace(tzinfo=utc)

    return obj
