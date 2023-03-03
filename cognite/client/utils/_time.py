
import numbers
import re
import time
from datetime import datetime, timedelta, timezone
UNIT_IN_MS_WITHOUT_WEEK = {'s': 1000, 'm': 60000, 'h': 3600000, 'd': 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, 'w': 604800000}
MIN_TIMESTAMP_MS = (- 2208988800000)
MAX_TIMESTAMP_MS = 4102444799999

def datetime_to_ms(dt):
    return int((1000 * dt.timestamp()))

def ms_to_datetime(ms):
    if (not (MIN_TIMESTAMP_MS <= ms <= MAX_TIMESTAMP_MS)):
        raise ValueError(f'Input ms={ms} does not satisfy: {MIN_TIMESTAMP_MS} <= ms <= {MAX_TIMESTAMP_MS}')
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms))

def time_string_to_ms(pattern, string, unit_in_ms):
    pattern = pattern.format('|'.join(unit_in_ms))
    res = re.fullmatch(pattern, string)
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        return (magnitude * unit_in_ms[unit])
    return None

def granularity_to_ms(granularity):
    ms = time_string_to_ms('(\\d+)({})', granularity, UNIT_IN_MS_WITHOUT_WEEK)
    if (ms is None):
        raise ValueError(f"Invalid granularity format: `{granularity}`. Must be on format <integer>(s|m|h|d). E.g. '5m', '3h' or '1d'.")
    return ms

def granularity_unit_to_ms(granularity):
    granularity = re.sub('^\\d+', '1', granularity)
    return granularity_to_ms(granularity)

def time_ago_to_ms(time_ago_string):
    if (time_ago_string == 'now'):
        return 0
    ms = time_string_to_ms('(\\d+)({})-ago', time_ago_string, UNIT_IN_MS)
    if (ms is None):
        raise ValueError(f"Invalid time-ago format: `{time_ago_string}`. Must be on format <integer>(s|m|h|d|w)-ago or 'now'. E.g. '3d-ago' or '1w-ago'.")
    return ms

def timestamp_to_ms(timestamp):
    if isinstance(timestamp, numbers.Number):
        ms = int(timestamp)
    elif isinstance(timestamp, str):
        ms = (int(round((time.time() * 1000))) - time_ago_to_ms(timestamp))
    elif isinstance(timestamp, datetime):
        ms = datetime_to_ms(timestamp)
    else:
        raise TypeError(f'Timestamp `{timestamp}` was of type {type(timestamp)}, but must be int, float, str or datetime,')
    if (ms < MIN_TIMESTAMP_MS):
        raise ValueError(f'Timestamps must represent a time after 1.1.1900, but {ms} was provided')
    return ms
TIME_ATTRIBUTES = {'start_time', 'end_time', 'last_updated_time', 'created_time', 'timestamp', 'scheduled_execution_time', 'source_created_time', 'source_modified_time'}

def _convert_time_attributes_in_dict(item):
    new_item = {}
    for (k, v) in item.items():
        if (k in TIME_ATTRIBUTES):
            try:
                v = str(datetime.utcfromtimestamp((v / 1000)))
            except (ValueError, OSError):
                pass
        new_item[k] = v
    return new_item

def convert_time_attributes_to_datetime(item):
    if isinstance(item, dict):
        return _convert_time_attributes_in_dict(item)
    if isinstance(item, list):
        new_items = []
        for el in item:
            new_items.append(_convert_time_attributes_in_dict(el))
        return new_items
    raise TypeError('item must be dict or list of dicts')

def align_start_and_end_for_granularity(start, end, granularity):
    remainder = (start % granularity_unit_to_ms(granularity))
    if remainder:
        start -= remainder
    gms = granularity_to_ms(granularity)
    remainder = ((end - start) % gms)
    if remainder:
        end += (gms - remainder)
    return (start, end)

def split_time_range(start, end, n_splits, granularity_in_ms):
    if (n_splits < 1):
        raise ValueError(f'Cannot split into less than 1 piece, got n_splits={n_splits}')
    tot_ms = (end - start)
    if ((n_splits * granularity_in_ms) > tot_ms):
        raise ValueError(f'Given time interval (tot_ms={tot_ms}) could not be split as `n_splits={n_splits}` times `granularity_in_ms={granularity_in_ms}` is larger than the interval itself.')
    delta_ms = (granularity_in_ms * round(((tot_ms / n_splits) / granularity_in_ms)))
    return [*((start + (delta_ms * i)) for i in range(n_splits)), end]
