def _granularity_to_ms(time_string):
    magnitude = int(''.join([c for c in time_string if c.isdigit()]))
    unit = ''.join([c for c in time_string if c.isalpha()])
    unit_in_ms = {'s': 1000, 'second': 1000, 'm': 60000, 'minute': 60000, 'h': 3600000, 'hour': 3600000, 'd': 86400000, 'day': 86400000}
    return magnitude * unit_in_ms[unit]
