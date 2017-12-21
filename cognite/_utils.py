import sys, math

def _granularity_to_ms(time_string):
    magnitude = int(''.join([c for c in time_string if c.isdigit()]))
    unit = ''.join([c for c in time_string if c.isalpha()])
    unit_in_ms = {'s': 1000, 'second': 1000, 'm': 60000, 'minute': 60000, 'h': 3600000, 'hour': 3600000, 'd': 86400000, 'day': 86400000}
    return magnitude * unit_in_ms[unit]


class _ProgressIndicator():
    def __init__(self, tagIds, start, end):
        from cognite.timeseries import get_latest, get_datapoints
        if start == None:
            self.start = float("inf")
            for tag in tagIds:
                if type(tag) == str:
                    tag_start = int(get_datapoints(tag, limit=1).to_json()['timestamp'])
                else:
                    tag_start = int(get_datapoints(tag['tagId'], limit=1).to_json()['timestamp'])
                if tag_start < self.start:
                    self.start = tag_start
        else:
            self.start = start

        if end == None:
            self.end = 0
            for tag in tagIds:
                if type(tag) == str:
                    tag_end = int(get_latest(tag).to_json()['timestamp'])
                else:
                    tag_end = int(get_latest(tag['tagId']).to_json()['timestamp'])
                if tag_end > self.end:
                    self.end = tag_end
        else:
            self.end = end
        self.length = self.end - self.start
        self.progress = 0.0
        print("Downloading requested data...")
        self._print_progress()

    def _update_progress(self, latest_timestamp):
        self.progress = 100 - (((self.end - latest_timestamp) / self.length) * 100)
        self._print_progress()

    def _print_progress(self):
        prog = int(math.ceil(self.progress) / 5)
        remainder = 20 - prog
        sys.stdout.write("\r{:5.1f}% |{}|".format(self.progress, '|' * prog + ' ' * remainder))
        sys.stdout.flush()
        if int(math.ceil(self.progress)) == 100:
            print()
