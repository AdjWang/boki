import re
import numpy as np


def percentile(datas):
    p50 = np.percentile(datas, 50)
    p90 = np.percentile(datas, 90)
    p99 = np.percentile(datas, 99)
    return p50, p90, p99, max(datas), len(datas)


if __name__ == '__main__':
    with open('/tmp/retwis.log', 'r') as f:
        logs = f.readlines()

    statistics = []
    for log in logs:
        found = re.findall(r'dispatchDelay: (\d+) us', log)
        if len(found) > 0:
            assert len(found) == 1
            statistics.append(int(found[0]))

    p50, p90, p99, p100, count = percentile(statistics)
    print(f'p50: {p50}; p90: {p90}; p99: {p99}; p100: {p100}; count: {count}')