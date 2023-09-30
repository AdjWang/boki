import re
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from multiprocessing import Pool
from functools import partial
from tqdm import tqdm
import pandas as pd

PARALLEL = 1

def percentile(datas):
    def __percentile(data_list, p):
        p = np.percentile(data_list, p, axis=0, method='inverted_cdf')
        if isinstance(p, np.ndarray):
            p = p.tolist()
        return p
    p30 = __percentile(datas, 30)
    p50 = __percentile(datas, 50)
    p70 = __percentile(datas, 70)
    p90 = __percentile(datas, 90)
    p99 = __percentile(datas, 99)
    return p30, p50, p70, p90, p99, max(datas), len(datas)


def print_percentile(title, datas):
    assert len(datas) > 0
    p30, p50, p70, p90, p99, p100, count = percentile(datas)
    if isinstance(p30, np.int64):
        print(f'{title} p30: {p30}; p50: {p50}; p70: {p70}; p90: {p90}; p99: {p99}; p100: {p100}; count: {count}')
    else:
        print(f'{title} p30: {p30:.3f}; p50: {p50:.3f}; p70: {p70:.3f}; p90: {p90:.3f}; p99: {p99:.3f}; p100: {p100:.3f}; count: {count}')


def extract_info_single(pattern, extractor, log):
    found = re.findall(pattern, log)
    if len(found) > 0:
        assert len(found) == 1
        return extractor(found[0])
    else:
        return None
    
def gather_info(logs, pattern, extractor):
    if PARALLEL > 1:
        with Pool(PARALLEL) as pool:
            results = pool.map(partial(extract_info_single, pattern, extractor), tqdm(logs, "regexp"))
            results = [i for i in tqdm(results, "filter") if i != None]
    else:
        results = [extract_info_single(pattern, extractor, log) for log in tqdm(logs, "regexp")]
        results = [i for i in tqdm(results, "filter") if i != None]
    return results


if __name__ == '__main__':
    logs = []
    count = -1
    with open('/tmp/retwis.log', 'r') as f:
        if count == -1:
            logs = f.readlines()
        else:
            for i, line in enumerate(f):
                if i >= count:
                    break
                logs.append(line)

    # 2023/09/30 10:39:40 [STAT] fullCallId=24167244103687 stat=[{1 70} {1 45} {1 44} {2 59} {3 678}]
    # const (
    # 	LogOpType_Append = iota
    # 	LogOpType_Read
    # 	LogOpType_SetAux
    # 	LogOpType_Total
    # )
    def __extract_get_logop_ratio(entry):
        stat_counter = {0:0, 1:0, 2:0, 3:0}
        stat_entries = entry.strip('{}').split('} {')
        for entry in stat_entries:
            op_type, op_delay = entry.split(' ')
            op_type, op_delay = int(op_type), int(op_delay)
            assert op_type in stat_counter
            stat_counter[op_type] += op_delay
        append_latency = stat_counter[0]
        read_latency = stat_counter[1]
        set_aux_latency = stat_counter[2]
        invocation_latency = stat_counter[3]
        assert invocation_latency > 0
        return (append_latency, read_latency, set_aux_latency, invocation_latency,
                append_latency/invocation_latency, read_latency/invocation_latency, set_aux_latency/invocation_latency)
    keys = ('append_latency', 'read_latency', 'set_aux_latency', 'invocation_latency', 'append_ratio', 'read_ratio', 'set_aux_ratio')
    logop_stat = gather_info(logs, r'\[STAT\] fullCallId=\d+ stat=\[(.*?)\]', __extract_get_logop_ratio)
    logop_stat = list(zip(*logop_stat))     # transpose
    if len(logop_stat) > 0:
        assert len(keys) == len(logop_stat)
        print('LogOpStat')
        for idx, v in enumerate(logop_stat):
            print_percentile(keys[idx], v)
        print('')

    # plt.figure()
    # x, CDF_counts = np.unique(append_ratio, return_counts = True)
    # y = np.cumsum(CDF_counts)/np.sum(CDF_counts)
    # plt.plot(x, y)
    # plt.savefig('/tmp/queue_prof.png')


    # pop_statistics = []
    # for log in logs:
    #     found = re.findall(r'pop empty=(\d+)', log)
    #     if len(found) > 0:
    #         assert len(found) == 1
    #         pop_statistics.append(int(found[0]))

    # p50, p90, p99, p100, count = percentile(pop_statistics)
    # print(f'p50: {p50}; p90: {p90}; p99: {p99}; p100: {p100}; count: {count}')
