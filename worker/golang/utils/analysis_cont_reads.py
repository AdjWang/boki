import re
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from multiprocessing import Pool
from functools import partial
from tqdm import tqdm
import pandas as pd

PARALLEL = 8

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
    if not isinstance(p30, np.float64):
        print(f'{title} p30: {p30}; p50: {p50}; p70: {p70}; p90: {p90}; p99: {p99}; p100: {p100}; count: {count}')
    else:
        print(f'{title} p30: {p30:.3f}; p50: {p50:.3f}; p70: {p70:.3f}; p90: {p90:.3f}; p99: {p99:.3f}; p100: {p100:.3f}; count: {count}')


def gather_info(logs, extractor):
    if PARALLEL > 1:
        with Pool(PARALLEL) as pool:
            results = pool.map(extractor, tqdm(logs, "regexp"))
            results = [i for i in tqdm(results, "filter") if i != None]
    else:
        results = [extractor(log) for log in tqdm(logs, "regexp")]
        results = [i for i in tqdm(results, "filter") if i != None]
    return results


class GetTxnLine:
    def __init__(self, log_line):
        self.__summary = []
        for log in log_line:
            delay = int(re.findall(r'delay:(\d+)', log)[0])
            statHint = int(re.findall(r'statHint:([\-0-9]+)', log)[0])
            self.__summary.append((delay, statHint))
    
    @property
    def cache_hit_ratio(self):
        return sum([i[1] == 0 for i in self.__summary]) / len(self.__summary)

    @property
    def min_latency(self):
        return min(self.__summary, key=lambda x: x[0])

    @property
    def max_latency(self):
        return max(self.__summary, key=lambda x: x[0])

    @property
    def total_latency(self):
        return sum([i[0] for i in self.__summary])

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

    # [PROF] GetTxn [{stackDepth:0 querySeqNum:4295385163 seqNum:4295385164 opObjName:post:0001c0300000012d delay:14 statHint:0} {stackDepth:0 querySeqNum:4295385163 seqNum:4295385164 opObjName:userid:00000c65 delay:15 statHint:0} {stackDepth:0 querySeqNum:4295385163 seqNum:4295385164 opObjName:userid:000009ae delay:20 statHint:0} {stackDepth:0 querySeqNum:4295385163 seqNum:4295385164 opObjName:userid:00000903 delay:20 statHint:0} {stackDepth:0 querySeqNum:4295385163 seqNum:4295385164 opObjName:userid:00000036 delay:3451 statHint:-1}]
    def __extract_get_query_ratio(entry):
        res1 = re.findall(r'.*?GetTxn.*?\[(.*)\]', entry)
        if len(res1) == 0:
            return None
        assert len(res1) == 1
        record_line = res1[0]
        entries = re.findall(r'\{(.*?)\}', record_line)
        line = GetTxnLine(entries)
        min_latency, max_latency, total_latency = line.min_latency, line.max_latency, line.total_latency
        return (min_latency, max_latency, max_latency[0]/total_latency, total_latency, line.cache_hit_ratio)
    keys = ('min_latency', 'max_latency', 'max_latency_ratio', 'total_latency', 'cache_hit_ratio')
    query_stat = gather_info(logs, __extract_get_query_ratio)
    query_stat = list(zip(*query_stat))     # transpose
    if len(query_stat) > 0:
        assert len(keys) == len(query_stat)
        print('LogOpStat')
        for idx, v in enumerate(query_stat):
            print_percentile(keys[idx], v)
        print('')

    # def __extract_get(entry):
    #     read_latency, read_count, apply_count, txn_read_count, txn_apply_count = entry
    #     read_latency, read_count, apply_count, txn_read_count, txn_apply_count = \
    #         int(read_latency), int(read_count), int(apply_count), int(txn_read_count), int(txn_apply_count)
    #     return ((read_latency, read_count), read_count, apply_count, txn_read_count, txn_apply_count)
    # keys = ('read_latency', 'read_count', 'apply_count', 'txn_read_count', 'txn_apply_count')
    # get_info = extract_info(logs, r'Get=nil read=(\d+) count r/a=\((\d+) (\d+)\) txn_r/a=\((\d+) (\d+)\)', __extract_get)
    # if len(get_info) > 0:
    #     assert len(keys) == len(get_info)
    #     print('GetNormal')
    #     for idx, v in enumerate(get_info):
    #         print_percentile(keys[idx], v)
    #     print('')

    # def __extract_set_normal(entry):
    #     total_latency, append_latency, read_latency, read_count, apply_count, txn_read_count, txn_apply_count = entry
    #     total_latency, append_latency, read_latency, read_count, apply_count, txn_read_count, txn_apply_count = \
    #         int(total_latency), int(append_latency), int(read_latency), int(read_count), int(apply_count), int(txn_read_count), int(txn_apply_count)
    #     return (total_latency, append_latency, (read_latency, read_count), read_count, apply_count, txn_read_count, txn_apply_count)
    # keys = ('total_latency', 'append_latency', 'read_latency', 'read_count', 'apply_count', 'txn_read_count', 'txn_apply_count')
    # set_normal_info = extract_info(logs, r'SetNormal=(\d+) append=(\d+) read=(\d+) count r/a=\((\d+) (\d+)\) txn_r/a=\((\d+) (\d+)\)', __extract_set_normal)
    # if len(set_normal_info) > 0:
    #     assert len(keys) == len(set_normal_info)
    #     print('SetNormal')
    #     for idx, v in enumerate(set_normal_info):
    #         print_percentile(keys[idx], v)
    #     print('')

    # def __extract_txn(entry):
    #     commit_latency, append_latency, commit_read_latency, txn_read_count, txn_apply_count = entry
    #     commit_latency, append_latency, commit_read_latency, txn_read_count, txn_apply_count = \
    #         int(commit_latency), int(append_latency), int(commit_read_latency), int(txn_read_count), int(txn_apply_count)
    #     return (commit_latency, append_latency, (commit_read_latency, txn_read_count), txn_read_count, txn_apply_count)
    # keys = ('commit_latency', 'append_latency', 'commit_read_latency', 'txn_read_count', 'txn_apply_count')
    # txn_info = extract_info(logs, r'Txn=(\d+) append=(\d+) commit=(\d+) count txn_r/a=\((\d+) (\d+)\)', __extract_txn)
    # if len(txn_info) > 0:
    #     assert len(keys) == len(txn_info)
    #     print('TxnCommit')
    #     for idx, v in enumerate(txn_info):
    #         print_percentile(keys[idx], v)
    #     print('')

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

