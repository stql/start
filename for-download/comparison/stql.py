#!/usr/bin/python3.4
"""
usage: This is a basic library for our stql project.
"""
from functools import reduce
from collections import defaultdict, OrderedDict, deque
from math import ceil
from operator import mul

from interval import Interval
import bisect
import sys


chr_to_length = {
    'chr1': 249250621,
    'chr2': 243199373,
    'chr3': 198022430,
    'chr4': 191154276,
    'chr5': 180915260,
    'chr6': 171115067,
    'chr7': 159138663,
    'chr8': 146364022,
    'chr9': 141213431,
    'chr10': 135534747,
    'chr11': 135006516,
    'chr12': 133851895,
    'chr13': 115169878,
    'chr14': 107349540,
    'chr15': 102531392,
    'chr16': 90354753,
    'chr17': 81195210,
    'chr18': 78077248,
    'chr19': 59128983,
    'chr20': 63025520,
    'chr21': 48129895,
    'chr22': 51304566,
    'chrX': 155270560,
    'chrY': 59373566,
    'chrM': 16571,
    'chrS': 28,
    'chrT': 13
}


def read_file(filename, fields_num=4):
    """
    It assumes the first four columns are chr, start, end, value, we can also specify the fields number,
    in case we don't want the value field
    @param filename: The file to be read
    @type filename: str
    @return: a dictionary, it is sorted by the start of each interval
    @rtype: ['chr', [Interval]]
    """
    d = defaultdict(list)
    with open(filename) as f:
        for line in f:
            row = line.rstrip('\n').split('\t')
            if fields_num == 4:
                chrom, start, end, value = row[0:4]
                d[chrom].append(Interval(int(start), int(end), float(value)))
            elif fields_num == 3:
                chrom, start, end = row[0:3]
                d[chrom].append(Interval(int(start), int(end)))
            else:
                raise Exception('Can not parse your file\n')
    d = OrderedDict(sorted(d.items()))
    for l in d.values():
        l.sort(key=lambda x: x.start)
    return d

def closest(xs, ys):
    '''
    @param xs: the first track, the interval must be sorted by the start
    @type xs:
    @param ys: the second track, the interval must be sorted by the start
    @type ys:
    @return:
    @rtype:
    '''

    p_cursor = 0
    max_left_end = 0
    left_candidates = []
    res = []
    for iy in range(len(ys)):
        y = ys[iy]
        while p_cursor < len(xs) and xs[p_cursor].end < y.start:
            if xs[p_cursor].end > max_left_end:
                max_left_end = xs[p_cursor].end
                left_candidates.clear()
                left_candidates.append(p_cursor)
            elif xs[p_cursor].end == max_left_end:
                left_candidates.append(p_cursor)
            p_cursor += 1
        left_closest_dis = sys.maxsize if max_left_end == 0 else y.start - max_left_end
        if p_cursor == len(xs):
            for candidate in left_candidates:
                res.append([iy, candidate])
        elif xs[p_cursor].overlaps(y):
            q_cursor = p_cursor
            while q_cursor < len(xs) and not xs[q_cursor].start > y.end:
                if xs[q_cursor].overlaps(y):
                    res.append([iy, q_cursor])
                q_cursor += 1
        else:
            right_closest_dis = xs[p_cursor].start - y.end
            if left_closest_dis > right_closest_dis:
                q_cursor = p_cursor
                right_min_start = xs[p_cursor].start
                while q_cursor < len(xs) and xs[q_cursor].start == right_min_start:
                    res.append([iy, q_cursor])
                    q_cursor += 1
            elif left_closest_dis == right_closest_dis:
                for candidate in left_candidates:
                    res.append([iy, candidate])
                q_cursor = p_cursor
                right_min_start = xs[p_cursor].start
                while q_cursor < len(xs) and xs[q_cursor].start == right_min_start:
                    res.append([iy, q_cursor])
                    q_cursor += 1
            else:
                for candidate in left_candidates:
                    res.append([iy, candidate])
    return res


def read_narrowpeak(fname, fields_num=4):
    """
    @param fname: the narrowPeak file
    @type fname:
    @return: a dictionary, chr -> list of Intervals, the chrs are sorted, in each list, the Interval are sorted by the start
    @rtype: chr:[Interval]
    """
    d = defaultdict(list)
    with open(fname) as f:
        for line in f:
            row = line.rstrip('\n').split('\t')
            chr = row[0]
            start = row[1]
            end = row[2]
            value = row[6]
            if fields_num == 4:
                d[chr].append(Interval(int(start), int(end), float(value)))
            elif fields_num == 3:
                d[chr].append(Interval(int(start), int(end)))
    d = OrderedDict(sorted(d.items()))
    for l in d.values():
        l.sort(key=lambda x: x.start)
    return d


def write_file(fname, d, fields_number=4, remove_zero=False):
    '''
    Given a dictionary, with chr and a list of Intervals, write the final result into disk
    @param fname:
    @type fname:
    @param d:
    @type d:
    @return:
    @rtype:
    '''
#    d = OrderedDict(sorted(d.items()))
    with open(fname, 'w') as f:
        for chr, lst in d.items():
            for interval in lst:
                if remove_zero:
                    if interval.value > 0:
                        f.write(interval.to_chr_string(chr, fields_number) + '\n')
                else:
                    f.write(interval.to_chr_string(chr, fields_number) + '\n')


def divide_track(xs):
    """
    Break the list into several sublists, in each list all intervals overlap with each other
    @param xs: list of all Intervals, all Intervals are sorted before using this function
    @type xs: [(Interval)]
    @return: a list of index, which indicates the first interval index of the original list
    @rtype: []
    """
    res = []
    last = 0
    biggest_end = xs[0].end
    # first the intervals
    for idx in range(1, len(xs)):
        if xs[idx].start <= biggest_end:  # remember to use <= instead of <
            if xs[idx].end > biggest_end:
                biggest_end = xs[idx].end
        else:
            res.append([last, idx])
            biggest_end = xs[idx].end
            last = idx
    else:
        res.append([last, len(xs)])
    return res


def intersectjoin(xss, yss, vd_model='vd_sum'):
    """
    This function implements the intersectjoin operation, it takes two tracks of intervals,
    and find the common parts of overlapped intervals
    @param xs: list of intervals, must be sorted first
    @type xs: [(Intervals)]
    @param ys: list of Intervals, must be sorted first
    @type ys: [(Intervals)]
    @param vd_model: vd_sum, vd_product,
    @type vd_model:
    @return: a new list of Intervals
    @rtype: [(Interval)]
    """
    res = []
    xs = deque(xss)
    ys = deque(yss)
    while xs and ys:
        if xs[0].precedes(ys[0]):
            xs.popleft()
        elif xs[0].follows(ys[0]):
            ys.popleft()
        else:
            res.append(xs[0].gen_overlap(ys[0], vd_model))
            if xs[0].end == ys[0].end:
                xs.popleft()
                ys.popleft()
            elif xs[0].end > ys[0].end:
                ys.popleft()
            else:
                xs.popleft()
    return res


def discretize(xs, vd_model='vd_sum'):
    """
    This function implements the discretize operator
    @param xs: a list of Intervals, must be sorted first
    @type xs: [(Interval)]
    @param vd_model: 5 kinds
    @type vd_model:
    @return: a new list of Intervals, with new value
    @rtype: [(Interval)]
    """
    tmp_res = deque()
    starts = deque()
    ends = deque()
    res = []
    origin_starts = []
    origin_ends = []
    origin_values = []
    start_list = []
    end_list = []
    start_set = set()
    end_set = set()
    for idx_pair in divide_track(xs):
        origin_starts.clear()
        origin_ends.clear()
        origin_values.clear()
        start_list.clear()
        end_list.clear()
        start_set.clear()
        end_set.clear()
        for idx in range(idx_pair[0], idx_pair[1]):
            origin_starts.append(xs[idx].start)
            origin_ends.append(xs[idx].end)
            origin_values.append(xs[idx].value)
            start_set.add(xs[idx].start)
            start_set.add(xs[idx].end + 1)
            end_set.add(xs[idx].start - 1)
            end_set.add(xs[idx].end)
        start_list = sorted(list(start_set))
        end_list = sorted(list(end_set))
        for idx in range(len(start_list) - 1):
            tmp_res.append([start_list[idx], end_list[idx + 1]])

        while tmp_res:
            values = []
            for idx in range(len(origin_starts)):
                if tmp_res[0][0] >= origin_starts[idx]:
                    if tmp_res[0][0] <= origin_ends[idx]:
                        values.append(origin_values[idx])
                else:
                    break

            if vd_model == 'vd_sum':
                value = sum(values)
            elif vd_model == 'vd_avg':
                value = sum(values) / len(values)
            elif vd_model == 'vd_product':
                value = reduce(mul, values, 1)
            elif vd_model == 'vd_max':
                value = max(values)
            elif vd_model == 'vd_min':
                value = min(values)

            res.append(Interval(tmp_res[0][0], tmp_res[0][1], value))
            tmp_res.popleft()

    return res

def exclusivejoin(xss, yss):
    """
    To compute segments covered by the first track, but not by the second track.
    The basic idea is to use the result of intersectjoin, then do some processing
    @param xs: list of Intervals
    @type xs:
    @param ys: list of Intervals
    @type ys:
    @return: new list of Intervals
    @rtype:
    """
    res = []
    xs = deque(xss)
    ys = deque(yss)
    while xs and ys:
        if xs[0].precedes(ys[0]):
            res.append(xs[0])
            xs.popleft()
        elif xs[0].follows(ys[0]):
            ys.popleft()
        elif xs[0].start > ys[0].start:
            if xs[0].end == ys[0].end:
                xs.popleft()
                ys.popleft()
            elif xs[0].end > ys[0].end:
                xs[0] = Interval(ys[0].end + 1, xs[0].end, xs[0].value)
                ys.popleft()
            else:
                xs.popleft()
        elif xs[0].start == ys[0].start:
            if xs[0].end == ys[0].end:
                xs.popleft()
                ys.popleft()
            elif xs[0].end > ys[0].end:
                xs[0] = Interval(ys[0].end + 1, xs[0].end, xs[0].value)
                ys.popleft()
            else:
                xs.popleft()
        elif xs[0].start < ys[0].start:
            res.append(Interval(xs[0].start, ys[0].start - 1, xs[0].value))
            if xs[0].end == ys[0].end:
                xs.popleft()
                ys.popleft()
            elif xs[0].end > ys[0].end:
                xs[0] = Interval(ys[0].end + 1, xs[0].end, xs[0].value)
                ys.popleft()
            else:
                xs.popleft()
    else:
        if xs:
            res.extend(xs)
    return res


def coalesce(xs, vd_model='vd_sum'):
    """
    This function implements the coalesce operator, it is based on discretize.
    @param xs: the list of Intervals
    @type xs:
    @param vd_model:
    @type vd_model:
    @return: list of Intervals
    @rtype:[Interval]
    """
    res1 = discretize(xs, vd_model)
    res = []
    start_idx = 0
    sum = 0
    total_leng = 0
    for idx in range(start_idx, len(res1) - 1):
        if res1[idx].adjacent(res1[idx+1]):
            sum += res1[idx].value * len(res1[idx])
            total_leng += len(res1[idx])
        else:
            sum += res1[idx].value * len(res1[idx])
            total_leng += len(res1[idx])
            res.append(Interval(res1[start_idx].start, res1[idx].end, sum / total_leng))
            start_idx = idx + 1
            sum = 0
            total_leng = 0
    else:
        sum += res1[-1].value * len(res1[-1])
        total_leng += len(res1[-1])
        res.append(Interval(res1[start_idx].start, res1[-1].end, sum / total_leng))
    return res

def project(xs, ys, vd_model='vd_sum'):
    """
    Take two tracks, ys is allowed to miss value field. But both of them are list of Intervals.
    It requires the list of intervals are sorted by start
    @param xss: list of Intervals
    @type xss:
    @param yss: list of Intervals, allowed to miss value field
    @type yss:
    @param vd_model:
    @type vd_model:
    @return: a track
    @rtype: list of Interval, with start, end, and value
    """
    res = []
    xs = discretize(xs, vd_model)
    start_list = []
    for x in xs:
        start_list.append(x.start)
    for y in ys:
        start_idx = bisect.bisect_left(start_list, y.start)
        end_idx = bisect.bisect_left(start_list, y.end)
        if (start_idx == end_idx == 0 and y.end < start_list[0]) or (start_idx == end_idx == len(start_list)):
            res.append(Interval(y.start, y.end, 0))
        else:
            value = 0
            if start_idx == 0:
                if end_idx == len(start_list):
                    real_range = range(start_idx, end_idx)
                else:
                    real_range = range(start_idx, end_idx+1)
            else:
                if end_idx == len(start_list):
                    real_range = range(start_idx-1, end_idx)
                else:
                    real_range = range(start_idx-1, end_idx+1)
            for i in real_range:
                if not (xs[i].end < y.start or xs[i].start > y.end):
                    overlap_start = max(xs[i].start, y.start)
                    overlap_end = min(xs[i].end, y.end)
                    value += xs[i].value * (overlap_end - overlap_start + 1) / len(y)
            res.append(Interval(y.start, y.end, value))
    return res


def distance_filter(xs, ys, max_distance):
    ''' This function takes xs, ys, if there exists an interval in xs that the distance between any intervals of ys and
    this interval is larger than max_distance, we return this new interval.
    For the second, the intervals may have overlap
    @param xs: list of Intervals
    @type xs: guaranteed to be sorted by start
    @param ys: list of Intervals
    @type ys: guaranteed to be sorted by start
    @param max_distance:
    @type max_distance:
    @return:
    @rtype:
    '''
    res = []
    ys = coalesce(ys)  # this is very important to my algorithm
    start_idx = 0
    for x in xs:
        if ys[0].start - x.end >= max_distance or x.start - ys[-1].end >= max_distance:
            res.append(x)
        else:
            if x.end > ys[0].start:
                while start_idx < len(ys):
                    if x.end <= ys[start_idx].start:
                        break
                    else:
                        start_idx += 1
                if start_idx < len(ys):
                    if ys[start_idx].start - x.end >= max_distance and x.start - ys[start_idx - 1].end >= max_distance:
                            res.append(x)
    return res

def virtual_project(chr, xs, binSize, vd_model='vd_sum'):
    """
    @param xs: track, generated from read_file(), only four columns
    @type xs:
    @param binSize: the specified size
    @type binSize:
    @param vd_model: the value model for discretization
    @type vd_model:
    @return: value_list, a list contains each value for output
    @rtype:
    """
    new_xs = discretize(xs, vd_model)
    value_list = [0] * ceil(chr_to_length[chr] / binSize) # Notice the ceil here
    for x in new_xs:
        binIndex = (x.start - 1) // binSize
        if x.end <= (binIndex + 1) * binSize:
            value_list[binIndex] += x.value * len(x)
        else:
            value_list[binIndex] += x.value * (binSize * (binIndex + 1) - x.start + 1)
            binIndex += 1
            while x.end > (binIndex + 1) * binSize:
                value_list[binIndex] += x.value * binSize
                binIndex += 1
            else:
                value_list[binIndex] += x.value * (x.end - binIndex * binSize)
    return value_list

def real_project(dxs, dys, vd_model='vd_sum'):
    """
    It computes projecting one track dxs on another track dys, if some chromosome is not in dxs, but in dys, we also need to output the
    result of intervals with the value set to 0.
    @param dxs:
    @type dxs:
    @param dys:
    @type dys:
    @param vd_model:
    @type vd_model:
    @return:
    @rtype:
    """
    res = defaultdict(list)
    for chr in dys.keys():
        if chr in dxs:
            res[chr] = project(dxs[chr], dys[chr], vd_model)
        else:
            tmp_res = []
            for y in dys[chr]:
                tmp_res.append(Interval(y.start, y.end, 0))
            res[chr] = tmp_res
    return res


def count_virtual_project(chr, xs, binSize):
    '''
    This function is very special, it takes a track(list of intervals), and return a list with values 0 or 1, 0 means no
    interval contributes to the corresponding bin, 1 means at least one interval contributes to the corresponding bin
    It is designed for cq4.
    @param chr:
    @type chr:
    @param xs: list of intervals, no value field
    @type xs:
    @param binSize:
    @type binSize:
    @return: a list, in each list, the value is 0 or 1
    @rtype:
    '''
    value_list = [0] * ceil(chr_to_length[chr] / binSize)
    for x in xs:
        start_index = (x.start - 1) // binSize
        end_index = (x.end - 1) // binSize
        for i in range(start_index, end_index + 1):
            value_list[i] = 1
    return value_list


# Use the last boolean to control whether we should write all information or only non-zero values
def write_virtual_project(fname, binSize, vd_model, d, all=True):
    res = defaultdict(list)
    for chr, lst in d.items():
        res[chr] = virtual_project(chr, lst, binSize, vd_model)
    res = OrderedDict(sorted(res.items()))
    with open(fname, 'w') as f:
        for chr, lst in res.items():
            for idx, val in enumerate(lst):
                if not all and val == 0:
                    continue
                else:
                    start = idx * binSize + 1
                    if idx == len(lst) - 1:
                        end = chr_to_length[chr]
                        value = val / (end - idx * binSize)
                    else:
                        end = (idx + 1) * binSize
                        value = val / binSize
                    tpl = chr + '\t' + str(start) + '\t' + str(end) + '\t' + str(value)
                    f.write(tpl + '\n')


def return_virtual_project(binSize, vd_model, d):
    res = defaultdict(list)
    tmp_res = defaultdict(list)
    for chr, lst in d.items():
        tmp_res[chr] = virtual_project(chr, lst, binSize, vd_model)
    for chr, lst in tmp_res.items():
        for idx, val in enumerate(lst):
            if val == 0:
                continue
            else:
                start = idx * binSize + 1
                if idx == len(lst) - 1:
                    end = chr_to_length[chr]
                    value = val / (end - idx * binSize)
                else:
                    end = (idx + 1) * binSize
                    value = val / binSize
                res[chr].append(Interval(start, end, value))
    return res


def __main__():
    return

if __name__ == "__main__": __main__()






