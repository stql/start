#!/usr/bin/python3.4
"""
This is the script to implement all the example queries.
"""
from stql import *
import os
from datetime import datetime
from operator import add
from collections import defaultdict, Counter

root_dir = '/home/rushui/stql/comparison/data/extracted/'
res_dir = '/home/rushui/stql/comparison/data/result/'
k562_dir = os.path.join(root_dir, 'K562')

timing_file = os.path.join(res_dir, 'timing.txt')

wgEncodeBroadHistoneGm12878H3k04me1StdSigV2 = os.path.join(root_dir, 'wgEncodeBroadHistone',
                                                            'wgEncodeBroadHistoneGm12878H3k04me1StdSigV2')
wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq',
                                                                   'wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1')

gencode_v19_annotation_gtf = os.path.join(root_dir, 'wgEncodeGencode', 'gencode.v19.annotation.gtf')

wgEncodeSydhHistoneHct116H3k04me1UcdPk_narrowPeak = os.path.join(root_dir,
                                                                    'wgEncodeSydhHistone',
                                                                    'wgEncodeSydhHistoneHct116H3k04me1UcdPk.narrowPeak')

wgEncodeSydhHistoneHct116H3k27acUcdPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhHistone',
                                                                         'wgEncodeSydhHistoneHct116H3k27acUcdPk.narrowPeak')

wgEncodeSydhTfbsHelas3CfosStdPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                          'wgEncodeSydhTfbsHelas3CfosStdPk.narrowPeak')
wgEncodeSydhTfbsHelas3CjunIggrabPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                             'wgEncodeSydhTfbsHelas3CjunIggrabPk.narrowPeak')

gm12878 = os.path.join(root_dir, 'wgEncodeSydhTfbs', 'Gm12878.narrowPeak')
k562 = os.path.join(root_dir, 'wgEncodeSydhTfbs', 'K562.narrowPeak')

wgEncodeBroadHistoneK562H3k27acStdSig = os.path.join(root_dir, 'wgEncodeBroadHistone', 'wgEncodeBroadHistoneK562H3k27acStdSig')

wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq', 'wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1')
wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq', 'wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1')

snp135 = os.path.join(root_dir, 'snp', 'snp135.txt')

def make_result_file(q, chr=''):
    q_folder = os.path.join(res_dir, q)
    if not os.path.exists(q_folder):
        os.makedirs(q_folder)
    return os.path.join(q_folder, chr + '_res.txt')

def write_time(a, b, msg):
    with open(timing_file, 'a') as f:
        f.write(msg + '\n')
        f.write(str((b - a).total_seconds()) + ' seconds' + '\n')

##############################################################
# Q1
# select *
# from project T on generate bins with length 100 with vd_sum using each model;
##############################################################
def sq1():
    a = datetime.now()
    q1_res = make_result_file('SQ1')
    write_virtual_project(q1_res, 100, 'vd_sum', read_file(wgEncodeBroadHistoneGm12878H3k04me1StdSigV2), all=False)
    b = datetime.now()
    write_time(a, b, 'SQ1')

######################################################
# Q2
# select *
# from project T1 on
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend
#   from T2 t
#   where t.interval.feature = 'gene') gene with vd_avg using each model;
######################################################
def sq2():
    a = datetime.now()

    q2_res = make_result_file('SQ2')

    def filter_gencode(fname):
            d = defaultdict(set)
            with open(fname) as f:
                for line in f:
                    row = line.rstrip('\n').split('\t')
                    chr = row[0]
                    feature = row[2]
                    start = int(row[3])
                    end = int(row[4])
                    if feature == 'gene':
                        d[chr].add(Interval(start, end))

            for l in d.values():
                l = list(l)
                l.sort(key=lambda x: x.start)
            return d

    dxs = read_file(wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1)
    dys = filter_gencode(gencode_v19_annotation_gtf)
    # assert len(dxs.keys()) == len(dys.keys())

    res = real_project(dxs, dys, 'vd_avg')
    res = OrderedDict(sorted(res.items()))

    write_file(q2_res, res, 4, True)
    b = datetime.now()
    write_time(a, b, 'SQ2')


################################################
# Q3
# select *
# from T1 intersectjoin T2;
################################################
def sq3():
    a = datetime.now()
    q3_res = make_result_file('SQ3')
    dxs = read_file(wgEncodeSydhHistoneHct116H3k04me1UcdPk_narrowPeak, 3)
    dys = read_file(wgEncodeSydhHistoneHct116H3k27acUcdPk_narrowPeak, 3)
    res = defaultdict(list)
    for chr in dys.keys():
        if chr in dxs:
            res[chr] = intersectjoin(dxs[chr], dys[chr])
    write_file(q3_res, res, 3)
    b = datetime.now()
    write_time(a, b, 'SQ3')

###############################################################
# Q4
# select *
# from T1 exclusivejoin
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend
# from T2 t
# where t.interval.feature = 'gene' and
# t.interval.attributes like '%gene_type "protein_coding"%' and
# (t.interval.attributes like '%level 1%' or t.interval.attributes like '%level 2%')) g;
###############################################################
def sq4():
    a = datetime.now()
    q4_res = make_result_file('SQ4')

    def filter_gencode(fname):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr = row[0]
                feature = row[2]
                start = int(row[3])
                end = int(row[4])
                attributes = row[8]
                if feature == 'gene' and 'gene_type "protein_coding"' in attributes and \
                        ('level 1' in attributes or 'level 2' in attributes):
                    d[chr].append(Interval(start, end))
        d = OrderedDict(sorted(d.items()))
        for l in d.values():
            l.sort(key=lambda x: x.start)
        return d

    dxs = read_file(wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1, 3)
    dys = filter_gencode(gencode_v19_annotation_gtf)
    res = defaultdict(list)
    for chr in dxs.keys():
        if chr in dys:
            res[chr] = exclusivejoin(dxs[chr], dys[chr])
        else:
            res[chr] = dxs[chr]
    write_file(q4_res, res, 3)
    b = datetime.now()
    write_time(a, b, 'SQ4')

#########################################################
# select *
# from coalesce
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend, t.interval.value
# from T t
# where t.interval.value > 2) n with vd_avg using each model;
#########################################################
def sq5():
    a = datetime.now()
    q5_res = make_result_file('SQ5')

    def read_filter_file(filename):
        d = defaultdict(list)
        with open(filename) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chrom, start, end, value = row[0:4]
                if float(value) > 2:
                    d[chrom].append(Interval(int(start), int(end), float(value)))
        d = OrderedDict(sorted(d.items()))
        for l in d.values():
            l.sort(key=lambda x: x.start)
        return d

    dxs = read_filter_file(wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1)
    res = defaultdict(list)
    for chr, xs in dxs.items():
        res[chr] = coalesce(xs, 'vd_avg')
    write_file(q5_res, res)
    b = datetime.now()
    write_time(a, b, 'SQ5')

################################################
# Q6
# select *
# from T1 t1, T2 t2
# where t1.interval overlaps with t2.interval;
################################################
def sq6():
    a = datetime.now()
    q6_res = make_result_file('SQ6')

    def readfile(fname):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                d[row[0]].append(row)
        return d

    def cartisian_product(dxs, dys):
        res = defaultdict(list)
        for chr, xs in dxs.items():
            if chr in dys:
                ys = dys[chr]
                for x in xs:
                    for y in ys:
                        x_interval = Interval(x[1], x[2])
                        y_interval = Interval(y[1], y[2])
                        if x_interval.overlaps(y_interval):
                            res[chr].append('\t'.join(map(str, x + y)))
        return res

    def writefile(fname, d):
        with open(fname, 'w') as f:
            for xs in d.values():
                for x in xs:
                    f.write(x + '\n')

    writefile(q6_res, cartisian_product(readfile(wgEncodeSydhTfbsHelas3CfosStdPk_narrowPeak),
                                        readfile(wgEncodeSydhTfbsHelas3CjunIggrabPk_narrowPeak)))
    b = datetime.now()
    write_time(a, b, 'SQ6')

########################################
# Q7
# select *
# from T t
# where t.interval.feature = 'gene' and length(t.interval) > 1000;
########################################
def sq7():
    a = datetime.now()
    q7_res = make_result_file('SQ7')

    def filter_gencode(fname):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr = row[0]
                feature = row[2]
                start = int(row[3])
                end = int(row[4])
                if feature == 'gene' and (end - start + 1) > 1000:
                    d[chr].append(line.rstrip('\n'))
        d = OrderedDict(sorted(d.items()))
        return d

    def writefile(fname, d):
        with open(fname, 'w') as f:
            for xs in d.values():
                for x in xs:
                    f.write(x + '\n')

    writefile(q7_res, filter_gencode(gencode_v19_annotation_gtf))

    b = datetime.now()
    write_time(a, b, 'SQ7')

#######################################
# Q8
# select count(*)
# from T t
# where t.interval.feature = 'gene' and
# t.interval.attributes not like '%gene_type "protein_coding"%';
#######################################
def sq8():
    a = datetime.now()
    q8_res = make_result_file('SQ8')

    cnt = 0
    with open(gencode_v19_annotation_gtf) as f:
        for line in f:
            row = line.rstrip('\n').split('\t')
            feature = row[2]
            attributes = row[8]
            if feature == 'gene' and 'gene_type "protein_coding"' not in attributes:
                cnt += 1

    with open(q8_res, 'w') as f:
        f.write(str(cnt) + '\n')

    b = datetime.now()
    write_time(a, b, 'SQ8')

#########################################
def sq9():
    a = datetime.now()
    q9_res = make_result_file('SQ9')

    snp135 = os.path.join(root_dir, 'snp', 'snp135.txt.chr21')
    gen = os.path.join(root_dir, 'wgEncodeGencode', 'gencode.v19.annotation.gtf.filtered.chr21')
    dsnp135 = read_file(snp135, 3)
    dgen = read_file(gen, 3)
    # dsnp135 = read_file(snp135, 3)
    # gencode_v19_annotation_gtf = os.path.join(root_dir, 'wgEncodeGencode', 'gencode.v19.annotation.gtf.filtered')
    # dgen = read_file(gencode_v19_annotation_gtf, 3)

    res = defaultdict(list)

    for chr in dsnp135.keys():
        if chr in dgen:
            res[chr] = closest(dgen[chr], dsnp135[chr])

    with open(q9_res, 'w') as f:
        for chr in res.keys():
            for x in res[chr]:
                l1 = dsnp135[chr]
                l2 = dgen[chr]
                tuple = str(chr) + '\t' + str(l1[x[0]].start) + '\t' + str(l1[x[0]].end) + '\t' + str(chr) + '\t' + str(l2[x[1]].start) + '\t' + str(l2[x[1]].end) + '\n'
                f.write(tuple)

    b = datetime.now()
    write_time(a, b, 'SQ9')


################################################
# CQ1
# select *
# from discretize ( select * from T1 union all
# select * from T2 union all
# ...
# select * from Tn ) U with vd_sum using each model;
################################################
def cq1():
    a = datetime.now()
    cq1_res = make_result_file('CQ1')

    d = read_narrowpeak(gm12878)
    dt = defaultdict(list)
    for chr, l in d.items():
        dt[chr] = discretize(l, 'vd_sum')
    write_file(cq1_res, dt)
    b = datetime.now()
    write_time(a, b, 'CQ1')

#########################################################################
# CQ2
# insert overwrite table T1
# select *
# from B exclusivejoin P;
# 
# insert overwrite table T2
# select T1.interval.chr, T1.interval.chrstart, T1.interval.chrend
# from T1, G
# where G.interval.level < 3 and distance(T1.interval, G.interval) < 10000;
# 
# insert overwrite table T3
# select *
# from T1 exclusivejoin T2;
#########################################################################
def cq2():
    BAR_Gm12878_merged = os.path.join(root_dir, 'HumanMetaTracks', 'BAR_Gm12878_merged.bed')
    PRM_Gm12878_merged = os.path.join(root_dir, 'HumanMetaTracks', 'PRM_Gm12878_merged.bed')
    a = datetime.now()
    cq2_res = make_result_file('CQ2')

    bar = read_file(BAR_Gm12878_merged, fields_num=3)
    prm = read_file(PRM_Gm12878_merged, fields_num=3)

# Perform exclusivejoin first, this is the first step
# Step 1
    dt1 = defaultdict(list)
    for chr in bar.keys():
        if chr in prm:
            dt1[chr] = exclusivejoin(bar[chr], prm[chr])

# Read related gencode files
# define the function first, and then return what we need.
# Step 2
    def filter_gencode(fname):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr = row[0]
                feature = row[2]
                start = int(row[3])
                end = int(row[4])
                attributes = row[8]
                if feature == 'gene' and ('level 1' in attributes or 'level 2' in attributes):
                    d[chr].append(Interval(start, end))
        d = OrderedDict(sorted(d.items()))
        for l in d.values():
            l.sort(key=lambda x: x.start)
        return d

    dgencode = filter_gencode(gencode_v19_annotation_gtf)
    dt2 = defaultdict(list)
    for chr in dt1.keys():
        if chr in dgencode:
            dt2[chr] = distance_filter(dt1[chr], dgencode[chr], 10000)

    write_file(cq2_res, dt2, fields_number=3)
    b = datetime.now()
    write_time(a, b, 'CQ2')

##################################################
# Step1, 2, 3, 4
##################################################
def cq3():
    a = datetime.now()
    cq3_res = make_result_file('CQ3')

# I can directly go to step 2 to get the result
# Step2
    dt2 = return_virtual_project(100, 'vd_sum', read_narrowpeak(gm12878))

# Step3
    def filter_gencode(fname):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr = row[0]
                feature = row[2]
                start = int(row[3])
                end = int(row[4])
                attributes = row[8]
                if feature == 'gene' and ('level 1' in attributes or 'level 2' in attributes):
                    d[chr].append(Interval(start, end))
        d = OrderedDict(sorted(d.items()))
        for l in d.values():
            l.sort(key=lambda x: x.start)
        return d

    dgencode = filter_gencode(gencode_v19_annotation_gtf)
    dt3 = defaultdict(list)
    for chr in dt2.keys():
        if chr in dgencode:
            dt3[chr] = distance_filter(dt2[chr], dgencode[chr], 10000)

# Step4
    dt4 = defaultdict(list)
    for chr in dt2.keys():
        if chr in dt3:
            dt4[chr] = coalesce(dt3[chr])

    write_file(cq3_res, dt4, fields_number=3)
    b = datetime.now()
    write_time(a, b, 'CQ3')

def cq4():
    a = datetime.now()
    cq4_res = make_result_file('CQ4')
    binSize = 2000

    # chr -> list of list of index, for all files
    d_count = defaultdict(list)
    for f in os.listdir(k562_dir):
        # chr -> list of intervals
        d_interval = read_narrowpeak(os.path.join(k562_dir, f), fields_num=3)
        for chr, xs in d_interval.items():
            d_count[chr].append(count_virtual_project(chr, xs, binSize))

    # for each chr, I have already accumulated the count,
    # so now I should select the interval which count is greater than 2.
    dt3 = defaultdict(list)
    for chr, list_of_xs in d_count.items():
        tmpxs = [0] * len(list_of_xs[0])
        for xs in list_of_xs:
            tmpxs = map(add, tmpxs, xs)
        dt3[chr] = list(tmpxs)

    # dt4, chr -> list of Interval,
    dt4 = defaultdict(list)
    for chr, xs in dt3.items():
        for idx, count in enumerate(xs):
            if count > 2:
                start = idx * binSize + 1
                if idx == len(xs) - 1:
                    end = chr_to_length[chr]
                else:
                    end = (idx + 1) * binSize
                dt4[chr].append(Interval(start, end))

    # Read this file
    dr = read_file(wgEncodeBroadHistoneK562H3k27acStdSig)

    dt5 = defaultdict(list)
    for chr in dt4.keys():
        if chr in dr:
            dt5[chr] = project(dr[chr], dt4[chr])
        else:
            for y in dt4[chr]:
                dt5[chr].append(Interval(y.start, y.end, 0))

    dt6 = defaultdict(list)
    for chr in dt5.keys():
        for x in dt5[chr]:
            if x.value - 3 > 1e-8:
                dt6[chr].append(x)

    dt6 = OrderedDict(sorted(dt6.items()))

    write_file(cq4_res, dt6)
    b = datetime.now()
    write_time(a, b, 'CQ4')


def cq5():
    chrs = ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9', 'chr10', 'chr11', 'chr12', 'chr13',
            'chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19', 'chr20', 'chr21', 'chr22', 'chrM', 'chrX', 'chrY']
    # chrs = ['chr11']
    for chrom in chrs:
        cq5_sub(chrom)

def cq5_sub(chrom):
    a = datetime.now()
    cq5_res = make_result_file('CQ5', chrom)

    # to finish step 1 and 2
    def filter_gencode(fname):
        plus_original_d = defaultdict(list)
        minus_original_d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr = row[0]
                feature = row[2]
                start = int(row[3])
                end = int(row[4])
                strand = row[6]
                attributes = row[8]
                if feature == 'gene' and 'gene_type "protein_coding"' in attributes:
                    if strand == '+':
                        plus_original_d[(chr, start-1500, start+500)].append((start, end, '+'))
                    elif strand == '-':
                        minus_original_d[(chr, end-500, end+1500)].append((start, end, '-'))

        distinct_set = plus_original_d.keys() | minus_original_d.keys()
        interval_d = defaultdict(list)

        for x in distinct_set:
            interval_d[x[0]].append(Interval(x[1], x[2]))

        for l in interval_d.values():
            l.sort(key=lambda x: x.start)

        return plus_original_d, minus_original_d, interval_d

# computer for each chr, because our machine doesn't have enough memory to hold the total files and intermediate results
    plus_original_d, minus_original_d, interval_d = filter_gencode(gencode_v19_annotation_gtf + '_' + chrom)

    wgEncodeSydhTfbsGm12878JundIggrabSig = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                        'wgEncodeSydhTfbsGm12878JundIggrabSig_' + chrom)
    wgEncodeSydhTfbsGm12878InputStdSig = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                      'wgEncodeSydhTfbsGm12878InputStdSig_' + chrom)
    wgEncodeSydhTfbsK562JundIggrabSig = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                     'wgEncodeSydhTfbsK562JundIggrabSig_' + chrom)
    wgEncodeSydhTfbsK562InputStdSig = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                   'wgEncodeSydhTfbsK562InputStdSig_' + chrom)

    res = defaultdict(list)

    dwgEncodeSydhTfbsGm12878JundIggrabSig = read_file(wgEncodeSydhTfbsGm12878JundIggrabSig)
    dwgEncodeSydhTfbsGm12878InputStdSig = read_file(wgEncodeSydhTfbsGm12878InputStdSig)
    dwgEncodeSydhTfbsK562JundIggrabSig = read_file(wgEncodeSydhTfbsK562JundIggrabSig)
    dwgEncodeSydhTfbsK562InputStdSig = read_file(wgEncodeSydhTfbsK562InputStdSig)

    d1 = real_project(dwgEncodeSydhTfbsGm12878JundIggrabSig, interval_d)
    d2 = real_project(dwgEncodeSydhTfbsGm12878InputStdSig, interval_d)
    d3 = real_project(dwgEncodeSydhTfbsK562JundIggrabSig, interval_d)
    d4 = real_project(dwgEncodeSydhTfbsK562InputStdSig, interval_d)

    for chr in interval_d.keys():
        l1 = d1[chr]
        l2 = d2[chr]
        l3 = d3[chr]
        l4 = d4[chr]
        for i in range(len(l1)):
            if l3[i].value - l4[i].value != 0:
                if (l1[i].value - l2[i].value) / (l3[i].value - l4[i].value) - 2 > 1e-8:
                    if (chr, l1[i].start, l1[i].end) in plus_original_d:
                        res[chr].extend(plus_original_d[(chr, l1[i].start, l1[i].end)])
                    if (chr, l1[i].start, l1[i].end) in minus_original_d:
                        res[chr].extend(minus_original_d[(chr, l1[i].start, l1[i].end)])

    res = OrderedDict(sorted(res.items()))
    for l in res.values():
        l.sort(key=lambda x: x[0])

    with open(cq5_res, 'w') as f:
        for chr, l in res.items():
            for x in l:
                f.write(str(chr) + '\t' + str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2]) + '\n')

    b = datetime.now()
    write_time(a, b, 'CQ5_' + chrom)


def cq6():
    a = datetime.now()
    cq6_res = make_result_file('CQ6')

    def read_special_file(fname, left):
        d = defaultdict(list)
        with open(fname) as f:
            for line in f:
                row = line.rstrip('\n').split('\t')
                chr, start, end, value = row[0:4]
                if float(value) > 2:
                    if left:
                        d[chr].append(Interval(int(start) - 200, int(end) - 200, float(value)))
                    else:
                        d[chr].append(Interval(int(start) + 200, int(end) + 200, float(value)))
        d = OrderedDict(sorted(d.items()))
        for l in d.values():
            l.sort(key=lambda x: x.start)
        return d

    dxs = read_special_file(wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1, left=True)
    dys = read_special_file(wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1, left=False)

    res = defaultdict(list)
    # Notice the order
    for chr in dys.keys():
        if chr in dxs:
            res[chr] = intersectjoin(dxs[chr], dys[chr])
    write_file(cq6_res, res, fields_number=3)
    b = datetime.now()
    write_time(a, b, 'CQ6')

def __main__():
    sq1()
    # sq2()
    # sq3()
    # sq4()
    # sq5()
    # sq6()
    # sq7()
    # sq8()
    # sq9()
    # cq1()
    # cq2()
    # cq3()
    # cq4()
    # cq5()
    # cq6()

if __name__ == '__main__':
    __main__()














