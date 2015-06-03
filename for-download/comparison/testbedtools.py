#!/usr/bin/python3.4
"""
This is the script to implement all the example queries.
"""
import os
from datetime import datetime
from subprocess import check_call, CalledProcessError
import uuid

from stql import *


# Because for bedtools, we need 0-based, so we must use 0-based files instead of 1-based files.
# So I use another folder to store the original files and result files.
# and the final result is also 0-based.
root_dir = '/home/rushui/stql/comparison/data/bedtools_extracted/'
res_dir = '/home/rushui/stql/comparison/data/bedtools_result'
bedtools = '/home/rushui/Develop/bedtools2/bin/bedtools'
k562_dir = os.path.join(root_dir, 'K562')

timing_file = os.path.join(res_dir, 'timing.txt')

chrom_info = os.path.join(root_dir, 'chromInfo.txt')
wgEncodeBroadHistoneGm12878H3k04me1StdSigV2 = os.path.join(root_dir, 'wgEncodeBroadHistone',
                                                           'wgEncodeBroadHistoneGm12878H3k04me1StdSigV2')
wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq',
                                                                    'wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1')

gencode_v19_annotation_gtf = os.path.join(root_dir, 'wgEncodeGencode', 'gencode.v19.annotation.gtf')

wgEncodeSydhTfbsHelas3CfosStdPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                          'wgEncodeSydhTfbsHelas3CfosStdPk.narrowPeak')
wgEncodeSydhTfbsHelas3CjunIggrabPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhTfbs',
                                                             'wgEncodeSydhTfbsHelas3CjunIggrabPk.narrowPeak')

gm12878 = os.path.join(root_dir, 'wgEncodeSydhTfbs', 'Gm12878.narrowPeak')
k562 = os.path.join(root_dir, 'wgEncodeSydhTfbs', 'K562.narrowPeak')

wgEncodeBroadHistoneK562H3k27acStdSig = os.path.join(root_dir, 'wgEncodeBroadHistone',
                                                     'wgEncodeBroadHistoneK562H3k27acStdSig')

wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq',
                                                               'wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1')
wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1 = os.path.join(root_dir, 'wgEncodeCshlLongRnaSeq',
                                                                'wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1')

def make_result_file(q):
    q_folder = os.path.join(res_dir, q)
    if not os.path.exists(q_folder):
        os.makedirs(q_folder)
    return os.path.join(q_folder, 'res.txt')

def write_time(a, b, msg):
    with open(timing_file, 'a') as f:
        f.write(msg + '\n')
        f.write(str((b - a).total_seconds()) + ' seconds' + '\n')

def create_tmpfile():
    fname = str(uuid.uuid4())
    new = os.path.join('/tmp', fname)
    return new

# It is also 0-based
def generate_binfile(bin_size):
    f1 = create_tmpfile()
    lines = []
    for k in sorted(chr_to_length.keys()):
        v = chr_to_length[k]
        idx = v // bin_size
        for i in range(0, idx + 1):
            chrom = k
            start = i * bin_size
            if i == idx:
                end = v
            else:
                end = (i + 1) * bin_size
            line = str(chrom) + '\t' + str(start) + '\t' + str(end) + '\n'
            lines.append(line)

    with open(f1, 'w') as f:
        for line in lines:
            f.write(line)

    return f1

##############################################################
# Q1
# I can only use this command to output the bins with a value greater than 0, I can't output the value with 0
# Of course, we can write simple scripts to output other 0 values.
# Anyway, we can use this tool to finish this query.
##############################################################
def q1():
    a = datetime.now()
    q1_res = make_result_file('Q1')
    cmd = bedtools + ' makewindows -g ' + chrom_info + ' -w 100 | ' + bedtools + ' intersect -a ' + \
        wgEncodeBroadHistoneGm12878H3k04me1StdSigV2 + ' -b stdin -wb | awk \'BEGIN {OFS="\\t"} {print $5, $6, $7, ' \
        '$4*($3-$2)/($7-$6)}\' | ' + bedtools + ' groupby -g 1-3 -c 4 ' \
        '-o sum > ' + q1_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q1')

######################################################
# Q2
# select *
# from project T1 on
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend
#   from T2 t
#   where t.interval.feature = 'gene') gene with vd_avg using each model;
# It also doesn't output the 0-value, but it still can finish the core jobs.
######################################################
def q2():
    a = datetime.now()
    q2_res = make_result_file('Q2')
    cmd = 'awk \'BEGIN {OFS="\\t"} {if ($3=="gene") print $1,$4-1,$5}\' ' + gencode_v19_annotation_gtf + ' | ' + \
          bedtools + ' intersect -a ' + wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1 + ' -b stdin -wb | ' + 'awk \'BEGIN {OFS="\\t"} {print $5, $6, $7, $4*($3-$2)/($7-$6)}\' | sort -k1,1 -k2,2n -k3,3n | ' + bedtools + ' groupby -g 1-3 -c 4 -o sum > ' + q2_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q2')

################################################
# Q3
# select *
# from T1 intersectjoin T2;
################################################
def q3():
    wgEncodeSydhHistoneHct116H3k04me1UcdPk_narrowPeak = os.path.join(root_dir,
                                                                     'wgEncodeSydhHistone',
                                                                     'wgEncodeSydhHistoneHct116H3k04me1UcdPk.narrowPeak')

    wgEncodeSydhHistoneHct116H3k27acUcdPk_narrowPeak = os.path.join(root_dir, 'wgEncodeSydhHistone',
                                                                    'wgEncodeSydhHistoneHct116H3k27acUcdPk.narrowPeak')
    a = datetime.now()
    q3_res = make_result_file('Q3')
    cmd = bedtools + ' intersect -a ' + wgEncodeSydhHistoneHct116H3k04me1UcdPk_narrowPeak + ' -b ' + wgEncodeSydhHistoneHct116H3k27acUcdPk_narrowPeak + ' -sorted | cut -f1-3 > ' + q3_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q3')

###############################################################
# Q4
# select *
# from T1 exclusivejoin
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend
# from T2 t
# where t.interval.feature = 'gene' and
# t.interval.attributes like '%gene_type "protein_coding"%' and
# (t.interval.attributes like '%level 1%' or t.interval.attributes like '%level 2%')) g;
# We use subtract command to finish this.
###############################################################
def q4():
    a = datetime.now()
    q4_res = make_result_file('Q4')
    cmd = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if (($3=="gene") && ($9 ~ /gene_type \\"protein_coding\\"/) && (($9 ~ /level 1/) || ($9 ~ /level 2/))) print $1, $4-1, $5}\' ' + gencode_v19_annotation_gtf + ' | sort -k1,1 -k2,2n -k3,3n | ' + bedtools + ' subtract -a ' + wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1 + ' -b stdin | awk \'{print $1, $2, $3}\' > ' + q4_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q4')

#########################################################
# select *
# from coalesce
# (select t.interval.chr, t.interval.chrstart, t.interval.chrend, t.interval.value
# from T t
# where t.interval.value > 2) n with vd_avg using each model;
# We use akw and bedtools to finish it
#########################################################
def q5():
    a = datetime.now()
    q5_res = make_result_file('Q5')
    cmd = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if ($4 > 2) print}\' ' + wgEncodeCshlLongRnaSeqGm12878CellTotalPlusRawSigRep1 + ' | ' + bedtools + ' merge -i stdin > ' + q5_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q5')

################################################
# Q6
# select *
# from T1 t1, T2 t2
# where t1.interval overlaps with t2.interval;
################################################
def q6():
    a = datetime.now()
    q6_res = make_result_file('Q6')
    cmd = bedtools + ' intersect -a ' + wgEncodeSydhTfbsHelas3CfosStdPk_narrowPeak + ' -b ' + wgEncodeSydhTfbsHelas3CjunIggrabPk_narrowPeak + ' -wa -wb > ' + q6_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q6')

########################################
# Q7
# select *
# from T t
# where t.interval.feature = 'gene' and length(t.interval) > 1000;
########################################
def q7():
    a = datetime.now()
    q7_res = make_result_file('Q7')
    cmd = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if (($3=="gene") && ($5 - $4 + 1 > 1000)) print}\' ' + gencode_v19_annotation_gtf + ' > ' + q7_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q7')

#######################################
# Q8
# select count(*)
# from T t
# where t.interval.feature = 'gene' and
# t.interval.attributes not like '%gene_type "protein_coding"%';
#######################################
def q8():
    a = datetime.now()
    q8_res = make_result_file('Q8')
    cmd = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if (($3=="gene") && !($9 ~ /gene_type \\"protein_coding\\"/)) print}\' ' + gencode_v19_annotation_gtf + ' | wc -l > ' + q8_res
    try:
        check_call(cmd, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'Q8')


def cq4():
    a = datetime.now()
    cq4_res = make_result_file('CQ4')
    bin_file = create_tmpfile()
    t2_file = create_tmpfile()
    cmd1 = bedtools + ' makewindows -g ' + chrom_info + ' -w 2000 > ' + bin_file
    cmds = []
    cmd2 = 'sort -k1,1 -k2,2n -k3,3n ' + t2_file + ' | ' + bedtools + ' groupby -g 1,2,3 -c 4 -o count | awk \'BEGIN {FS="\\t";OFS="\\t"} {if ($4 > 2) print $1,$2,$3}\' ' + ' | ' + bedtools + ' intersect -a ' + wgEncodeBroadHistoneK562H3k27acStdSig + ' -b stdin -wb | awk \'BEGIN {FS="\\t";OFS="\\t"} {print $5, $6, $7, $4*($3-$2)/($7-$6)}\' ' + ' | sort -k1,1 -k2,2n -k3,3n | ' + bedtools + ' groupby -g 1-3 -c 4 -o sum | awk \'BEGIN {FS="\\t";OFS="\\t"} {if ($4 > 3) print}\' > ' + cq4_res
    for f in os.listdir(k562_dir):
        each_file = os.path.join(k562_dir, f)
        cmd = bedtools + ' intersect -a ' + each_file + ' -b ' + bin_file + ' -wb | awk \'BEGIN {OFS="\\t"} {print $11, $12, $13, $7*($3-$2)/($13-$12)}\' | ' + bedtools + ' groupby -g 1-3 -c 4 -o sum >> ' + t2_file
        cmds.append(cmd)
    try:
        check_call(cmd1, shell=True)
        for cmd in cmds:
            check_call(cmd, shell=True)
        check_call(cmd2, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'CQ4')
    os.remove(bin_file)
    os.remove(t2_file)


def cq6():
    a = datetime.now()
    cq6_res = make_result_file('CQ6')
    f1 = create_tmpfile()
    f2 = create_tmpfile()
    cmd1 = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if (($4 > 2) && ($2-200 > 0) && ($3-200 > 0)) print $1, $2-200, $3-200}\' ' + wgEncodeCshlLongRnaSeqK562CellPapPlusRawSigRep1 + ' | sort -k1,1 -k2,2n > ' + f1
    cmd2 = 'awk \'BEGIN {FS="\\t";OFS="\\t"} {if ($4 > 2) print $1, $2+200, $3+200}\' ' + wgEncodeCshlLongRnaSeqK562CellPapMinusRawSigRep1 + ' | sort -k1,1 -k2,2n > ' + f2
    cmd3 = bedtools + ' intersect -a ' + f1 + ' -b ' + f2 + ' -sorted > ' + cq6_res
    try:
        check_call(cmd1, shell=True)
        check_call(cmd2, shell=True)
        check_call(cmd3, shell=True)
    except CalledProcessError:
        pass
    b = datetime.now()
    write_time(a, b, 'CQ6')
    os.remove(f1)
    os.remove(f2)

def __main__():
    q1()
    # q2()
    # q3()
    # q4()
    # q5()
    # q6()
    # q7()
    # q8()
    # cq2()
    # cq3()
    # cq4()
    # cq6()

if __name__ == '__main__':
    __main__()