#!/usr/bin/python
# -*- coding: utf-8 -*--
"""
cdpi_query.app模块的总入口
读入原始DPI数据：
1. 生成中间结果供CRM+ 的cdpi_fetch.app二级分析任务使用
2. 在reducer中入库RTB的结果
"""
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('.')
import ConfigParser
import subprocess
import os
import logging
import datetime

# V1.7: 这个文件将取代job.sh，作为cdpi_query.app等模块任务的入口。
# baichuan.sh 中将直接调用这个文件，完成所需的一系列逻辑判断和任务执行工作。

current_path = os.path.split(os.path.realpath(__file__))[0] + '/'

config_parser = ConfigParser.ConfigParser()
# 将设置中的值设置进入环境变量
config_parser.optionxform = str
config_parser.read(current_path + os.sep +'config.ini')

for section_name in config_parser.sections():
    for key, value in config_parser.items(section_name):
        print(key + " : " + value)
        os.environ[key] = value

# WARN: do not move up this import
import get_mr_in_path

MAX_BATCH_MR_JOBS = os.environ.get('MAX_BATCH_MR_JOBS', 5)
try:
    MAX_BATCH_MR_JOBS = int(MAX_BATCH_MR_JOBS)
except ValueError:
    MAX_BATCH_MR_JOBS = 5
    logging.error('MAX_BATCH_MR_JOBS should be an Integer, using default: %d', MAX_BATCH_MR_JOBS)

print 'MAX_BATCH_MR_JOBS: %d' % MAX_BATCH_MR_JOBS

OUT_PATH = os.environ.get('OUT_PATH', "bcdata/user_tag")

print 'OUT_PATH: %s' % OUT_PATH

try:
    job_day = sys.argv[1]
except IndexError:
    job_day = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

print 'run_query_jobs.py: Param1: day (%s);' % (job_day)

current_path = os.path.split(os.path.realpath(__file__))[0] + '/'

# 并发度控制容器
batch_process = set()

# 总任务控制容器
all_query_jobs = set()

# 移动网CRM+ RTB任务批量启动

print 'run job'

# 用环境变量或者参数的形式将IN_PATH传递进去
input_files = get_mr_in_path.generate_input_files(day=job_day)
print 'ARG input_files:', input_files
if not input_files:
    print 'no files, job will not start.'
    exit(-1)

job_time = datetime.datetime.strptime(job_day, '%Y%m%d')
prev_day_time = job_time + datetime.timedelta(days=-1)
prev_day = prev_day_time.strftime('%Y%m%d')
prev_files = get_mr_in_path.get_prev_output_files(day=prev_day, out_path=OUT_PATH)
print 'ARG prev_files: ', prev_files
if prev_files:
    input_files = ','.join([input_files, prev_files])

print 'FINAL input files: ', input_files

query_job = subprocess.Popen(['sh', current_path + 'job.sh', job_day, OUT_PATH, input_files])
# query_job = subprocess.Popen(['echo', province, input_files])
all_query_jobs.add(query_job)
batch_process.add(query_job)
if len(batch_process) == MAX_BATCH_MR_JOBS:
    exit_codes = [p.wait() for p in batch_process]
    print exit_codes
    batch_process.clear()

exit_codes = [p.wait() for p in all_query_jobs]
print exit_codes
all_query_jobs.clear()
