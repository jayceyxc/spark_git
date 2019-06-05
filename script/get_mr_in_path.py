#!/usr/bin/python
# -*- coding: utf-8 -*--
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('.')
import time
import os
import os.path
import subprocess
from subprocess import Popen, PIPE,CalledProcessError
# Monkey Patch for python 2.6.6
def check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = Popen(stdout=PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        # here CalledProcessError is different in 2.6.6 from newer versions
        raise CalledProcessError(retcode, cmd)
    return output
if 'check_output' not in dir(subprocess):
    subprocess.check_output = check_output

try:
    dpi_path = os.environ['DPI_PATH']
except KeyError:
    dpi_path = '/daas/bstl/dpiqixin/'

def is_path_exists(hdfs_path):
    file_list = []
    try:
        z = subprocess.check_output(['hadoop', 'fs', '-du', hdfs_path])
    except subprocess.CalledProcessError:
        z = ''
    for line in z.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            du_segs = line.split()
            if len(du_segs) == 2:
                # original hadoop
                file_size, filename = du_segs
            elif len(du_segs) == 3:
                # Cloudera hadoop v 5.x and above
                file_size, replication_size, filename = du_segs
            else:
                continue
            if file_size != '0':
                file_list.append(filename)
        except ValueError:
            continue
    print hdfs_path, len(file_list)
    return len(file_list) > 0


def generate_input_files(day):
    """
    
    :param day: 
    :return: 
    """
    path = '/'.join([dpi_path, day])
    path = path + "/*"
    if not is_path_exists(path):
        path = ''

    file_list_str = ','.join([path]).strip(',')
    return file_list_str


def get_prev_output_files(day, out_path):
    """
    
    :param day: 
    :param path: 
    :return: 
    """
    path='/'.join([out_path, day])
    path = path + "/*"
    if not is_path_exists(path):
        path = ''

    file_list_str = ','.join([path]).strip(',')
    return file_list_str
