#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 11:01:23 2018

@author: pradeep

The script fetches all the YARN logs for specified application and
provides desired output as per regex pattern
"""

import re
import subprocess
from collections import defaultdict

class YARNLogProcessor:
    def __init__(self, appid, cid=None):
        self.appid = appid
        self.cid = cid
        
    def get_logs(self):        
        cmd = 'yarn logs -applicationId ' + self.appid            
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
        return p.communicate()[0]
        
    def gather_diag_data(self, logs, cid=None):
        p = re.compile("GenericSparkMetricsReporter:.*- "+self.appid+".([^.]+).(.*) : (.*)")
        diag_data = defaultdict(lambda: defaultdict())
        for line in logs.split('\n'):
            if 'GenericSparkMetricsReporter' in line:
                m = p.search(line)
                if m:
                    diag_data[m.group(1)][m.group(2)] = m.group(3)
        return diag_data

if __name__ == '__main__':
    appid = 'application_1523423788079_0004'
    cid = 'container_1523423788079_0004_01_000002'
    ylp = YARNLogProcessor(appid, cid)
    logs = ylp.get_logs()
    diag = ylp.gather_diag_data(logs)
    for key in diag.keys():
        print ">>>>>>>>>>>>>>>>>", key
        for k in sorted(diag[key]):
            print '\t', k, ':', diag[key][k]
        print "=============================================================="
        
        
        
        
