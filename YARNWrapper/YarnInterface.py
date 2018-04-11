#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 11:01:23 2018

@author: pradeep

Python wrapper for YARN Applications
TBD: Extended
"""
from knit import Knit

class YARNInterface:
    def __init__(self, app_id):
        self.yarn = Knit(autodetect=True)
        self.yarn.app_id = app_id
        
    def get_AM_logs(self, app_id):
        self.yarn = app_id
        containers = self.yarn.logs.keys()
        appmasterC = containers[0]
        return self.yarn.logs[appmasterC]
