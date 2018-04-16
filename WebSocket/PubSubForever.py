#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 14:52:12 2018

@author: pradeep

The script subscribes to the specified topics and continuously records the
frames. The recorded data is saved in the respective topic files.
'--topics' is space separated list of topics
'--socket' is the socket to which subscriptions go
'--flush' is #frames after which the frames are written to file
'--unsubscribe' is #frames after which a random topic is unsubscribed
"""
import argparse, json, logging, os
import websocket
from random import randint
from collections import defaultdict

class PubSubForever:
    def __init__(self, topics, socket, wrt = 10, uns = 15):
        self.cnt = self.old = self.new = 0
        self.ws = None
        self.wrt = wrt
        self.uns = uns
        self.topics = topics
        self.socket = socket        
        self.store = defaultdict(list)
        self.log = self.get_logger()
        self.log.info("INPUTS: Topics: "+str(topics)+" Socket: " +
                      socket+", Write: "+str(wrt)+", Unsubsscribe: "+str(uns))
        
    def on_message(self, ws, message):
        self.cnt += 1
        data = json.loads(message)
        topic = data['topic']
        if self.cnt % 100 == 0:
            self.log.info(str(self.cnt) + " " + topic)
        self.store[topic].append(data)
        if self.cnt % self.wrt == 0:
            self.write_to_file()
        if self.cnt % self.uns == 0:
            self.unsubscribe()
            
    def on_error(self, ws, error):
        self.log.error(str(error) + "Exiting the infinite loop!")
    
    def on_close(self, ws):
        self.ws.close()
    
    def on_open(self, ws):
        self.log.info("Subscribing topics:")
        for t in self.topics:
            self.ws.send('{"type":"subscribe","topic":"%s"}' % t)
            self.log.info("\tSubscribed: " + t)
        self.log.info("All topics subscribed!")
                
    def get_logger(self):
        #FORMAT = "[%(asctime)s %(levelname)s %(filename)s:%(lineno)s "
        #FORMAT = FORMAT + "%(funcName)10s()] %(message)s"
        FORMAT = "%(asctime)s %(levelname)8s%(lineno)3s %(message)s"
        formatter = logging.Formatter(FORMAT)
        log_name = os.path.join(os.path.basename(__file__)+".log")
        logging.basicConfig(format=FORMAT, filename=str(log_name))        
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
#        logger.addHandler(logging.StreamHandler().setFormatter(formatter))
        return logger
        
    def write_to_file(self):
        self.log.info("Writing to topic files")
        for t in self.topics:
            fh = open(t+".txt", "a+")
            for msg in self.store[t]:
                fh.write(json.dumps(msg) + "\n")
            fh.close()
        self.store = defaultdict(list)
    
    def unsubscribe(self):
        self.log.info("Unsubscibing @counter:"+ str(self.cnt))
        self.new = randint(0,len(self.topics)-1)
        if self.new != self.old:
            self.ws.send('{"type":"unsubscribe","topic":"%s"}' % 
                         self.topics[self.new])
            self.ws.send('{"type":"subscribe","topic":"%s"}' % 
                         self.topics[self.old])
            self.log.info("\tUnsubscribed:"+ self.topics[self.new])
            self.log.info("\tSubscribed:"+ self.topics[self.old])   
            self.old = self.new
    
    def on_call(self):
        #websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(self.socket, on_error = self.on_error,
                    on_message = self.on_message, on_close = self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()


if __name__ == '__main__':
#    topics = ['cluster.metrics', 'applications']
#    appids = ['applications.application_1523592835217_0001']
#    for appid in appids:
#        topics.append(appid)
#        topics.append(appid+'.events')
#        topics.append(appid+'.logicalOperators')
#        topics.append(appid+'.physicalOperators')
#        topics.append(appid+'.containers')

    parser=argparse.ArgumentParser()    
    parser.add_argument('--topics', '-t', required=True, nargs='*')
    parser.add_argument('--socket', '-s', default='ws://jarvis:9090/pubsub')
    parser.add_argument('--write', '-w', default=10, type=int)
    parser.add_argument('--unsubscribe', '-u', default=15, type=int)
    args=parser.parse_args()
    
    psf = PubSubForever(args.topics, args.socket, args.write, args.unsubscribe)
    psf.on_call()
