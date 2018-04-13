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
import argparse
import json
import websocket
from random import randint
from collections import defaultdict

class PubSubForever:
    def __init__(self, topics, socket, wrt = 10, uns = 100):
        self.cnt = self.old = self.new = 0
        self.ws = None
        self.wrt = wrt
        self.uns = uns
        self.topics = topics
        self.socket = socket        
        self.store = defaultdict(list)

    def on_message(self, ws, message):
        self.cnt += 1
        data = json.loads(message)
        topic = data['topic']
        print self.cnt,topic
        self.store[topic].append(data)
        if self.cnt % self.wrt == 0:
            self.write_to_file()
        if self.cnt % self.uns == 0:
            self.unsubscribe()
            
    def on_error(self, ws, error):
        print "\nON_ERROR: ", error
    
    def on_close(self, ws):
        self.ws.close()
    
    def on_open(self, ws):
        print "Subscribing topics:"
        for t in self.topics:
            self.ws.send('{"type":"subscribe","topic":"%s"}' % t)
            print "\tSubscribed: " + t
        print "All topics subscribed!"
        for t in self.topics:
            self.ws.send('{"type":"subscribe","topic":"%s"}' % t)
            print "\tSubscribed: " + t
        print "All topics subscribed!"
                
    def write_to_file(self):
        for t in self.topics:
            fh = open(t+".txt", "a+")
            for msg in self.store[t]:
                fh.write(json.dumps(msg) + "\n")
            fh.close()
        self.store = defaultdict(list)
    
    def unsubscribe(self):
        print "Unsubscibing @counter:", self.cnt
        self.new = randint(0,len(self.topics)-1)
        if self.new != self.old:
            self.ws.send('{"type":"unsubscribe","topic":"%s"}' % 
                         self.topics[self.new])
            self.ws.send('{"type":"subscribe","topic":"%s"}' % 
                         self.topics[self.old])
            print "\tUnsubscribed:", self.topics[self.new]
            print "\tSubscribed:", self.topics[self.old]            
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
    parser.add_argument('--flush', '-f', default=100)
    parser.add_argument('--unsubscribe', '-u', default=100)
    args=parser.parse_args()
    print("INPUTS: \n\tTopics: %s\n\tSocket: %s, Flush: %s, Unsubsscribe: %s"
          % (str(args.topics), args.socket, args.flush, args.unsubscribe))
    
    psf = PubSubForever(args.topics, args.socket, args.flush, args.unsubscribe)
    psf.on_call()
    print "Done!!!!"
