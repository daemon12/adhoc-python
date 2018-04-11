#!/usr/bin/python
import websocket
import json
from collections import defaultdict
from pprint import pprint

class PubSubWebSocket:
    def __init__(self, topic = 'cluster.metrics', num_frames = 1, 
                 socket = 'ws://node36.morado.com:19090/pubsub'):
        self.cnt = 0
        self.ws = None
        self.topic = topic
        self.socket = socket
        self.num_frames = num_frames
        self.retval=defaultdict(list)
        print "Topic=", self.topic, ", numFrames=", self.num_frames  

    def on_message(self, ws, message):
        x = json.loads(message)
        if (x['topic'] == self.topic):
            self.retval[self.cnt] = x['data']
            self.cnt+=1
        if self.cnt == self.num_frames:
            self.on_close(self.ws)            
            
    def on_error(self, ws, error):
        print "\nON_ERROR: ", error
    
    def on_close(self, ws):
        self.ws.close()
    
    def on_open(self, ws):
        self.ws.send('{"type":"subscribe","topic":"%s"}' % self.topic)
    
    def on_call(self):
        #websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(self.socket, on_error = self.on_error,
                    on_message = self.on_message, on_close = self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()
        return self.retval


print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
psw = PubSubWebSocket('applications', 1)
RETVAL = psw.on_call()
pprint(RETVAL)
print RETVAL[0]['apps'][0]['appPackageSource']['configPackage']['name']
