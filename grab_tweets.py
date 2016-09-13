'''
Created on Sep 13, 2016

@author: hlamba
'''
import os
import sys
import json
from prepare_api import load_clients
from prepare_api import shift_clients
import time
import datetime
import sqlite3
import urllib2
import oauth2 as oauth
import threading
import httplib2


global log_file


def load_api_clients(base_out_dir):
    f=open('api_accounts.json','r')
    json_arr=json.load(f)
    arr_clients=load_clients(json_arr)
    f.close()
    
    arr_outfiles=[]
    for i in range(0,len(arr_clients)):
        arr_outfiles.append(os.path.join(base_out_dir,"Output_"+str(i)+".json"))
        
    print "Read Clients:"+str(len(arr_clients))
    print "Read Outfiles:"+str(len(arr_outfiles))
        
    return arr_clients,arr_outfiles

class threaded_sample_api(threading.Thread):
    
    def __init__(self,name,api_client,out_file):
        threading.Thread.__init__(self)
        self.name=name
        self.api_client=api_client
        self.out_file=out_file
        self.url='https://stream.twitter.com/1.1/statuses/sample.json'
        
    def fetch(self,url,curr_client):
        uri,body=oauth.Client.get_uri_of_request(curr_client.client, url)
        req = urllib2.Request(uri)
        f = urllib2.urlopen(req)
        return f
    
    def run(self):
        f=self.fetch(self.url,self.api_client)
        fw=open(self.out_file,'a')
        
        while True:
            multiline = False
            line = ''
            while True:
                if multiline:
                    line += '\n' + f.readline()
                else:
                    line=f.readline()
                multiline = not line.endswith('\r\n')
            
                if multiline:
                    continue
            
                if line:
                    status=json.loads(line)
                    json.dump(status,fw)
                    fw.write('\n')
                    fw.flush()
        fw.close()
        
def grab_statuses(base_out_dir):
    arr_clients,arr_outfiles = load_api_clients(base_out_dir)
    
    threads=[]
    for i in range(0,len(arr_clients)):
        print "Starting Thread:"+str(i)
        name='Thread:'+str(i)
        api_client = arr_clients[i]
        out_file = arr_outfiles[i]
        thread = threaded_sample_api(name,api_client,out_file)
        thread.start()
        threads.append(thread)
        print 'Finishing Thread:'+str(i)
        
    print 'Starting to Join Thread Now'
    for thread in threads:
        thread.join()


base_out_dir='/Users/hlamba/Documents/Experiments/Twitter_SampleAPITweets'
grab_statuses(base_out_dir)





