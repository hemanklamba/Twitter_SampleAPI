'''
Created on Sep 13, 2016

@author: hlamba
'''
import oauth2 as oauth
import httplib2
import json
import socket
import os
import sys

class API_Client(object):
    
    def __init__(self,client_id,cons_key,cons_secret,acc_key,acc_secret):
        self.client_id=client_id
        self.cons_key=cons_key
        self.cons_secret=cons_secret
        self.acc_key=acc_key
        self.acc_secret=acc_secret
        
        consumer=oauth.Consumer(cons_key,cons_secret)
        token=oauth.Token(acc_key,acc_secret)
        client=oauth.Client(consumer,token)

        self.client=client
        
    def get_request(self,url,http_method="GET",post_body="",http_headers=None):
        
        resp,content=self.client.request(
                url,
                method=http_method,
                body=post_body,
                headers=http_headers,
                #force_auth_header=True
        )
        
        return content,resp.status
    
    def get_remaining_hits(self,resource,resource_key):
        remaining_hits=0
        try:
            resp=400
            while(resp!=200):
                url='https://api.twitter.com/1.1/application/rate_limit_status.json?resources='+str(resource)
                ratereq,resp=self.get_request(url)
                rls=json.loads(ratereq)
            remaining_hits=rls['resources'][resource][resource_key]['remaining'];
            print ('remaining hits'+str(remaining_hits))
        except Exception,e:
            print "Exception while doing this."
            
        return remaining_hits

def load_clients(json_arr):
    client_arr=[]
    for i in range(0,len(json_arr)):
        client_id=i
        element = json_arr[i]
        cons_key=element['consumer_key']
        cons_sec=element['consumer_secret']
        acc_key=element['access_key']
        acc_secret=element['access_secret']
        
        client=API_Client(client_id,cons_key,cons_sec,acc_key,acc_secret)
        client_arr.append(client)
        print "Client:"+str(client_id)+" Loaded."
    return client_arr

def shift_clients(curr_i,arr):
    curr_i=(curr_i+1) % len(arr)
    return curr_i,arr[curr_i]



