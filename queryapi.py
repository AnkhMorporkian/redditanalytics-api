'''
Created on Sep 6, 2013

@author: ankhmorporkian
'''
from sphinxapi import *
import MySQLdb
import zlib
import json
import pprint
import re
import time
import memcache
import hashlib
import inspect
import login_info
c = "search"
limit = 100
query = ""
class Query(object):
    def __init__(self, query="ALL", operation="search", limit=5, sort="date", subreddit="", fromtime=0, totime=1914975401):
        self.memcache = memcache.Client(['127.0.0.1:11211'], debug=0)
        self.user = login_info.user
        self.password = login_info.password
        self.json_output = {'data':[], 'debug':{}}
        self.result = None
        self.query = query
        self.limit = limit
        self.cl = SphinxClient()
        self.sort = str(sort)
        self.subreddit = subreddit
        self.index = None
        self.fromtime = int(fromtime)
        self.totime = int(totime)
        self.arglist = [query, operation, limit, sort, subreddit, fromtime, totime] 
        self.key = hashlib.md5(''.join([str(arg) for arg in self.arglist])).hexdigest()
        self.con = None
        self.cur = None
        self.connectMySQL()
        self.cache_data = None
        self.timeout = 10.0
        self.cl.SetConnectTimeout(self.timeout)
        self.search_time = 0
        self.matches = []
        self.dbquery = ''
        self.sr_ids = []
        
        self.sphinxConnect()
        if operation == 'search':
            self.search()
        if operation == 'searchcomments':
            self.searchComments()
        if operation == 'topsubs':
            self.getTopSubmissions()
            
    def connectMySQL(self):
        self.con = MySQLdb.connect('localhost', self.user, self.password, 'reddit')
        self.cur = self.con.cursor(MySQLdb.cursors.DictCursor)
        
    def sphinxConnect(self):
        self.cl.SetLimits(0, self.limit)
        self.cl.SetServer('localhost', 9312)
        self.cl.SetSortMode(SPH_SORT_EXTENDED, "%s DESC" % self.sort)
        self.cl.SetMatchMode(SPH_MATCH_EXTENDED2)

    def sphinxResult(self):
        if self.query != "": 
            self.result = self.cl.Query(self.query, self.index)
            self.json_output['debug']['total_found'] = self.result['total_found']
            self.json_output['debug']['total_returned'] = self.result['total']
            self.json_output['debug']['search_time'] = float(self.result['time'])
            self.search_time = self.json_output["debug"]["search_time"]
            if not self.result:
                print self.cl.GetLastWarning()
                print self.cl.GetLastError()
            
            # return False
        
        self.json_output['debug']['query'] = self.query
        self.json_output['debug']['sort'] = self.sort
        if self.subreddit == '':
            self.json_output['debug']['subreddit'] = "all"
        else:
            self.json_output['debug']['subreddit'] = self.subreddit
        self.json_output['debug']['limit'] = self.limit
        self.json_output['debug']['cached'] = False
        return self.result
    
    def cacheCheck(self):
        return False
        data = self.memcache.get(self.key)
        if data is not None:
            data["debug"]["cached"] = True
            self.json_output = data
            return True
        return False
   
    def cache(self):
        time = int(3600 * (self.search_time / 5))
        self.json_output["debug"]["cache_time"] = 300 if time < 300 else time
        self.memcache.set(self.key, self.json_output, time)
        
    def output(self):
        return json.dumps(self.json_output)
    
    def getSubreddits(self, id, table):
        subreddits = []
        if self.query != '':
            if self.subreddit != '':
                self.dbquery = "SELECT id FROM _subreddits WHERE subreddit IN (%s)" % ','.join(["'%s'" % x for x in self.subreddit.split(',')])
                self.cur.execute(self.dbquery)
                subreddits = [int(x[0]) for x in self.cur.fetchall()]
                self.cl.SetFilter("subreddit_id", self.sr_ids)
            self.sphinxResult()
            self.matches = self.result['matches']
        else:
            limit = " LIMIT %s" % self.limit
            
            addon = " WHERE subreddit_id IN (%s)" % ','.join(["'%s'" % x for x in subreddits]) if len(subreddits) > 0 else '' 
            addon = addon + limit
            query = "SELECT %s FROM %s%s" % (id, table, addon)
            results = self.cur.execute(query)
            rows = self.cur.fetchallDict()
            self.matches.extend(rows)

    def search(self):
        self.index = 'main'
        if not self.cacheCheck():
            if self.query == "ALL":
                self.query = ""
            self.setLimit(100)
            self.getSubreddits('id', 'data')

            ids = ','.join([str(int(x['id'])) for x in self.matches])
            query = "SELECT id,json FROM _raw WHERE id IN (%s)" % (ids)
            print ids
            results = self.cur.execute(query)
            rows = self.cur.fetchallDict()
            for row in rows:
                self.json_output['data'].append(json.loads(zlib.decompress(row['json'])))
        self.cache()
        return self.output()
                    
    def searchComments(self):
        self.index = 'comments'
        
        if not self.cacheCheck():
            self.setLimit(500)
            self.getSubreddits('comment_id', 'comments_index')
            ids = ','.join([str(int(x['comment_id'])) for x in self.matches])
            query = "SELECT json FROM comments WHERE comment_id IN (%s)" % (ids)
            results = self.cur.execute(query)
            rows = self.cur.fetchallDict()
            for row in rows:
                self.json_output['data'].append(json.loads(zlib.decompress(row["json"])))
        self.cache()
        return self.output()
        
        
    def setLimit(self, limit):
        if self.limit > limit:
            self.limit = limit
        else:
            self.limit = self.limit

    def getTopSubmissions(self):
        self.index = "main"
        self.limit = 25
        self.setLimit(25)
        if self.query == "":
            return ""
        self.cl.SetSortMode(SPH_SORT_EXTENDED, "score DESC")
        self.cl.SetFilterRange('date', self.fromtime, self.totime)
        self.getSubreddits('id', 'data')
        for res in self.matches[:25]:
            print "Got here"
            query = "SELECT id,json FROM _raw WHERE id=%s" % (res['id'])
            results = self.cur.execute(query)
            row = self.cur.fetchone()
            self.json_output['data'].append(json.loads(zlib.decompress(row[1])))
        self.cache()
        self.json_output = self.json_output
        return json.dumps(self.json_output)
