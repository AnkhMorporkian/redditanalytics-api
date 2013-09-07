'''
Created on Sep 6, 2013

@author: ankhmorporkian
'''
from sphinxapi import *
import MySQLdb
import decimal
import hashlib
import inspect
import json
import login_info
import memcache
import pprint
import re
import time
import zlib
c = "search"
limit = 100
query = ""
class Query(object):
    def __init__(self, query="", operation="search", limit=5, sort="date", subreddit="", fromtime=0, totime=1914975401):
        """
        Initialization function for Query. Takes in appropriate data for a query,
        creates a MySQL DB connection, and chooses which query to run.
        
        :param query: The search string to check against. Not used in all operation types.
        :type query: str
        :param operation: The operation to perform.
        :type operation: str
        :param limit: Limit number of results to this number. Overridden on method calls if it is
         > than the predefined maximum for that operation type.
        :type limit: int
        :param sort: Sort method, mysql.
        :type sort: str
        :param subreddit: Subreddit. Should either be a single word or seperated by commas.
        :type subreddit: string
        :param fromtime: Date filter, beginning unixtime. Not used by all methods.
        :type fromtime: int
        :param totime: Date filter, end unixtime. Not used by all methods.
        :type totime: int 
        """
        
        self.json_output = {'data':[], 'debug':{}}  # JSON output shared by all functions.
        self.start_time = time.clock()  # Timer
        self.search_time = 0
        self.matches = []
        self.dbquery = ''
        self.sr_ids = []
        self.result = None
        self.index = None
        
        """Pull MySQL login information from login_info.py."""
        self.user = login_info.user
        self.password = login_info.password
        
        """Set variables from passed arguments, enforcing type."""
        self.query = str(query)
        self.limit = int(limit)
        self.sort = str(sort)
        self.subreddit = str(subreddit)
        self.fromtime = int(fromtime)
        self.totime = int(totime)
        self.operation = str(operation)
        
        """Perform various other initialization tasks."""
        self.connectMySQL()
        self.memcacheInit()
        self.sphinxInit()
        
        """Select which function to run based on the operation. Defaults to search"""
        if operation == 'search':
            self.search()
        elif operation == 'searchcomments':
            self.searchComments()
        elif operation == 'topsubs':
            self.getTopSubmissions()
        elif operation == 'activethreads':
            self.getMostActiveThreads()
        elif operation == 'subreddits':
            self.getSubredditsLike()
        elif operation == 'subreddit_by_minute':
            self.subredditMinutely()
        else:
            self.search()

    def __str__(self):
        return json.dumps(self.output)
    
    def _cache(f):
        def replacement(self):
            if not self.cacheCheck():
                f(self)
                self.cache()
            return self.output()
        return replacement
    def _secondcache(f):
        def replacement(self):
            if not self.cacheCheck():
                f(self)
                self.cache(1)
            return self.output()
        return replacement 
        
    def sphinxInit(self):
        """
        Connects to Sphinx and sets various modes.
        """
        timeout = 10.0
        self.cl = SphinxClient()
        self.cl.SetConnectTimeout(timeout)
        self.cl.SetLimits(0, self.limit)
        self.cl.SetServer('localhost', 9312)
        self.cl.SetSortMode(SPH_SORT_EXTENDED, "%s DESC" % self.sort)
        self.cl.SetMatchMode(SPH_MATCH_EXTENDED2)

    def memcacheInit(self):
        """
        Connects to memcache and generates a key based on the argument list.
        """
        self.memcache = memcache.Client(['127.0.0.1:11211'], debug=0)
        self.arglist = [self.query, self.operation, self.limit, self.sort, self.subreddit, self.fromtime, self.totime] 
        self.key = hashlib.md5(''.join([str(arg) for arg in self.arglist])).hexdigest()
        self.cache_data = None
        
    def connectMySQL(self):
        """
        Connects to the MySQL database. 
        """
        # conv[246] = int  # Hack to convert decimals to ints.
        self.con = MySQLdb.connect('localhost', self.user, self.password, 'reddit', charset='utf8')
        self.cur = self.con.cursor(MySQLdb.cursors.DictCursor)
        
    def sphinxResult(self):
        """
        Generates a sphinx result based on the query.
        """
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
        """
        Check if an item is in the cache or not. If it is, update the JSON debug 
        to reflect that fact.
        """
        data = self.memcache.get(self.key)
        if data is not None:
            data["debug"]["cached"] = True
            self.json_output = data
            return True
        return False
   
    def cache(self, time=None):
        """
        Caches a result. Time is based on the amount of time the search took.
        """
        if not time:
            time = int(3600 * (self.search_time / 5))
            time = 300 if time < 300 else time
        self.json_output["debug"]["cache_time"] = time
        self.memcache.set(self.key, self.json_output, time)
        
    def decimal_default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj)
        raise TypeError
        
    def output(self):
        """
        Returns a string dump of a JSON object.
        """
        self.json_output['debug']['debug_time'] = time.clock() - self.start_time
        return json.dumps(self.json_output)
    

    def getSubredditIDs(self):
        self.dbquery = "SELECT id FROM _subreddits WHERE subreddit IN (%s)" % ','.join(["'%s'" % x for x in self.subreddit.split(',')])
        self.cur.execute(self.dbquery)
        subreddits = [int(x['id']) for x in self.cur.fetchall()]
        return subreddits

    def getSubreddits(self, id, table):
        """
        :param id: The ID column to match against.
        :type id: str
        :param table: The table to match to.
        :type table: str
        """
        
        # TODO: This is all spaghetti code. Rewrite it.         
        subreddits = []
        if self.query != '':
            if self.subreddit != '':
                subreddits = self.getSubredditIDs()
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


    def ungzip(self, row):
        return self.json_output['data'].append(json.loads(zlib.decompress(row['json'])))
    
    @_cache
    def search(self):
        """
        Search operation. Returns all submissions that match the query.
        """
        self.index = 'main'
        sqlquery = "SELECT id,json FROM submissions WHERE id IN (%s)"

        if self.query == "ALL":
            self.query = ""
        self.setLimit(100)
        self.getSubreddits('id', 'submissions_index')

        ids = ','.join([str(int(x['id'])) for x in self.matches])
        rows = self.sqlQuery(sqlquery, ids)
        for row in rows:
            self.ungzip(row)


    def sqlQuery(self, query, *args):
           query = query % args
           results = self.cur.execute(query)
           rows = self.cur.fetchallDict()
           return rows
    
    @_cache
    def searchComments(self):
        """
        Search comments operation. Returns all comments that match the query.
        """
        self.index = 'comments'
        self.setLimit(500)
        self.getSubreddits('comment_id', 'comments_index')
        ids = ','.join([str(int(x['comment_id'])) for x in self.matches])
        query = "SELECT json FROM comments WHERE comment_id IN (%s)" % (ids)
        results = self.cur.execute(query)
        rows = self.cur.fetchallDict()
        for row in rows:
            self.ungzip(row)
        self.cache()
        return self.output()
    
    def setLimit(self, limit):
        """
        Sets a limit based.
        :param limit: Limit
        :type limit: integer
        """
        if self.limit > limit:
            self.limit = limit
        else:
            self.limit = self.limit

    def getMostActiveThreads(self):
        """
        Returns the most active threads.
        """
        self.dbquery = """
        SELECT count(*) as count, subreddit, title,LOWER(CONV(link_id,10,36)) as link_id
        FROM comments_index, _subreddits, submission_titles
        WHERE date > UNIX_TIMESTAMP(now()) - %s AND
        _titles.id = link_id AND
        _subreddits.id = subreddit_id
        GROUP BY comments_index.link_id
        ORDER BY count(*) DESC
        LIMIT %s
        """ % (300, 25)
        self.cur.execute(self.dbquery)
        self.json_output['data'] += self.cur.fetchallDict()
        return self.output()

    def getTopSubmissions(self):
        """
        Returns the top submission.
        
        TODO: A little broken. Recode.
        """
        self.index = "main"
        self.limit = 25
        self.setLimit(25)
        if self.query == "":
            return ""
        self.cl.SetSortMode(SPH_SORT_EXTENDED, "score DESC")
        self.cl.SetFilterRange('date', self.fromtime, self.totime)
        self.getSubreddits('id', 'submissions_index')
        for res in self.matches[:25]:
            print "Got here"
            query = "SELECT id,json FROM submissions WHERE id=%s" % (res['id'])
            results = self.cur.execute(query)
            row = self.cur.fetchone()
            self.json_output['data'].append(json.loads(zlib.decompress(row[1])))
        self.cache()
        return self.output()

    def getSubredditsLike(self):
        self.setLimit(1000)
        query = "SELECT subreddit,id FROM _subreddits WHERE subreddit LIKE '%s%%' LIMIT %s" % (self.query, self.limit)
        print query
        results = self.cur.execute(query)
        rows = self.cur.fetchallDict()
        self.json_output['data'].extend(rows)
        return self.output
    
    @_secondcache
    def subredditMinutely(self):
        subreddits = self.getSubredditIDs()
        query = """
                SELECT date, cast(sum(comment_count) as int) AS count
                FROM  `_subreddits_minute`
                %s 
                GROUP BY date
                ORDER BY date DESC
                LIMIT %s 
                """
        wherestring = ""
        if len(subreddits) > 0:
            wherestring = "WHERE subreddit_id IN (%s)" % ','.join([str(x) for x in subreddits])   
        self.json_output['data'].extend(self.sqlQuery(query, wherestring, self.limit))
        
