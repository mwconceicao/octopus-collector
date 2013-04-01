#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Octopus : Collector
'''

import datetime
import psycopg2
import re
import sys
import time

'''
###################
## Configuration ##
###################
'''
# Squid access log file
SQUID_ACCESS_LOG = "/usr/local/squid/logs/access.log"

# Database options
DB_HOST     = "localhost"
DB_USERNAME = "octopus"
DB_PASSWORD = "octopus"
DB_DATABASE = "octopus"
'''
###################
'''

class SquidLogFile:
    """Squid Log file class"""
    path = ""
    
    def __init__(self, logFilePath):
        self.path = logFilePath
    
    def clean(self):
        fileHandler = open(self.path, 'w')
        fileHandler.close()
    
    def parse(self):
        print("Opening access log file...")

        fileHandler = open(self.path, 'r')
        lines = fileHandler.readlines()
        fileHandler.close()
        
        self.clean()
        
        print("Found " + str(len(lines)) + " Squid access requests.")
        print("Parsing log file...")
        
        accessLogRequests = []
        
        regex = re.compile("\s+")
        for line in lines:
            line = regex.sub(" ", line).strip().split(" ")
            
            request = SquidAccessLogRequest()
            request.timestamp      = int(line[0].split(".")[0])
            request.response_time  = line[1]
            request.client_address = line[2]
            request.result         = line[3].split('/')[0]
            request.status_code    = line[3].split('/')[1]
            request.size           = line[4]
            request.request_method = line[5]
            request.uri            = line[6]
            request.user           = line[7]
            request.peering_code   = line[8].split('/')[0]
            request.peering_host   = line[8].split('/')[1]
            request.content_type   = line[9]
            accessLogRequests.append(request)
            #sys.stdout.write(".")
            #time.sleep(1)  
        #print
        
        return accessLogRequests

class SquidAccessLogRequest:
    timestamp      = 0
    response_time  = ""
    client_address = ""
    result         = ""
    status_code    = ""
    size           = ""
    request_method = ""
    uri            = ""
    user           = ""
    peering_code   = ""
    peering_host   = ""
    content_type   = ""



print(":: Starting @ " + str(datetime.datetime.now()) + " ::")
print("Connecting do database...")

dbConnection = psycopg2.connect("host="+DB_HOST+" user="+DB_USERNAME+" password="+DB_PASSWORD+" dbname="+DB_DATABASE)
dbCursor = dbConnection.cursor()

logFile = SquidLogFile(SQUID_ACCESS_LOG)
requests = logFile.parse()

print("Saving to database...")

for request in requests:
    try:
        dbCursor.execute(  # execute / mogrify 
         """INSERT INTO
                squid_access_log_request
                  ("timestamp",
                   "response_time",
                   "client_address",
                   "result",
                   "status_code",
                   "size",
                   "request_method",
                   "uri",
                   "user",
                   "peering_code",
                   "peering_host",
                   "content_type")
            VALUES
                ( %s::abstime::timestamp,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );""",              
            
            (     request.timestamp,
                  request.response_time,
                  request.client_address,
                  request.result,
                  request.status_code,
                  request.size,
                  request.request_method,
                  request.uri,
                  request.user,
                  request.peering_code,
                  request.peering_host,
                  request.content_type )
        )
    except Exception as e:
        print("Database error: " + str(e))
    
    #sys.stdout.write("+")
    dbConnection.commit()
    #time.sleep(1)
    
print

#print("Committing data to database...")
dbConnection.commit()

print("Closing database connection...")
dbCursor.close()
dbConnection.close()

print(":: Finished @ " + str(datetime.datetime.now()) + " ::")

