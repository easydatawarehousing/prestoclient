#!/usr/bin/env python

""" PrestoClient provides a method to communicate with a Presto server. Presto is a fast query
    engine developed by Facebook that runs distributed queries against Hadoop HDFS servers.

    Copyright 2013 Ivo Herweijer | easydatawarehousing.com

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at:

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

import httplib
import urllib2
import json
import getpass
from time import sleep


class PrestoClient:
    """
    PrestoClient
    ============

    PrestoClient implements a Python class to communicate with a Presto server.
    Presto (U{http://prestodb.io/}) is a fast query engine developed
    by Facebook that runs distributed queries against a (cluster of)
    Hadoop HDFS servers (U{http://hadoop.apache.org/}).
    Presto uses SQL as its query language. Presto is an alternative for
    Hadoop-Hive.

    PrestoClient was developed using Presto 0.52 and tested on Presto 0.52 and 0.54.

    You can use this class with this sample code:

    >>> import prestoclient
    >>>
    >>> sql = "SHOW TABLES"
    >>>
    >>> # Replace localhost with ip address or dns name of the Presto server running the discovery service
    >>> presto = prestoclient.PrestoClient("localhost")
    >>>
    >>> if not presto.startquery(sql):
    >>>     print presto.getlasterrormessage()
    >>> else:
    >>>     presto.waituntilfinished(True) # Remove True parameter to skip printing status messages
    >>>     print "Columns: ", presto.getcolumns()
    >>>     print "Datalength: ", presto.getdatalength(), " Data: ", presto.getdata()

    Presto client protocol
    ======================

    The communication protocol used between Presto clients and servers is not documented. It seems to
    be as follows:

    Client sends http POST request. Headerinformation should include: X-Presto-Catalog, X-Presto-Source,
    X-Presto-Schema, User-Agent, X-Presto-User. The body of the request should contain the sql statement.
    The server responds by returning JSON data. This data should contain 2 uri's. One giving the link
    to get more information about the query execution (infoUri) and the other one giving the link to fetch
    the next packet of data (nextUri).

    The client should send GET requests to the server (header: X-Presto-Source, User-Agent, X-Presto-User.
    Body: empty) following the nextUri link from the last response
    until the server response does not give any more nextUri links. The server response also contains a
    'state' variable. When there is no nextUri the state should be one of: FINISHED, FAILED or CANCELED.
    Each response by the server to a 'nextUri' may contain information about the columns returned by the
    query and all- or part of the querydata.

    The server reponse may contain a variable with the uri to cancel the query (partialCancelUri). The
    client may issue a DELETE request to the server using this link.
    The Presto server will retain information about finished queries for 15 minutes. When a client does
    not respond to the server (by following the nextUri links) the server will cancel these 'dead' queries
    after 5 minutes. These timeouts are hardcoded in the Presto server source code.

    ToDo
    ====

        - Make the PrestoClient class re-usable. Currently you can only start one query per instance of
        this class.

        - Add support for insert/update queries (if and when Presto server supports this).

    Availability
    ============

    Source code is available through: U{https://github.com/easydatawarehousing/prestoclient}

    Additional information may be found here: U{http://www.easydatawarehousing.com/tag/presto/}

    """

    __source = "RPresto"                        #: Client name sent to Presto server
    __version = "0.1.0"                         #: PrestoClient version string
    __useragent = __source + "/" + __version    #: Useragent name sent to Presto server
    __urltimeout = 5000                         #: Timeout in millisec to wait for Presto server to respond
    __updatewaittimemsec = 1500                 #: Wait time in millisec to wait between requests to Presto server
    __server = ""                               #: IP address or DNS name of Presto server
    __port = 0                                  #: TCP port of Presto server
    __catalog = ""                              #: Catalog name to be used by Presto server
    __user = ""                                 #: Username to pass to Presto server
    __lasterror = ""                            #: Html error of last request
    __lastinfouri = ""                          #: Uri to query information on the Presto server
    __lastnexturi = ""                          #: Uri to next dataframe on the Presto server
    __lastcanceluri = ""                        #: Uri to cancel query on the Presto server
    __laststate = ""                            #: State returned by last request to Presto server
    __cancelquery = False                       #: Boolean, when set to True signals that query should be cancelled
    __isstarted = False                         #: Boolean, when set to True no new queries can be started
    __response = {}                             #: Buffer for last response of Presto server
    __columns = {}                              #: Buffer for the column information returned by the query
    __data = []                                 #: Buffer for the data returned by the query

    def __init__(self, in_server, in_port=8080, in_catalog="hive", in_user=""):
        """ Constructor of PrestoClient class.

        Arguments:

        in_server  -- IP Address or dns name of the Presto server running the discovery service

        in_port    -- TCP port of the Prestoserver running the discovery service (default 8080)

        in_catalog -- Catalog name that the Prestoserver should use to query hdfs (default 'hive')

        in_user    -- Username to pass to the Prestoserver. If left blank the username from the OS is used
        (default '')

        """
        self.__server = in_server
        self.__port = in_port
        self.__catalog = in_catalog

        if in_user == "":
            self.__user = getpass.getuser()
        else:
            self.__user = in_user

        return

    def getversion(self):
        """ Return PrestoClient version number. """
        return self.__version

    def getresponse(self):
        """ Return response of last executed request to the prestoserver. """
        return self.__response

    def getlasterrormessage(self):
        """ Return error message of last executed request to the prestoserver or empty string if there is no error. """
        return self.__lasterror

    def getcolumns(self):
        """ Return the column information of the queryresults. Nested list of datatype / fieldname. """
        return self.__columns

    def getdatalength(self):
        """ Return the length of the currently buffered data in number of rows. """
        return len(self.__data)

    def getdata(self):
        """ Return the currently buffered data. """
        return self.__data

    def cleardata(self):
        """ Empty the data buffer. Use this function to implement your own 'streaming' data retrieval setup. """
        self.__data = {}
        return

    def startquery(self, in_sql_statement, in_schema="default"):
        """ Start a query. Currently, only one query per instance of the PrestoClient class is allowed.

        Arguments:

        in_sql_statement -- The query that should be executed by the Presto server

        in_schema        -- The HDFS schema that should be used (default 'default')

        """
        headers = {"X-Presto-Catalog": self.__catalog,
                   "X-Presto-Source":  self.__source,
                   "X-Presto-Schema":  in_schema,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        self.__lasterror = ""

        sql = in_sql_statement
        sql = sql.strip()
        sql = sql.rstrip(";")

        if sql == "":
            self.__lasterror = "No query statement"
            return False

        if self.__isstarted:
            self.__lasterror = "Query already started. Please create a new instance of PrestoClient class"
            return False

        self.__isstarted = True

        try:
            conn = httplib.HTTPConnection(self.__server, self.__port, False, self.__urltimeout)
            conn.request("POST", "/v1/statement", sql, headers)
            response = conn.getresponse()

            if response.status != 200:
                self.__lasterror = "Connection error: " + str(response.status) + " " + response.reason
                conn.close()
                return False

            answer = response.read()

            conn.close()

            self.__response = json.loads(answer)
            self.__getvarsfromresponse()

        except httplib.HTTPException:
            self.__lasterror = "Connection error: " + str(httplib.HTTPException.message)
            return False

        else:
            pass

        return True

    def waituntilfinished(self, in_verbose=False):
        """ Returns when query has finished.

        Arguments:

        in_verbose -- If True print some simple progress messages (default False)

        """
        tries = 0

        while self.queryisrunning():
            if in_verbose:
                tries += 1
                print "Ping: ", tries, " Rows=", len(self.__data)

            sleep(self.__updatewaittimemsec/1000)

        return

    def queryisrunning(self):
        """ Returns True if query is running. Use this function to implement your own data retrieval setup. """
        if not self.__getnext():
            return False

        if self.__cancelquery:
            self.__cancel()
            return False

        if self.__laststate == "FINISHED" or self.__laststate == "FAILED" or self.__laststate == "CANCELED":
            # Pick up any remaining data and messages
            while self.__getnext():
                pass

            return False

        return True

    def getqueryinfo(self):
        """ Requests query information from the Presto server and returns this as a dictonary. The Presto
        server removes this information 15 minutes after finishing the query.

        """
        if self.__lastinfouri == "":
            return {}

        if not self.__openuri(self.__lastinfouri):
            return {}

        return self.__response

    def cancelquery(self):
        """ Inform Prestoclient to cancel the running query. When queryisrunning() is called
        prestoclient will send a cancel query request to the Presto server.

        """
        self.__cancelquery = True
        return

    def __openuri(self, in_uri):
        """ Internal function, sends a GET request to the Presto server """
        headers = {"X-Presto-Source":  self.__source,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        self.__lasterror = ""

        try:
            conn = urllib2.urlopen(in_uri, None, self.__urltimeout)
            answer = conn.read()
            conn.close()
            self.__response = json.loads(answer)

        except urllib2.HTTPError as e:
            self.__lasterror = "HTTP error: " + e.reason
            return False

        except urllib2.URLError as e:
            self.__lasterror = "URL error: " + e.reason
            return False

        else:
            pass

        return True

    def __getvarsfromresponse(self):
        """ Internal function, retrieves some information from the response of the Presto server. """
        if "infoUri" in self.__response:
            self.__lastinfouri = self.__response["infoUri"]

        if "nextUri" in self.__response:
            self.__lastnexturi = self.__response["nextUri"]
        else:
            self.__lastnexturi = ""

        if "partialCancelUri" in self.__response:
            self.__lastcanceluri = self.__response["partialCancelUri"]

        if "state" in self.__response:
            self.__laststate = self.__response["state"]
        elif "stats" in self.__response:
            if "state" in self.__response["stats"]:
                self.__laststate = self.__response["stats"]["state"]

        if not self.__columns:
            if "columns" in self.__response:
                self.__columns = self.__response["columns"]

        if "data" in self.__response:
            if self.__data:
                self.__data.extend(self.__response["data"])
            else:
                self.__data = self.__response["data"]

        return

    def __getnext(self):
        """ Internal function, starts a new request to the Presto server using the 'nextUri' link. """
        if self.__lastnexturi == "":
            return False

        if not self.__openuri(self.__lastnexturi):
            return False

        self.__getvarsfromresponse()

        return True

    def __cancel(self):
        """ Internal function, sends a cancel request to the Prestoserver. """
        if self.__lastcanceluri == "":
            return False

        headers = {"X-Presto-Source":  self.__source,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        self.__lasterror = ""

        try:
            conn = httplib.HTTPConnection(self.__server, self.__port, False, self.__urltimeout)
            conn.request("DELETE", self.__lastcanceluri, None, headers)
            response = conn.getresponse()

            if response.status != 204:
                self.__lasterror = "Connection error: " + str(response.status) + " " + response.reason
                conn.close()
                return False

            conn.close()

        except httplib.HTTPException:
            self.__lasterror = "Connection error: " + str(httplib.HTTPException.message)
            return False

        else:
            pass

        return True
