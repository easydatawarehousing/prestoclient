PrestoClient
============

PrestoClient implements a Python class to communicate with a Presto server.
Presto (http://prestodb.io/) is a fast query engine developed
by Facebook that runs distributed queries against a (cluster of)
Hadoop HDFS servers (http://hadoop.apache.org/).
Presto uses SQL as its query language. Presto is an alternative for
Hadoop-Hive.

PrestoClient was developed using Presto 0.52 and tested on Presto 0.52 and 0.54. Python version used is 2.7.6

You can use this class with this sample code:

	import prestoclient
	
	sql = "SHOW TABLES"
	
	# Replace localhost with ip address or dns name of the Presto server running the discovery service
	presto = prestoclient.PrestoClient("localhost")
	
	if not presto.startquery(sql):
		print "Error: ", presto.getlasterrormessage()
	else:
		presto.waituntilfinished(True) # Remove True parameter to skip printing status messages
		
		# We're done now, so let's show the results
		print "Columns: ", presto.getcolumns()
		if presto.getstatus() == "FAILED": print "Error : ", presto.getlasterrormessage()
		if presto.getdata(): print "Datalength: ", presto.getnumberofdatarows(), " Data: ", presto.getdata()


Presto client protocol
----------------------
The communication protocol used between Presto clients and servers is not documented yet. It seems to
be as follows:

Client sends http POST request to the Presto server, page: "/v1/statement". Header information should
include: X-Presto-Catalog, X-Presto-Source, X-Presto-Schema, User-Agent, X-Presto-User. The body of the
request should contain the sql statement. The server responds by returning JSON data (http status-code 200).
This reply may contain up to 3 uri's. One giving the link to get more information about the query execution
('infoUri'), another giving the link to fetch the next packet of data ('nextUri') and one with the uri to
cancel the query ('partialCancelUri').

The client should send GET requests to the server (Header: X-Presto-Source, User-Agent, X-Presto-User.
Body: empty) following the 'nextUri' link from the previous response until the servers response does not
contain an 'nextUri' link anymore. When there is no 'nextUri' the query is finished. If the last response
from the server included an error section ('error') the query failed, otherwise the query succeeded. If
the http status of the server response is anything other than 200 with Content-Type application/json, the
query should also be considered failed. A 503 http response means that the server is (too) busy. Retry the
request after waiting at least 50ms.
The server response may contain a 'state' variable. This is for informational purposes only (may be subject
to change in future implementations).
Each response by the server to a 'nextUri' may contain information about the columns returned by the query
and all- or part of the querydata. If the response contains a data section the columns section will always
be available.

The server reponse may contain a variable with the uri to cancel the query ('partialCancelUri'). The client
may issue a DELETE request to the server using this link. Response http status-code is 204.

The Presto server will retain information about finished queries for 15 minutes. When a client does not
respond to the server (by following the 'nextUri' links) the server will cancel these 'dead' queries after
5 minutes. These timeouts are hardcoded in the Presto server source code.

ToDo
----
- Enable PrestoClient to handle multiple running queries simultaneously. Currently you can only run one query per instance of this class.
- Add support for https connections
- Add support for insert/update queries (if and when Presto server supports this).

Availability
------------
Source code is available through: https://github.com/easydatawarehousing/prestoclient

Additional information may be found here: http://www.easydatawarehousing.com/tag/presto/

Copyright
---------
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
