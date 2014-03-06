cPrestoClient
=============

cPrestoClient implements the client protocol to communicate with a Presto server.

Presto (http://prestodb.io/) is a fast query engine developed
by Facebook that runs distributed queries against a (cluster of)
Hadoop HDFS servers (http://hadoop.apache.org/).  
Presto uses SQL as its query language and is an alternative for Hadoop-Hive.

Features
--------
- Written in fast C code
- Only streaming data, so no big buffers to hold data
- Fully UTF-8 compliant

Installation
------------
The only external dependancy of cPrestoClient is LibCurl. This is installed on most
Linux/Debian systems by default. As a convenience, the curl header files are included
in this download (version 7.34).

On Linux/Debian use these commands:

	git clone https://github.com/easydatawarehousing/prestoclient.git  
	cd prestoclient/C  
	cmake .  
	make  

Optionally you can make changes to CMakeLists.txt and run cmake

On Windows, using Visual Studio:
- Download and unzip: https://github.com/easydatawarehousing/prestoclient/archive/master.zip
- Open prestoclient/C/msvc/prestoclient.sln
- Download an appropriate version of cURL: http://curl.haxx.se/download.html  
  Copy the debug version of libcurl.dll to: ./C/msvc/Debug  
  and libcurl.lib files to: ./C/prestoclient/curl/lib/Debug  
  Do the same for the release versions, but to the Release folders.

Usage
-----
Included in this download is file main.c, this may be used as an example of how to use the C interface
defined in prestoclient.h

Basically it implements a commandline utility that takes an sql statement and dumps the results
of that query as comma separated text to stdout.

Execute with:
	cprestoclient "servername" "sql-statement"

ToDo
----
- Implementation of Presto client protocol should be stable
- Add a callback function to recieve status/progress messages while the query is running
- Add functions that handle datatype conversion (string to boolean/long/double)
- This C implementation is meant to be used in other software that needs fast communication with
  a Presto server. Planned is a rewrite of the R version (current version also in this download).
  This client will also be used in the Easy-To-Oracle suite of database connect tools. See:
  http://www.easydatawarehousing.com/easy-to-oracle-free-open-source-data-integration-for-oracle-databases/


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

Availability
------------
Source code is available through: https://github.com/easydatawarehousing/prestoclient

Additional information may be found here: http://www.easydatawarehousing.com/tag/presto/

Copyright
---------
Copyright 2013-2014 Ivo Herweijer | easydatawarehousing.com

Licensed under the GPLv3 license. Optionally a commercial license is available.
(See: http://www.easydatawarehousing.com)
