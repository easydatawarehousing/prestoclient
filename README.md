PrestoClient
============

PrestoClient implements the client protocol to communicate with a Presto server.
Presto [prestodb.io](http://prestodb.io/) is a fast query engine developed
by Facebook that runs distributed queries against a (cluster of)
Hadoop HDFS servers [Hadoop](http://hadoop.apache.org/).
Presto uses SQL as its query language. Presto is an alternative for
Hadoop-Hive.

Versions
--------
PrestoClient currently supports these versions:

- Python  
- R language (deprecated, use the [DBI client](https://github.com/prestodb/RPresto))  
- C  

Comparison
----------
I gathered some statistics on performance and memory use of different versions (in a non-scientific
way by using time and ps). Results are from querying data from one table and writing this in CSV
format to stdout. The resulting csv file would be about 20Mb (but piped into /dev/null).
The average best times are shown here:

| Version    | runtime (sec) | memory (Mb) |
| ---------- | -------------:| -----------:|
| Java (CLI) | 10.5          | 150         |
| Python     | 9.7           | 300         |
| C          | 5.3           | 1           |

Note that the memory usage of the C version is so low because the query data is not stored, only passed
through.

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

License
-------
Most versions are licensed under Apache 2.0. The C version uses the GPLv3 license.
