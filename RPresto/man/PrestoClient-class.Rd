\name{PrestoClient-class}
\Rdversion{1.1}
\docType{class}
\alias{PrestoClient-class}
\alias{PrestoClient}
\title{Class \code{"PrestoClient"}}
\description{
PrestoClient implements a method to communicate with a Presto server. Presto (http://prestodb.io/) is a fast query engine developed by Facebook that runs distributed queries against a (cluster of) Hadoop HDFS servers (http://hadoop.apache.org/).\cr
Presto uses SQL as its query language. Presto is an alternative for Hadoop-Hive.\cr
PrestoClient was developed and tested on Presto 0.54. R version used: 3.0.2, RCurl: 1.95-4.1, jsonlite: 0.9.1
}
\section{Extends}{
All reference classes extend and inherit methods from \code{"\linkS4class{envRefClass}"}.
}
\references{
http://prestodb.io/\cr
https://github.com/easydatawarehousing/prestoclient\cr
http://www.easydatawarehousing.com/tag/presto/
}
\author{
Author: Ivo Herweijer
}
\note{
}
\seealso{
}
\examples{
sql <- "SHOW TABLES"

# Replace localhost with ip address or dns name of the Presto server running the discovery service
pc <- PrestoClient("localhost")

if (!pc$startquery(sql) ) {
  print(pc$getlasterrormessage() )
} else {
  pc$waituntilfinished(TRUE) # Remove True parameter to skip printing status messages
  if (pc$getstatus() == "FAILED") {
    print(pc$getlasterrormessage() )
  }
  
  # We're done now, so let's show the results
  pc$getdata()
}


# Another way to run a query and return the results as a data.frame is:
sql  <- "SHOW TABLES"
pc   <- PrestoClient("localhost")
mydf <- pc$runquery(sql)
}
\keyword{classes}
\section{Fields}{
All member variables are private and should not be used directly. Use the get- methods to retrieve important values.
}
\section{Methods}{
  \describe{
    \item{\code{getversion()}:}{ Return PrestoClient version number. }
    \item{\code{initialize(inServer, inPort, inCatalog, inUser)}:}{ Constructor of PrestoClient class.\cr\cr
        Arguments:\cr
        inServer  -- IP Address or dns name of the Presto server running the discovery service\cr
        inPort    -- TCP port of the Prestoserver running the discovery service (default 8080)\cr
        inCatalog -- Catalog name that the Prestoserver should use to query hdfs (default 'hive')\cr
        inUser    -- Username to pass to the Prestoserver. If left blank the username from the OS is used (default '') }
    \item{\code{runquery(inSqlStatement, inSchema, inVerbose)}:}{ Convenience function to start a query and wait till it is finished and return the data. Currently, only one simultaneous query per instance of the PrestoClient class is allowed.\cr
        Starting a new query will discard any data previously retrieved !\cr\cr
        Arguments:\cr
        inSqlStatement -- The query that should be executed by the Presto server\cr
        inSchema       -- The HDFS schema that should be used (default 'default')\cr
        inVerbose      -- If True print some simple progress messages (default False) }
    \item{\code{startquery(inSqlStatement, inSchema)}:}{ Start a query. Currently, only one simultaneous query per instance of the PrestoClient class is allowed.\cr
        Starting a new query will discard any data previously retrieved !\cr\cr
        Arguments:\cr
        inSqlStatement -- The query that should be executed by the Presto server\cr
        inSchema       -- The HDFS schema that should be used (default 'default')\cr\cr
        Returns TRUE if call succeeded. }
    \item{\code{waituntilfinished(inVerbose)}:}{ Returns when query has finished. Override this function to implement your own data retrieval setup.\cr
        For instance to run this function in a separate thread so other threads may request a cancellation.\cr\cr
        Arguments:\cr
        inVerbose -- If True print some simple progress messages (default False) }
    \item{\code{queryisrunning()}:}{ Returns TRUE if query is running. }
    \item{\code{cancelquery()}:}{ Inform Prestoclient to cancel the running query. When queryisrunning() is called prestoclient will send a cancel query request to the Presto server. }
    \item{\code{getqueryinfo()}:}{ Requests query information from the Presto server and returns this as a dictonary. The Presto server removes this information 15 minutes after finishing the query. }
    \item{\code{cleardata()}:}{ Empty the data buffer. You can use this function to implement your own 'streaming' data retrieval setup. }
    \item{\code{getdata()}:}{ Return the currently buffered data as a data.frame }
    \item{\code{getnumberofdatarows()}:}{ Return the length of the currently buffered data in number of rows. }
    \item{\code{getcolumns()}:}{ Return the column information of the queryresults. data.frame of datatype / fieldname. }
    \item{\code{getlasterrormessage()}:}{ Return error message of last executed request to the prestoserver or empty string if there is no error. }
    \item{\code{getlastserverstate()}:}{ Return state of the request as reported by the Presto server. }
    \item{\code{getstatus()}:}{ Return status of the client. Note this is not the same as the state reported by the Presto server! }
    \item{\code{getlastresponse()}:}{ Return response of last executed request to the prestoserver. }
    \item{\code{openuri(inUri, inSimplifyDF)}:}{ Internal function: sends a GET request to the Presto server. }
    \item{\code{getvarsfromresponse()}:}{ Internal function: retrieves some information from the response of the Presto server. Keeps the last known values, except for 'nextUri'. }
    \item{\code{updatedatatypes()}:}{ Internal function: changes datatype of query data to that specified by the columns info. }
    \item{\code{cancel()}:}{ Internal function: sends a cancel request to the Prestoserver. }
  }
}