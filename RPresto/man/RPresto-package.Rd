\name{RPresto-package}
\alias{RPresto-package}
\alias{RPresto}
\docType{package}
\title{Run queries on a Presto server}
\description{PrestoClient provides a method to communicate with a Presto server. Presto is a fast query engine developed by Facebook that runs distributed queries against Hadoop HDFS servers.}
\details{
\tabular{ll}{
Package: \tab RPresto\cr
Type: \tab Package\cr
Version: \tab 0.2.1\cr
Date: \tab 2013-12-15\cr
License: \tab Apache License 2.0\cr
Depends: \tab RCurl, jsonlite\cr
}
}
\author{
Author: Ivo Herweijer\cr
Maintainer: Ivo Herweijer <info@easydatawarehousing.com>
}
\references{
http://prestodb.io/\cr
https://github.com/easydatawarehousing/prestoclient\cr
http://www.easydatawarehousing.com/tag/presto/
}
\keyword{ package }
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
