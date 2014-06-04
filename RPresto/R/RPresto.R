# PrestoClient provides a method to communicate with a Presto server. Presto is a fast query
# engine developed by Facebook that runs distributed queries against Hadoop HDFS servers.
#
# Copyright 2013 Ivo Herweijer | easydatawarehousing.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

library("RCurl")
library("jsonlite")

###################################################################################################################
PrestoClient <- setRefClass(
  "PrestoClient"
,
  fields = list(
    mSource = "character",            # Client name sent to Presto server
    mVersion = "character",           # PrestoClient version string
    mUseragent = "character",         # Useragent name sent to Presto server
    mUrlTimeout = "numeric",          # Timeout in millisec to wait for Presto server to respond
    mUpdateWaittimemsec = "numeric",  # Wait time in millisec to wait between requests to Presto server
    #mRetryWaittimemsec = "numeric",   # Wait time in millisec to wait before retrying a request
    #mMaximumRetries = "numeric",      # Maximum number of retries fro request in case of 503 errors
    mServer = "character",            # IP address or DNS name of Presto server
    mPort = "numeric",                # TCP port of Presto server
    mCatalog = "character",           # Catalog name to be used by Presto server
    mUser = "character",              # Username to pass to Presto server
    mLasterror = "character",         # Html error of last request
    mLastinfouri = "character",       # Uri to query information on the Presto server
    mLastnexturi = "character",       # Uri to next dataframe on the Presto server
    mLastcanceluri = "character",     # Uri to cancel query on the Presto server
    mLaststate = "character",         # State returned by last request to Presto server
    mClientStatus = "character",      # Status defined by PrestoClient: NONE, RUNNING, SUCCEEDED, FAILED
    mCancelquery = "logical",         # Boolean, when set to True signals that query should be cancelled
    mResponse = "list",               # Buffer for last response of Presto server
    mColumns = "data.frame",          # Buffer for the column information returned by the query
    mData = "data.frame"              # Buffer for the data returned by the query
  )
,
  methods = list(
    initialize = function(inServer, inPort=8080, inCatalog="hive", inUser="") {
      mSource             <<- "RPresto"
      mVersion            <<- "0.2.1"
      mUseragent          <<- paste(mSource, "/", mVersion, sep = "")
      mUrlTimeout         <<- 5000
      mUpdateWaittimemsec <<- 1500
      #mRetryWaittimemsec  <<- 100
      #mMaximumRetries     <<- 5
      mServer             <<- inServer
      mPort               <<- inPort
      mCatalog            <<- inCatalog      
      mUser               <<- ifelse(inUser == "", Sys.info()["user"], inUser)
      mLasterror          <<- ""
      mLastinfouri        <<- ""
      mLastnexturi        <<- ""
      mLastcanceluri      <<- ""
      mLaststate          <<- ""
      mClientStatus       <<- "NONE"
      mCancelquery        <<- FALSE
      mResponse           <<- list()
      mColumns            <<- data.frame()
      mData               <<- data.frame()
    },
    getversion = function() {
      return(mVersion)
    },
    getlastresponse = function() {
      return(mResponse)
    },
    getstatus = function() {
      return(mClientStatus)
    },
    getlastserverstate = function() {
      return(mLaststate)
    },
    getlasterrormessage = function() {
      return(mLasterror)
    },
    getcolumns = function() {
      return(mColumns)
    },
    getnumberofdatarows = function() {
      return(nrow(mData) )
    },
    getdata = function() {
      return(mData)
    },
    cleardata = function() {
      return(mData <<- data.frame() )
    },
    startquery = function(inSqlStatement, inSchema="default") {
      success <- FALSE

      header <- c("X-Presto-Catalog" = mCatalog,
                  "X-Presto-Source"  = mSource,
                  "X-Presto-Schema"  = inSchema,
                  "User-Agent"       = mUseragent,
                  "X-Presto-User"    = mUser)
      
      # Prepare statement
      sql <- inSqlStatement
      sql <- sub(" +$", "", sql)
      sql <- sub(";+$", "", sql)
      
      if (sql == "") {
        mLasterror <<- "No query entered"
      } else {
        # Check current state, any status except running is okay
        if (mClientStatus == "RUNNING") {
          mLasterror <<- "Query already running. Please create a new instance of PrestoClient class"
        } else {
          # Reset variables
          mLasterror     <<- ""
          mLastinfouri   <<- ""
          mLastnexturi   <<- ""
          mLastcanceluri <<- ""
          mLaststate     <<- ""
          mCancelquery   <<- FALSE
          mResponse      <<- list()
          mColumns       <<- data.frame()
          mData          <<- data.frame()
          
          resp    <- basicTextGatherer()
          handle  <- getCurlHandle()
          url     <- paste("http://", mServer, ":", mPort, "/v1/statement", sep = "")
          
          tryCatch(curlPerform(url = url, httpheader=header, postfields=sql, writefunction=resp$update, curl=handle, timeout.ms=mUrlTimeout, followlocation=TRUE),
                   error = function(x) {
                     mLasterror <<- paste("Error connecting to server: ", mServer, ":", as.character(mPort), " : ", class(x)[1], sep = "")
                   })
          
          if (mLasterror == "") {
            status <- getCurlInfo(handle)$response.code
            
            if (status != 200) {
              mLasterror <<- paste("Connection error:", as.character(status) )
            } else {
              mResponse <<- jsonlite::fromJSON(resp$value())
              getvarsfromresponse()
              success <- TRUE
            }
          }
        }
      }
      
      return(success)
    },
    runquery = function(inSqlStatement, inSchema="default", inVerbose=FALSE) {
      if (!startquery(inSqlStatement, inSchema) ) {
        print(mLasterror)
      } else {
        waituntilfinished(inVerbose)
        if (mLasterror == "FAILED") print(mLasterror)
      }
      return(mData)
    },
    waituntilfinished = function(inVerbose=FALSE) {
      tries <- 0
      
      while (queryisrunning() ) {
        if (inVerbose) {
          tries <- tries + 1
          print(paste("Ping: ", as.character(tries), " Rows =", as.character(getnumberofdatarows())))
        }
        
        Sys.sleep(mUpdateWaittimemsec / 1000)
      }
      
      if (inVerbose) print(paste("Done: ", as.character(tries), " Rows =", as.character(getnumberofdatarows())))
      
      updatedatatypes()
    },
    queryisrunning = function() {
      running <- TRUE
      
      if (mCancelquery) {
        cancel()
        running <- FALSE
      }
      else {
        if (mLastnexturi == "") {
          running <- FALSE
        }
        else {
          if (!openuri(mLastnexturi) ) {
            running <- FALSE
          }
          else {
            getvarsfromresponse()
            
            if (mLastnexturi == "") {
              running <- FALSE
            }
          }
        }
      }
      
      return(running)
    },
    getqueryinfo = function() {
      info <- list()

      if (mLastinfouri != "") {
        if (openuri(mLastinfouri, FALSE) ) {
          info <- mResponse
        }
      }
      
      return(info)
    },
    cancelquery = function() {
      mCancelquery <<- TRUE
    },
    openuri = function(inUri, inSimplifyDF=TRUE) {
      success <- FALSE
      mLasterror <<- ""
      header <- c("X-Presto-Source"  = mSource,
                  "User-Agent"       = mUseragent,
                  "X-Presto-User"    = mUser)
      
      # Todo: implement handling of 503 response
      resp    <- basicTextGatherer()
      handle  <- getCurlHandle()
      tryCatch(curlPerform(url = inUri, httpheader=header, writefunction=resp$update, curl=handle, timeout.ms=mUrlTimeout, followlocation=TRUE),
               error = function(x) {
                 mLasterror <<- paste("URL error: ", inUri, " : ", class(x)[1], sep = "")
               })
      
      if (mLasterror == "") {
        mResponse <<- jsonlite::fromJSON(resp$value(), simplifyDataFrame=inSimplifyDF)
        success    <- TRUE
      }
      
      return(success)
    },
    getvarsfromresponse = function() {
      mLastnexturi <<- ""
      if ("infoUri" %in% names(mResponse) )            mLastinfouri   <<- mResponse$infoUri
      if ("nextUri" %in% names(mResponse) )            mLastnexturi   <<- mResponse$nextUri
      if ("partialCancelUri" %in% names(mResponse) )   mLastcanceluri <<- mResponse$partialCancelUri
      if ("state" %in% names(mResponse) )              mLaststate     <<- mResponse$state
      else {
        if ("stats" %in% names(mResponse) & "state" %in% names(mResponse$stats) )
          mLaststate     <<- mResponse$stats$state
      }

      if ("state" %in% names(mResponse) )              mLaststate     <<- mResponse$state
      
      if (length(mColumns) == 0) {
        if ("columns" %in% names(mResponse) )          mColumns       <<- mResponse$columns
      }
      
      # Add/append data
      if ("data" %in% names(mResponse) ) {
        if (length(mData) == 0) {
          mData          <<- data.frame(mResponse$data, stringsAsFactors=FALSE)
          names(mData)   <<- mColumns[,1]
        }        
        else {
          tempdata        <- data.frame(mResponse$data, stringsAsFactors=FALSE)
          names(tempdata) <- mColumns[,1]
          mData          <<- rbind(mData, tempdata)
        }
      }
      
      # Determine state
      if (mLastnexturi != "") {
        mClientStatus <<- "RUNNING"
      }
      else {
        if ("error" %in% names(mResponse) ) {
          mClientStatus <<- "FAILED"
          
          # Get errormessage
          if ("failureInfo" %in% names(mResponse$error) & "message" %in% names(mResponse$error$failureInfo) ) {
            mLasterror <<- mResponse$error$failureInfo$message
          }
        }
        else {
          mClientStatus <<- "SUCCEEDED"
        }
      }
    },
    updatedatatypes = function() {
      if (length(mColumns) > 0 & length(mData) > 0) {
        for (i in 1:nrow(mColumns) ) {
          if (mColumns[i,2] == "bigint" | mColumns[i,2] == "double") {
            mData[,i] <<- as.numeric(mData[,i])
          } else {
            if (mColumns[i,2] == "boolean") {
              mData[,i] <<- as.logical(mData[,i])
            }
          }
        }
      }
    },
    cancel = function() {
      success <- FALSE
      mLasterror <<- ""
      
      if (mLastcanceluri != "") {
        header <- c("X-Presto-Source"  = mSource,
                    "User-Agent"       = mUseragent,
                    "X-Presto-User"    = mUser)
        
        resp    <- basicTextGatherer()
        handle  <- getCurlHandle()
        tryCatch(httpDELETE(url = inUri, httpheader=header, writefunction=resp$update, curl=handle, timeout.ms=mUrlTimeout),
                 error = function(x) {
                   mLasterror <<- paste("URL error: ", mLastcanceluri, " : ", class(x)[1], sep = "")
                 })
        
        if (mLasterror == "") {
          success    <- TRUE
          mClientStatus <<- "NONE"
        }
      }
      
      return(success)
    }
  )
)