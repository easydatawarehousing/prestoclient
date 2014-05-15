/*
* This file is part of cPrestoClient
*
* Copyright (C) 2014 Ivo Herweijer
*
* cPrestoClient is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* You can contact me via email: info@easydatawarehousing.com
*/

// Do not include this file in your project
// These are private defines and type declarations for prestoclient
// Use "prestoclient.h" instead

#ifndef EASYPTORA_PRESTOCLIENTTYPES_HH
#define EASYPTORA_PRESTOCLIENTTYPES_HH

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <curl/curl.h>

/* --- Defines -------------------------------------------------------------------------------------------------------- */
#define PRESTOCLIENT_QUERY_URL "/v1/statement";				// URL added to servername to start a query
#define PRESTOCLIENT_CURL_BUFFERSIZE CURL_MAX_WRITE_SIZE;	// (Preferred) Bufferzize for communication with curl
#define PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST 200;			// Expected http response code for get and post requests
#define PRESTOCLIENT_CURL_EXPECT_HTTP_DELETE   204;			// Expected http response code for delete requests
#define PRESTOCLIENT_CURL_EXPECT_HTTP_BUSY     503;			// Expected http response code when presto server is busy

/* --- Enums ---------------------------------------------------------------------------------------------------------- */
enum E_RESULTCODES
{
	PRESTOCLIENT_RESULT_OK = 0,
	PRESTOCLIENT_RESULT_BAD_REQUEST_DATA,
	PRESTOCLIENT_RESULT_SERVER_ERROR,
	PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED,
	PRESTOCLIENT_RESULT_CURL_ERROR,
	PRESTOCLIENT_RESULT_PARSE_JSON_ERROR
};

enum E_HTTP_REQUEST_TYPES
{
	PRESTOCLIENT_HTTP_REQUEST_TYPE_GET,
	PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
	PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE
};

enum E_JSON_READSTATES
{
	JSON_RS_SEARCH_OBJECT = 0
,	JSON_RS_READ_STRING
,	JSON_RS_READ_NONSTRING
};

enum E_JSON_CONTROL_CHARS
{
	JSON_CC_NONE = 0
,	JSON_CC_WS			// Whitespace (space, tab, line feed, form feed, carriage return)
,	JSON_CC_OO			// Object open {
,	JSON_CC_OC			// Object close }
,	JSON_CC_AO			// Array open [
,	JSON_CC_AC			// Array close ]
,	JSON_CC_BS			// Backslash
,	JSON_CC_QT			// Double quote
,	JSON_CC_COLON		// Colon
,	JSON_CC_COMMA		// Comma
};

enum E_JSON_TAGTYPES
{
	JSON_TT_UNKNOWN = 0
,	JSON_TT_STRING
,	JSON_TT_NUMBER
,	JSON_TT_OBJECT_OPEN
,	JSON_TT_OBJECT_CLOSE
,	JSON_TT_ARRAY_OPEN
,	JSON_TT_ARRAY_CLOSE
,	JSON_TT_COLON
,	JSON_TT_COMMA
,	JSON_TT_TRUE
,	JSON_TT_FALSE
,	JSON_TT_NULL
};

/* --- Typedefs ------------------------------------------------------------------------------------------------------- */
#ifndef bool
#define bool	signed char
#define true	1
#define false	0
#endif

/* --- Structs -------------------------------------------------------------------------------------------------------- */
typedef struct ST_JSONPARSER
{
	enum E_JSON_READSTATES		  state;						// State of state-machine
	bool						  isbackslash;					// If true, the previous character was a BS
	unsigned int				  readposition;					// Readposition within curl buffer
	bool						  skipnextread;					// If true don't read the next character, keep the current character
	bool						  error;						// Set to true when a parse error is detected
	char						 *c;							// Current character
	enum E_JSON_CONTROL_CHARS	  control;						// Meaning of current character as control character
	unsigned int				  clength;						// Length of current character (1..4 bytes)
	char						 *tagbuffer;					// Buffer for storing tag that is currently being read
	unsigned int				  tagbuffersize;				// Maximum size of tag buffer
	unsigned int				  tagbufferactualsize;			// Actual size of tag buffer
	enum E_JSON_TAGTYPES		  tagtype;						// Type of value returned by the parser
} JSONPARSER;

typedef struct ST_JSONLEXER
{
	enum E_JSON_TAGTYPES		  previoustag;					// json type of the previous tag
	enum E_JSON_TAGTYPES		 *tagorder;						// Array containing types of json parent elements of the current tag
	char						**tagordername;					// Array containing name of json parent elements
	unsigned int				  tagordersize;					// Maximum number of elements for tagorder array
	unsigned int				  tagorderactualsize;			// Actual number of elements used in tagorder array
	unsigned int				  column;						// Column index of current tag
	bool						  error;						// Set to true when a lexer error is detected
	char						 *name;							// Last found name string
	unsigned int				  namesize;						// Maximum length of name
	unsigned int				  nameactualsize;				// Actual length of name
	char						 *value;						// Last found value string
	unsigned int				  valuesize;					// Maximum length of value
	unsigned int				  valueactualsize;				// Actual length of value

} JSONLEXER;

typedef struct ST_PRESTOCLIENT_FIELD
{
	char						 *name;							// Name of column
	enum E_FIELDTYPES			  type;							// Type of field
	char						 *data;							// Buffer for fielddata
	unsigned int				  datasize;						// Size of data buffer
	bool						  dataisnull;					// Set to true if content of data is null
} PRESTOCLIENT_FIELD;

typedef struct ST_PRESTOCLIENT PRESTOCLIENT;

typedef struct ST_PRESTOCLIENT_RESULT
{
	PRESTOCLIENT				 *client;						// Pointer to PRESTOCLIENT
	CURL						 *hcurl;						// Handle to libCurl
	char						 *curl_error_buffer;			// Buffer for storing curl error messages
	void (*write_callback_function)(void*, void*);				// Functionpointer to client function handling queryoutput
	void (*describe_callback_function)(void*, void*);			// Functionpointer to client function handling output description
	void						 *client_object;				// Pointer to object to pass to client function
	char						 *lastinfouri;					// Uri to query information on the Presto server
	char						 *lastnexturi;					// Uri to next dataframe on the Presto server
	char						 *lastcanceluri;				// Uri to cancel query on the Presto server
	char						 *laststate;					// State returned by last request to Presto server
	char						 *lasterrormessage;				// Last error message returned by Presto server
	enum E_CLIENTSTATUS			  clientstatus;					// Status defined by PrestoClient: NONE, RUNNING, SUCCEEDED, FAILED
	bool						  cancelquery;					// Boolean, when set to true signals that query should be cancelled
	char						 *lastresponse;					// Buffer for curl response
	size_t						  lastresponsebuffersize;		// Maximum size of the curl buffer
	size_t						  lastresponseactualsize;		// Actual size of the curl buffer
	PRESTOCLIENT_FIELD			**columns;						// Buffer for the column information returned by the query
	unsigned int				  columncount;					// Number of columns in output or 0 if unknown
	bool						  columninfoavailable;			// Flag set to true if columninfo is available and complete (also used for json lexer)
	bool						  columninfoprinted;			// Flag set to true if columninfo has been printed to output
	int							  currentdatacolumn;			// Index to datafield (columns array) currently handled or -1 when not parsing field data
	bool						  dataavailable;				// Flag set to true if a row of data is available
	enum E_RESULTCODES			  errorcode;					// Errorcode, set when terminating a request
	JSONPARSER					 *json;							// Pointer to the json parser
	JSONLEXER					 *lexer;						// Pointer to the json lexer
} PRESTOCLIENT_RESULT;

typedef struct ST_PRESTOCLIENT
{
	char						 *useragent;					// Useragent name sent to Presto server
	char						 *server;						// IP address or DNS name of Presto server
	unsigned int				  port;							// TCP port of Presto server
	char						 *catalog;						// Catalog name to be used by Presto server
	char						 *user;							// Username to pass to Presto server
	char						 *timezone;						// Timezone to pass to Presto server
	char						 *language;						// Language to pass to Presto server
	PRESTOCLIENT_RESULT			**results;						// Array containing query status and data
	unsigned int				  active_results;				// Number of queries issued
} PRESTOCLIENT;

/* --- Functions ------------------------------------------------------------------------------------------------------ */

// Utility functions
extern char* get_username();
extern void util_sleep(const int sleeptime_msec);

// Memory handling functions
extern void alloc_copy(char **var, const char *newvalue);
extern void alloc_add(char **var, const char *addedvalue);
extern PRESTOCLIENT_FIELD* new_prestofield();

// JSON Functions
extern bool json_reader(PRESTOCLIENT_RESULT* result);
extern void json_delete_parser(JSONPARSER* json);
extern void json_delete_lexer(JSONLEXER* lexer);
extern void json_reset_lexer(JSONLEXER* lexer);

#endif // EASYPTORA_PRESTOCLIENTTYPES_HH