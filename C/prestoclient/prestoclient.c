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

#include "prestoclient.h"
#include "prestoclienttypes.h"
#include <assert.h>

/* --- Private functions ---------------------------------------------------------------------------------------------- */

// malloc/realloc memory for the variable and copy the newvalue to the variable. Exit on failure
void alloc_copy(char **var, const char *newvalue)
{
	unsigned int newlength, currlength = 0;

	assert(var);
	assert(newvalue);

	newlength = (strlen(newvalue) + 1) * sizeof(char);

	if (*var)
	{
		// realloc
		currlength = (strlen(*var) + 1) * sizeof(char);

		if (currlength < newlength)
			*var = (char*)realloc( (char*)*var, newlength);
	}
	else
	{
		// malloc
		*var = (char*)malloc(newlength);
	}

	// Not doing rigorous checking and handling of all malloc's because:
	// - On the intended platforms for this code (Linux, windows boxes with lots of (virtual)memory) malloc failures are very rare
	// - Because such failures are rare it's very difficult to test proper malloc handling code
	// - Handling failures will likely also fail
	// Whenever an alloc fails we're doing an: exit(1)
	if (!var)
		exit(1);

	strcpy(*var, newvalue);
}

void alloc_add(char **var, const char *addedvalue)
{
	unsigned int newlength, currlength = 0;

	assert(var);
	assert(addedvalue);

	newlength = (strlen(addedvalue) + 1) * sizeof(char);

	if (*var)
	{
		currlength = (strlen(*var) + 2) * sizeof(char);
		*var = (char*)realloc( (char*)*var, currlength + newlength);
	}
	else
	{
		*var = (char*)malloc(newlength);
		(*var)[0] = 0;
	}

	if (!var)
		exit(1);

	if (strlen(*var) > 0)
		strcat(*var, "\n");

	strcat(*var, addedvalue);
}

PRESTOCLIENT_FIELD* new_prestofield()
{
	PRESTOCLIENT_FIELD* field = (PRESTOCLIENT_FIELD*)malloc( sizeof(PRESTOCLIENT_FIELD) );

	if (!field)
		exit(1);

	field->name       = NULL;
	field->type       = PRESTOCLIENT_TYPE_VARCHAR;
	field->datasize   = 1024 * sizeof(char);
	field->data       = (char*)malloc(field->datasize + 1);
	field->dataisnull = false;

	if (!field->data)
		exit(1);

	return field;
}

static PRESTOCLIENT_RESULT* new_prestoresult()
{
	PRESTOCLIENT_RESULT* result = (PRESTOCLIENT_RESULT*)malloc( sizeof(PRESTOCLIENT_RESULT) );

	if (!result)
		exit(1);

	result->client                 = NULL;
	result->hcurl                  = NULL;
	result->curl_error_buffer      = NULL;
	result->lastinfouri            = NULL;
	result->lastnexturi            = NULL;
	result->lastcanceluri          = NULL;
	result->laststate              = NULL;
	result->lasterrormessage       = NULL;
	result->clientstatus           = PRESTOCLIENT_STATUS_NONE;
	result->cancelquery            = false;
	result->lastresponse           = NULL;
	result->lastresponsebuffersize = 0;
	result->lastresponseactualsize = 0;
	result->columns                = NULL;
	result->columncount            = 0;
	result->columninfoavailable    = false;
	result->columninfoprinted      = false;
	result->currentdatacolumn      = -1;
	result->dataavailable          = false;
	result->errorcode              = PRESTOCLIENT_RESULT_OK;
	result->json                   = NULL;
	result->lexer                  = NULL;

	return result;
}

static PRESTOCLIENT* new_prestoclient()
{
	PRESTOCLIENT* client = (PRESTOCLIENT*)malloc( sizeof(PRESTOCLIENT) );

	if (!client)
		exit(1);

	client->useragent      = NULL;
	client->server         = NULL;
	client->port           = PRESTOCLIENT_DEFAULT_PORT;
	client->catalog        = NULL;
	client->user           = NULL;
	client->results        = NULL;
	client->active_results = 0;

	return client;
}

static void delete_prestofield(PRESTOCLIENT_FIELD* field)
{
	if (!field)
		return;

	if (field->name)
		free(field->name);

	if (field->data)
		free(field->data);

	free(field);
}

// Add this result set to the PRESTOCLIENT
static void register_result(PRESTOCLIENT_RESULT* result)
{
	PRESTOCLIENT *client;

	if (!result)
		return;

	if (!result->client)
		return;

	client = result->client;

	client->active_results++;

	if (client->active_results == 1)
		client->results = (PRESTOCLIENT_RESULT**)malloc( sizeof(PRESTOCLIENT_RESULT*) );
	else
		client->results = (PRESTOCLIENT_RESULT**)realloc( (PRESTOCLIENT_RESULT**)client->results, client->active_results * sizeof(PRESTOCLIENT_RESULT*) );

	if (!client->results)
		exit(1);

	client->results[client->active_results - 1] = result;
}

// Delete this result set from memory and remove from PRESTOCLIENT
static void delete_prestoresult(PRESTOCLIENT_RESULT* result)
{
	unsigned int i;

	if (!result)
		return;

	if (result->hcurl)
	{
		curl_easy_cleanup(result->hcurl);

		if (result->curl_error_buffer)
			free(result->curl_error_buffer);
	}

	json_delete_parser(result->json);

	json_delete_lexer(result->lexer);

	if (result->lastinfouri)
		free(result->lastinfouri);

	if (result->lastnexturi)
		free(result->lastnexturi);

	if (result->lastcanceluri)
		free(result->lastcanceluri);

	if (result->laststate)
		free(result->laststate);

	if (result->lasterrormessage)
		free(result->lasterrormessage);

	if (result->lastresponse)
		free(result->lastresponse);

	for (i = 0; i < result->columncount; i++)
		delete_prestofield(result->columns[i]);

	if (result->columns)
		free(result->columns);

	free(result);
}

// Add a key/value to curl header list
static void add_headerline(struct curl_slist **header, char *name, char *value)
{
	int length = (strlen(name) + strlen(value) + 3) * sizeof(char);
	char *line = (char*)malloc(length);

	if (!line)
		exit(1);

	strcpy(line, name);
	strcat(line, ": ");
	strcat(line, value);
	strcat(line, "\0");

	*header = curl_slist_append(*header, line);

	free(line);
}

// Callback function for CURL data. Data is added to the resultset databuffer
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t contentsize = size * nmemb;
	PRESTOCLIENT_RESULT	*result = (PRESTOCLIENT_RESULT*)userp;

	// Do we need a bigger buffer ? Should not happen
	if (result->lastresponseactualsize + contentsize > result->lastresponsebuffersize)
	{
		result->lastresponse = (char*)realloc(result->lastresponse, result->lastresponseactualsize + contentsize + 1);

		if (!result->lastresponse)
			exit(1);

		result->lastresponsebuffersize = result->lastresponseactualsize + contentsize + 1;
	}

	// Add contents to buffer
	memcpy( &(result->lastresponse[result->lastresponseactualsize]), contents, contentsize);

	// Update actual size
	result->lastresponseactualsize += contentsize;

	// Add terminating zero
	result->lastresponse[result->lastresponseactualsize] = 0;

	// Start/continue parsing json. Stop on errors
	if (!json_reader(result) )
		return 0;

	// Return number of bytes processed or zero if the query should be cancelled
	return (result->cancelquery ? 0 : contentsize);
}

// Send a http request to the Presto server. in_uri is emptied
static unsigned int openuri(enum E_HTTP_REQUEST_TYPES in_request_type,
					CURL *hcurl,
					const char *in_server,
					unsigned int *in_port,
					char **in_uri,
					const char *in_body,
					const char *in_catalog,
					const char *in_schema,
					const char *in_useragent,
					const char *in_user,
					const unsigned long *in_buffersize,
					PRESTOCLIENT_RESULT *result
					)
{
	CURLcode curlstatus;
	char *uasource, *query_url, *full_url, port[32];
	struct curl_slist *headers;
	bool retry;
    unsigned int retrycount, length;
	long http_code, expected_http_code, expected_http_code_busy;

	uasource   = PRESTOCLIENT_SOURCE;
	query_url  = PRESTOCLIENT_QUERY_URL;
	headers    = NULL;
	expected_http_code_busy = PRESTOCLIENT_CURL_EXPECT_HTTP_BUSY;

	// Check parameters
	if (!hcurl        ||
		!in_useragent ||
		!in_user      ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST   && ( !in_server || !in_port || !in_body || !in_catalog || !in_schema || !in_buffersize || !result) ) ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_GET    && ( !in_uri || !*in_uri || !result) ) ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE && ( !in_uri || !*in_uri) )
		)
	{
		result->errorcode = PRESTOCLIENT_RESULT_BAD_REQUEST_DATA;
		return result->errorcode;
	}

	// Set up curl error buffer
	if (!result->curl_error_buffer)
	{
		result->curl_error_buffer = (char*)calloc(CURL_ERROR_SIZE, sizeof(char) );
		// Not a fatal error if alloc didn't succeed
	}

	if (result->curl_error_buffer)
	{
		result->curl_error_buffer[0] = 0;
		curl_easy_setopt(hcurl, CURLOPT_ERRORBUFFER, result->curl_error_buffer);
	}

	// URL
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST)
	{
		// Port
		sprintf(port, "%i", *in_port);

		// Url
		length = (strlen(query_url) + strlen(in_server) + 15) * sizeof(char);	// 15 = http:// :12345
		full_url = (char*)malloc(length);

		if (!full_url)
			exit(1);

		strcpy(full_url, "http://");
		strcat(full_url, in_server);
		strcat(full_url, ":");
		strcat(full_url, port);
		strcat(full_url, query_url);
		curl_easy_setopt(hcurl, CURLOPT_URL, full_url);
		free(full_url);
	}
	else
	{
		curl_easy_setopt(hcurl, CURLOPT_URL, *in_uri);
		*in_uri[0] = 0;
	}

	// CURL options
	curl_easy_setopt(hcurl, CURLOPT_CONNECTTIMEOUT_MS, (long)PRESTOCLIENT_URLTIMEOUT );

	switch (in_request_type)
	{
		case PRESTOCLIENT_HTTP_REQUEST_TYPE_POST:
		{
			expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST;
			curl_easy_setopt(hcurl, CURLOPT_POST, (long)1 );
			curl_easy_setopt(hcurl, CURLOPT_BUFFERSIZE, (long)(in_buffersize - 1) );
			break;
		}

		case PRESTOCLIENT_HTTP_REQUEST_TYPE_GET:
		{
			expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST;
			curl_easy_setopt(hcurl, CURLOPT_HTTPGET, (long)1 );
			curl_easy_setopt(hcurl, CURLOPT_BUFFERSIZE, (long)(in_buffersize - 1) );
			break;
		}

		case PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE:
		{
			expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_DELETE;
			curl_easy_setopt(hcurl, CURLOPT_HTTPGET, (long)1 );
			curl_easy_setopt(hcurl, CURLOPT_CUSTOMREQUEST, "DELETE");
			break;
		}
	}

	// HTTP Headers
	if (in_catalog)		add_headerline(&headers, "X-Presto-Catalog", (char*)in_catalog);
						add_headerline(&headers, "X-Presto-Source",  uasource);
	if (in_schema)		add_headerline(&headers, "X-Presto-Schema",  (char*)in_schema);
	if (in_useragent)	add_headerline(&headers, "User-Agent",       (char*)in_useragent);
	if (in_user)		add_headerline(&headers, "X-Presto-User",    (char*)in_user);

	// Set Writeback function and request body
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST || in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_GET)
	{
		curl_easy_setopt(hcurl, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(hcurl, CURLOPT_WRITEDATA,     (void*)result);
	}

	// Set request body
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST)
	{
		curl_easy_setopt(hcurl, CURLOPT_POSTFIELDS,    in_body);
		curl_easy_setopt(hcurl, CURLOPT_POSTFIELDSIZE, (long)strlen(in_body) );
	}

	// Set header
	curl_easy_setopt(hcurl, CURLOPT_HTTPHEADER, headers);

	// Execute CURL request, retry when server is busy
	result->errorcode = PRESTOCLIENT_RESULT_OK;
	retry      = true;
	retrycount = 0;

	while (retry)
	{
		retrycount++;

		// Execute request
		curlstatus = curl_easy_perform(hcurl);

		if (curlstatus == CURLE_OK)
		{
			// Get return code
			http_code = 0;
			curl_easy_getinfo(hcurl, CURLINFO_RESPONSE_CODE, &http_code);

			if (http_code == expected_http_code)
			{
				retry  = false;
			}
			else if (http_code == expected_http_code_busy)
			{
				// Server is busy
				util_sleep(PRESTOCLIENT_RETRYWAITTIMEMSEC * retrycount);
			}
			else
			{
				result->errorcode = PRESTOCLIENT_RESULT_SERVER_ERROR;
				// Re-using port buffer
				sprintf(port, "Http-code: %d", (unsigned int)http_code);
				alloc_copy(&result->curl_error_buffer, port);
				retry = false;
			}

			if (retry && retrycount > PRESTOCLIENT_MAXIMUMRETRIES)
			{
				result->errorcode = PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED;
				retry = false;
			}
		}
		else
		{
			result->errorcode = PRESTOCLIENT_RESULT_CURL_ERROR;
			retry = false;
		}
	}

	// Cleanup
	curl_slist_free_all(headers);

	return result->errorcode;
}

// Send a cancel request to the Prestoserver
static void cancel(PRESTOCLIENT_RESULT *result)
{
	if (result->lastcanceluri)
	{
		// Not checking returncode since we're cancelling the request and don't care if it succeeded or not
		openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE,
					result->hcurl,
					NULL,
					NULL,
					&result->lastcanceluri,
					NULL,
					NULL,
					NULL,
					result->client->useragent,
					result->client->user,
					NULL,
					(void*)result);
	}
}

// Fetch the next uri from the prestoserver, handle the response and determine if we're done or not
static bool prestoclient_queryisrunning(PRESTOCLIENT_RESULT *result)
{
	PRESTOCLIENT* prestoclient;

	if (!result)
		return false;

	if (result->cancelquery)
	{
		cancel(result);
		return false;
	}

	// Do we have a url ?
	if (!result->lastnexturi || strlen(result->lastnexturi) == 0)
		return false;

	prestoclient = result->client;
	if (!prestoclient)
		return false;

	// Start request. This will execute callbackfunction when data is recieved
	if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_GET,
					result->hcurl,
					NULL,
					NULL,
					&result->lastnexturi,
					NULL,
					NULL,
					NULL,
					prestoclient->useragent,
					prestoclient->user,
					NULL,
					(void*)result) == PRESTOCLIENT_RESULT_OK)
	{
		// Determine client state
		if (result->lastnexturi && strlen(result->lastnexturi) > 0)
			result->clientstatus = PRESTOCLIENT_STATUS_RUNNING;
		else
		{
			if (result->lasterrormessage && strlen(result->lasterrormessage) > 0)
				result->clientstatus = PRESTOCLIENT_STATUS_FAILED;
			else
				result->clientstatus = PRESTOCLIENT_STATUS_SUCCEEDED;
		}

		// Update columninfoavailable flag
		if (result->columncount > 0 && !result->columninfoavailable)
			result->columninfoavailable = true;

		// Call print header callback function
		if (!result->columninfoprinted && result->columninfoavailable)
		{
			result->columninfoprinted = true;

			if (result->describe_callback_function)
				result->describe_callback_function(result->client_object, (void*)result);
		}

		// Clear lexer data for next run
		json_reset_lexer(result->lexer);
	}
	else
	{
		return false;
	}
	
	if (!result->lastnexturi || strlen(result->lastnexturi) == 0)
		return false;

	return true;
}

// Start fetching packets until we're done. Wait for a specified interval between requests
static void prestoclient_waituntilfinished(PRESTOCLIENT_RESULT *result)
{
	while( prestoclient_queryisrunning(result) )
	{
		// Once there is data use the short wait interval
		if (result->dataavailable)
		{
			util_sleep(PRESTOCLIENT_RETRIEVEWAITTIMEMSEC);
		}
		else
		{
			util_sleep(PRESTOCLIENT_UPDATEWAITTIMEMSEC);
		}
	}
}

/* --- Public functions ----------------------------------------------------------------------------------------------- */
char* prestoclient_getversion()
{
	return PRESTOCLIENT_VERSION;
}

PRESTOCLIENT* prestoclient_init(const char *in_server, const unsigned int *in_port, const char *in_catalog,
								const char *in_user, const char *in_pwd)
{
	PRESTOCLIENT *client = NULL;
	char *uasource, *uaversion, *defaultcatalog;
	unsigned int length;

	uasource       = PRESTOCLIENT_SOURCE;
	uaversion      = PRESTOCLIENT_VERSION;
	defaultcatalog = PRESTOCLIENT_DEFAULT_CATALOG;

	(void)in_pwd;	// Get rid of compiler warning
	
	if (in_server && strlen(in_server) > 0)
	{
		client = new_prestoclient();

		length = (strlen(uasource) + strlen(uaversion) + 2) * sizeof(char);
		client->useragent = (char*)malloc(length);
		if (!client->useragent)
			exit(1);

		strcpy(client->useragent, uasource);
		strcat(client->useragent, "/");
		strcat(client->useragent, uaversion);

		// TODO: check if in_server contains a port ?

		alloc_copy(&client->server, in_server);

		if (in_port && *in_port > 0 && *in_port <= 65535)
			client->port = *in_port;

		alloc_copy(&client->catalog, in_catalog ? in_catalog : defaultcatalog);

		if (in_user)
		{
			alloc_copy(&client->user, in_user);
		}
		else
		{
			client->user = get_username();
		}
	}

	return client;
}

void prestoclient_close(PRESTOCLIENT *prestoclient)
{
	unsigned int i;

	if (prestoclient)
	{
		if (prestoclient->useragent)
			free(prestoclient->useragent);

		if (prestoclient->server)
			free(prestoclient->server);

		if (prestoclient->catalog)
			free(prestoclient->catalog);

		if (prestoclient->user)
			free(prestoclient->user);

		if (prestoclient->results)
		{
			for (i = 0; i < prestoclient->active_results; i++)
				delete_prestoresult(prestoclient->results[i]);

			free(prestoclient->results);
		}

		free(prestoclient);
		prestoclient = NULL;
	}
}

PRESTOCLIENT_RESULT* prestoclient_query(PRESTOCLIENT *prestoclient, const char *in_sql_statement, const char *in_schema,
										void (*in_write_callback_function)(void*, void*),
										void (*in_describe_callback_function)(void*, void*),
										void *in_client_object)
{
	PRESTOCLIENT_RESULT	*result = NULL;
	char *uasource, *defschema, *query_url;
	unsigned long buffersize;

	uasource   = PRESTOCLIENT_SOURCE;
	defschema  = PRESTOCLIENT_DEFAULT_SCHEMA;
	query_url  = PRESTOCLIENT_QUERY_URL;
	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	if (prestoclient && in_sql_statement && strlen(in_sql_statement) > 0)
	{
		// Prepare the result set
		result = new_prestoresult();

		result->client = prestoclient;

		result->write_callback_function = in_write_callback_function;

		result->describe_callback_function = in_describe_callback_function;

		result->client_object = in_client_object;

		result->hcurl = curl_easy_init();

		if (!result->hcurl)
		{
			delete_prestoresult(result);
			return NULL;
		}

		// Reserve memory for curl data buffer
		result->lastresponse = (char*)malloc(buffersize + 1);	//  * sizeof(char) ?
		if (!result->lastresponse)
			exit(1);
		memset(result->lastresponse, 0, buffersize);			//  * sizeof(char) ?
		result->lastresponsebuffersize = buffersize;

		// Add resultset to the client
		register_result(result);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					result->hcurl,
					prestoclient->server,
					&prestoclient->port,
					NULL,
					in_sql_statement,
					prestoclient->catalog,
					in_schema ? in_schema : defschema,
					prestoclient->useragent,
					prestoclient->user,
					&buffersize,
					(void*)result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(result);
		}
	}

	return result;
}

unsigned int prestoclient_getstatus(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return PRESTOCLIENT_STATUS_NONE;

	return result->clientstatus;
}

char* prestoclient_getlastserverstate(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	return (result->laststate ? result->laststate : "");
}

char* prestoclient_getlastservererror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	return result->lasterrormessage;
}

unsigned int prestoclient_getcolumncount(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return false;

	return result->columncount;
}

char* prestoclient_getcolumnname(PRESTOCLIENT_RESULT *result, const unsigned int columnindex)
{
	if (!result)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;

	return result->columns[columnindex]->name;
}

unsigned int prestoclient_getcolumntype(PRESTOCLIENT_RESULT *result, const unsigned int columnindex)
{
	if (!result)
		return PRESTOCLIENT_TYPE_UNDEFINED;

	if (columnindex >= result->columncount)
		return PRESTOCLIENT_TYPE_UNDEFINED;

	return result->columns[columnindex]->type;
}

char* prestoclient_getcolumntypedescription(PRESTOCLIENT_RESULT *result, const unsigned int columnindex)
{
	if (!result)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;

	switch (result->columns[columnindex]->type)
	{
		case PRESTOCLIENT_TYPE_VARCHAR:
		{
			return "PRESTO_VARCHAR";
			break;
		}

		case PRESTOCLIENT_TYPE_BIGINT:
		{
			return "PRESTO_BIGINT";
			break;
		}

		case PRESTOCLIENT_TYPE_BOOLEAN:
		{
			return "PRESTO_BOOLEAN";
			break;
		}

		case PRESTOCLIENT_TYPE_DOUBLE:
		{
			return "PRESTO_DOUBLE";
			break;
		}

		default:
		{
			return "PRESTO_UNDEFINED";
			break;
		}
	}
}

char* prestoclient_getcolumndata(PRESTOCLIENT_RESULT *result, const unsigned int columnindex)
{
	if (!result || !result->columns)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;

	return result->columns[columnindex]->data;
}

int prestoclient_getnullcolumnvalue(PRESTOCLIENT_RESULT *result, const unsigned int columnindex)
{
	if (!result || !result->columns)
		return true;

	if (columnindex >= result->columncount)
		return true;

	return result->columns[columnindex]->dataisnull ? true : false;
}

void prestoclient_cancelquery(PRESTOCLIENT_RESULT *result)
{
	if (result)
		result->cancelquery = true;
}

char* prestoclient_getlastclienterror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	switch (result->errorcode)
	{
		case PRESTOCLIENT_RESULT_OK:
			return NULL;
		case PRESTOCLIENT_RESULT_BAD_REQUEST_DATA:
			return "Not all parameters to start request are available";
		case PRESTOCLIENT_RESULT_SERVER_ERROR:
			return "Server returned error";
		case PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED:
			return "Server is busy";
		case PRESTOCLIENT_RESULT_CURL_ERROR:
			return "CURL error occurred";
		case PRESTOCLIENT_RESULT_PARSE_JSON_ERROR:
			return "Error parsing returned json object";
		default:
			return "Invalid errorcode";
	}
}

char* prestoclient_getlastcurlerror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	if (!result->curl_error_buffer)
		return NULL;

	if (strlen(result->curl_error_buffer) == 0)
		return NULL;

	return (result->curl_error_buffer ? result->curl_error_buffer : "");
}