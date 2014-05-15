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

static JSONPARSER* json_new_parser()
{
	JSONPARSER* json = (JSONPARSER*)malloc( sizeof(JSONPARSER) );

	if (!json)
		exit(1);

	json->state					= JSON_RS_SEARCH_OBJECT;
	json->isbackslash			= false;
	json->readposition			= 0;
	json->skipnextread			= false;
	json->error					= false;
	json->c						= NULL;
	json->control				= JSON_CC_NONE;
	json->clength				= 0;
	json->tagbuffersize			= 1024 * sizeof(char);
	json->tagbuffer				= (char*)malloc(json->tagbuffersize + 1);
	json->tagbufferactualsize	= 0;
	json->tagtype				= JSON_TT_UNKNOWN;

	if (!json->tagbuffer)
		exit(1);

	return json;
}

static JSONLEXER* json_new_lexer()
{
	unsigned int i;
	JSONLEXER* lexer = (JSONLEXER*)malloc( sizeof(JSONLEXER) );

	if (!lexer)
		exit(1);

	lexer->previoustag			= JSON_TT_UNKNOWN;

	lexer->tagordersize			= 10;
	lexer->tagorder				= (enum E_JSON_TAGTYPES*)calloc(lexer->tagordersize, sizeof(enum E_JSON_TAGTYPES) );
	lexer->tagordername			= (char**)calloc(lexer->tagordersize, sizeof(char*) );
	lexer->tagorderactualsize	= 0;

	lexer->column				= 0;
	lexer->error				= false;

	lexer->namesize				= 20 * sizeof(char);
	lexer->name					= (char*)malloc(lexer->namesize + 1);
	lexer->nameactualsize		= 0;

	lexer->valuesize			= 1024 * sizeof(char);
	lexer->value				= (char*)malloc(lexer->valuesize + 1);
	lexer->valueactualsize		= 0;

	if (!lexer->tagorder || ! lexer->name || ! lexer->value)
		exit(1);

	lexer->name[0]				= 0;
	lexer->value[0]				= 0;

	for (i = 0; i < lexer->tagordersize; i++)
	{
		lexer->tagorder[i] = JSON_TT_UNKNOWN;

		lexer->tagordername[i] = (char*)malloc(lexer->namesize + 1);
		if (!lexer->tagordername[i])
			exit(1);
		lexer->tagordername[i][0] = 0;
	}

	return lexer;
}

static void json_add_lexer_tagorder(JSONLEXER* lexer, const enum E_JSON_TAGTYPES newtagorder, const char* newtagordername)
{
	if (!lexer || !newtagordername)
	{
		assert(false);
		return;
	}

	lexer->tagorderactualsize++;

	if (lexer->tagorderactualsize > lexer->tagordersize)
	{
		lexer->tagorder     = (enum E_JSON_TAGTYPES*)realloc( (enum E_JSON_TAGTYPES*)lexer->tagorder, lexer->tagorderactualsize * sizeof(enum E_JSON_TAGTYPES) );
		lexer->tagordername	= (char**)realloc( (char**)lexer->tagordername, lexer->tagorderactualsize * sizeof(char*) );

		lexer->tagordersize = lexer->tagorderactualsize;

		if (!lexer->tagorder || !lexer->tagordername)
			exit(1);

		lexer->tagordername[lexer->tagorderactualsize - 1] = (char*)malloc(20 * sizeof(char) + 1);
		if (!lexer->tagordername[lexer->tagorderactualsize - 1])
			exit(1);
	}

	lexer->tagorder[lexer->tagorderactualsize - 1] = newtagorder;
	strncpy(lexer->tagordername[lexer->tagorderactualsize - 1], newtagordername, 20);
}

static void json_remove_lexer_last_tagorder(JSONLEXER* lexer)
{
	if (!lexer || lexer->tagorderactualsize == 0)
	{
		assert(false);
		return;
	}

	lexer->tagorderactualsize--;
	lexer->tagorder[lexer->tagorderactualsize] = JSON_TT_UNKNOWN;
	lexer->tagordername[lexer->tagorderactualsize][0] = 0;
}

/*static int json_find_tagorder_element(JSONLEXER* lexer, const enum E_JSON_TAGTYPES tagtype, const char* tagname)
{
	int i;

	if (!lexer || !tagname)
	{
		assert(false);
		return -2;
	}

	for (i = (int)lexer->tagorderactualsize - 1; i > 0; i--)
	{
		if (lexer->tagorder[i] == tagtype && strcmp(lexer->tagordername[i], tagname) == 0 )
		{
			return i;
		}
	}

	return -1;
}*/

static bool json_in_array(JSONLEXER* lexer)
{
	if (!lexer || lexer->tagorderactualsize == 0)
	{
		assert(lexer);
		return false;
	}

	if (lexer->tagorder[lexer->tagorderactualsize - 1] == JSON_TT_ARRAY_OPEN)
	{
		return true;
	}

	return false;
}

static bool json_getnextchar(PRESTOCLIENT_RESULT* result)
{
	if (!result)
	{
		assert(false);
		return false;
	}

	if (result->json->skipnextread)
	{
		result->json->skipnextread = false;
	}
	else
	{
		// Is stream still open ?
		if (result->lastresponseactualsize == 0 || result->json->readposition > (result->lastresponseactualsize - 1) || result->lastresponse[result->json->readposition] == 0 )
			return false;

		// Determine length of UTF-8 character
			 if ( (result->lastresponse[result->json->readposition] & 128) ==   0)
			result->json->clength = 1;
		else if ( (result->lastresponse[result->json->readposition] & 240) == 240)
			result->json->clength = 4;
		else if ( (result->lastresponse[result->json->readposition] & 224) == 224)
			result->json->clength = 3;
		else
			result->json->clength = 2;

		// Is there enough data in curl buffer ?
		if (result->json->readposition + result->json->clength > (result->lastresponseactualsize) )
		{
			// We need more data
			return false;
		}

		// Set pointer to current character
		result->json->c = &result->lastresponse[result->json->readposition];

		// Go to next character
		result->json->readposition += result->json->clength;

		// Is new char a special type ?
		switch (result->json->c[0])
		{
			case ' '  :
			case '\t' :
			case '\r' :
			case '\n' :
			case '\f' :		result->json->control = JSON_CC_WS;		break;
			case '{' :		result->json->control = JSON_CC_OO;		break;
			case '}' :		result->json->control = JSON_CC_OC;		break;
			case '[' :		result->json->control = JSON_CC_AO;		break;
			case ']' :		result->json->control = JSON_CC_AC;		break;
			case '\\' :		result->json->control = JSON_CC_BS;		break;
			case '\"' :		result->json->control = JSON_CC_QT;		break;
			case ':' :		result->json->control = JSON_CC_COLON;	break;
			case ',' :		result->json->control = JSON_CC_COMMA;	break;
			default:		result->json->control = JSON_CC_NONE;	break;
		}
	}

	return true;
}

static void json_addtotag(JSONPARSER* json)
{
	if (!json)
		return;

	if (json->tagbufferactualsize + json->clength >= json->tagbuffersize)
	{
		json->tagbuffersize += 1024;
		json->tagbuffer = (char*)realloc( (char*)json->tagbuffer, json->tagbuffersize);
		if (!json->tagbuffer)
			exit(1);
	}

	strncat(json->tagbuffer, &json->c[0], json->clength);
	json->tagbufferactualsize += json->clength;
}

static void json_copytag(char **target, unsigned int *targetsize, unsigned int *targetactualsize, char **tag, unsigned int *tagactualsize, const char *usevalue)
{
	if (usevalue)
	{
		if (*targetsize < strlen(usevalue) )
		{
			*target = (char*)realloc( (char*)*target, strlen(usevalue) * sizeof(char) + 1 );
			*targetsize = strlen(usevalue) * sizeof(char);
		}
	}
	else if (*targetsize < *tagactualsize)
	{
		*target = (char*)realloc( (char*)*target, *tagactualsize * sizeof(char) + 1 );
		*targetsize = *tagactualsize * sizeof(char);
	}

	if (! *target )
		exit(1);

	if (usevalue)
	{
		strcpy(*target, usevalue);
		*targetactualsize = strlen(usevalue);
	}
	else
	{
		strcpy(*target, *tag);
		*targetactualsize = *tagactualsize;
	}

	(*tag)[0] = 0;
	*tagactualsize = 0;
}

// Parser/tokenizer
static bool json_parser(PRESTOCLIENT_RESULT* result)
{
	bool	callback = false;

	result->json->tagtype = JSON_TT_UNKNOWN;

	while (!result->json->error && !callback)
	{
		// Get next character
		if (!json_getnextchar(result) )
			return false;

		// Handle the new character
		switch (result->json->state)
		{
			case JSON_RS_SEARCH_OBJECT:
			{
				switch (result->json->control)
				{
					case JSON_CC_BS:
					{
						result->json->error = true;
						break;
					}

					case JSON_CC_WS:
					{
						// Skip whitespace after control characters
						break;
					}

					case JSON_CC_OO:
					{
						result->json->tagtype = JSON_TT_OBJECT_OPEN;
						callback = true;
						break;
					}

					case JSON_CC_OC:
					{
						result->json->tagtype = JSON_TT_OBJECT_CLOSE;
						callback = true;
						break;
					}

					case JSON_CC_AO:
					{
						result->json->tagtype = JSON_TT_ARRAY_OPEN;
						callback = true;
						break;
					}

					case JSON_CC_AC:
					{
						result->json->tagtype = JSON_TT_ARRAY_CLOSE;
						callback = true;
						break;
					}

					case JSON_CC_QT:
					{
						result->json->state = JSON_RS_READ_STRING;
						break;
					}

					case JSON_CC_COLON:
					{
						result->json->tagtype = JSON_TT_COLON;
						callback = true;
						break;
					}

					case JSON_CC_COMMA:
					{
						result->json->tagtype = JSON_TT_COMMA;
						callback = true;
						break;
					}

					case JSON_CC_NONE:
					{
						result->json->state = JSON_RS_READ_NONSTRING;
						result->json->skipnextread = true;
						break;
					}
				}

				break;
			}

			case JSON_RS_READ_STRING:
			{
				if (result->json->isbackslash)
				{
					// Previous character was a BS
					result->json->isbackslash = false;

					// We're not translating any escape code here, just add to string
					json_addtotag(result->json);
				}
				else if (result->json->control == JSON_CC_BS)
				{
					// Found a backslash
					result->json->isbackslash = true;
					json_addtotag(result->json);
				}
				else if (result->json->control == JSON_CC_QT)
				{
					// Found a non-escaped double quote -> end of string
					result->json->state = JSON_RS_SEARCH_OBJECT;
					result->json->tagtype = JSON_TT_STRING;
					callback = true;
				}
				else
				{
					json_addtotag(result->json);
				}

				break;
			}

			case JSON_RS_READ_NONSTRING:
			{
				if (result->json->control == JSON_CC_AC || result->json->control == JSON_CC_OC || result->json->control == JSON_CC_COMMA)
				{
					result->json->state = JSON_RS_SEARCH_OBJECT;
					result->json->skipnextread = true;
					callback = true;

					if      (strncmp(result->json->tagbuffer, "true",  4) == 0)
						result->json->tagtype = JSON_TT_TRUE;
					else if (strncmp(result->json->tagbuffer, "false", 5) == 0)
						result->json->tagtype = JSON_TT_FALSE;
					else if (strncmp(result->json->tagbuffer, "null",  4) == 0)
						result->json->tagtype = JSON_TT_NULL;
					else
						result->json->tagtype = JSON_TT_NUMBER;
				}
				else
				{
					json_addtotag(result->json);
				}

				break;
			}
		}
	}

	return (!result->json->error);
}

// Forward declaration
static void json_extract_variables(PRESTOCLIENT_RESULT *result);

// Lexical analysis
static bool json_lexer(PRESTOCLIENT_RESULT* result)
{
	switch (result->json->tagtype)
	{
		case JSON_TT_UNKNOWN:
		{
			result->lexer->error = true;
			break;
		}

		case JSON_TT_OBJECT_OPEN:
		case JSON_TT_ARRAY_OPEN:
		{
			json_add_lexer_tagorder(result->lexer, result->json->tagtype, result->lexer->name);
			result->lexer->name[0] = 0;
			break;
		}

		case JSON_TT_COLON:
		case JSON_TT_COMMA:
		{
			// We're only capturing these characters to set: result->lexer->previoustag
			break;
		}

		case JSON_TT_OBJECT_CLOSE:
		case JSON_TT_ARRAY_CLOSE:
		{
			json_remove_lexer_last_tagorder(result->lexer);
			break;
		}

		case JSON_TT_STRING:
		{
			// Todo: replace any \u codes ?

			// Name or value ?
			if (result->lexer->previoustag == JSON_TT_COLON || json_in_array(result->lexer) )
			{
				// Value
				json_copytag(&result->lexer->value, &result->lexer->valuesize, &result->lexer->valueactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, NULL);
				json_extract_variables(result);
			}
			else
				// Name
				json_copytag(&result->lexer->name, &result->lexer->namesize, &result->lexer->nameactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, NULL);

			break;
		}

		case JSON_TT_NUMBER:
		{
			json_copytag(&result->lexer->value, &result->lexer->valuesize, &result->lexer->valueactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, NULL);
			json_extract_variables(result);
			break;
		}

		case JSON_TT_TRUE:
		{
			json_copytag(&result->lexer->value, &result->lexer->valuesize, &result->lexer->valueactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, "1");
			json_extract_variables(result);
			break;
		}

		case JSON_TT_FALSE:
		{
			json_copytag(&result->lexer->value, &result->lexer->valuesize, &result->lexer->valueactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, "0");
			json_extract_variables(result);
			break;
		}

		case JSON_TT_NULL:
		{
			json_copytag(&result->lexer->value, &result->lexer->valuesize, &result->lexer->valueactualsize, &result->json->tagbuffer, &result->json->tagbufferactualsize, "\0");
			json_extract_variables(result);
			break;
		}
	}

	result->lexer->previoustag = result->json->tagtype;

	return (!result->lexer->error);
}

static void json_extract_variables(PRESTOCLIENT_RESULT *result);

bool json_reader(PRESTOCLIENT_RESULT* result)
{
	if (!result->json)
		result->json = json_new_parser();

	if (!result->lexer)
		result->lexer = json_new_lexer();

	while (json_parser(result) && json_lexer(result) )
	{
		// Clear tag buffer after using
		result->json->tagbuffer[0] = 0;
		result->json->tagbufferactualsize = 0;
	}

	// Empty curl buffer
	if (result->json->readposition == (result->lastresponseactualsize) )
	{
		// We can safely empty the entire buffer
		result->lastresponse[0] = 0;
		result->lastresponseactualsize = 0;
		result->json->readposition = 0;
	}
	else
	{
		// we need to preserve the unhandled remainder of the curl buffer (Should be no more than 4 bytes = 1 utf-8 character)
		// This piece of code is not thoroughly tested yet!
		memcpy( (void*)result->lastresponse, (void*)(&result->lastresponse[result->json->readposition]), result->lastresponseactualsize - result->json->readposition + 1);
		result->lastresponseactualsize -= result->json->readposition - 1;
		result->lastresponse[result->lastresponseactualsize] = 0;
		result->json->readposition = 0;
	}

	return (!result->json->error && !result->lexer->error);
}

void json_delete_parser(JSONPARSER* json)
{
	if (!json)
		return;

	if (json->tagbuffer)
		free(json->tagbuffer);

	free(json);
}

void json_delete_lexer(JSONLEXER* lexer)
{
	unsigned int i;

	if (!lexer)
		return;

	if (lexer->tagorder)
		free(lexer->tagorder);

	for (i = 0; i < lexer->tagordersize; i++)
	{
		if (lexer->tagordername[i])
			free(lexer->tagordername[i]);
	}

	if (lexer->tagordername)
        free(lexer->tagordername);

	if (lexer->name)
		free(lexer->name);

	if (lexer->value)
		free(lexer->value);

	free(lexer);
}

void json_reset_lexer(JSONLEXER* lexer)
{
	unsigned int i;

	if (!lexer)
		return;

	lexer->previoustag			= JSON_TT_UNKNOWN;

	lexer->tagorderactualsize	= 0;

	lexer->column				= 0;
	lexer->error				= false;

	lexer->name[0]				= 0;
	lexer->nameactualsize		= 0;

	lexer->value[0]				= 0;
	lexer->valueactualsize		= 0;

	for (i = 0; i < lexer->tagordersize; i++)
	{
		lexer->tagorder[i] = JSON_TT_UNKNOWN;
		lexer->tagordername[i][0] = 0;
	}
}

// This function is specific to prestoclient, not generic json
static void json_extract_variables(PRESTOCLIENT_RESULT *result)
{
	// Extract data
	if (result->lexer->tagorderactualsize > 2 &&
		strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 2], "data") == 0)
	{
		// Print headers
		if (!result->columninfoavailable && result->columncount > 0)
		{
			// If there is a data element, column info must be complete
			result->columninfoavailable = true;

			// Call print header callback function
			if (!result->columninfoprinted)
			{
				result->columninfoprinted = true;

				if (result->describe_callback_function)
					result->describe_callback_function(result->client_object, (void*)result);
			}
		}

		assert(result->columninfoavailable);

		// Determine column
		result->currentdatacolumn++;

		assert(result->currentdatacolumn < (int)result->columncount);

		// Copy value
		if (result->json->tagtype == JSON_TT_NULL)
		{
			result->columns[result->currentdatacolumn]->dataisnull = true;
			result->columns[result->currentdatacolumn]->data[0] = 0;
		}
		else
		{
			result->columns[result->currentdatacolumn]->dataisnull = false;

			if (result->lexer->valueactualsize > result->columns[result->currentdatacolumn]->datasize)
			{
				result->columns[result->currentdatacolumn]->data = (char*)realloc( (char*)result->columns[result->currentdatacolumn]->data, result->lexer->valueactualsize * sizeof(char) + 1);
				result->columns[result->currentdatacolumn]->datasize = result->lexer->valueactualsize;
				if (!result->columns[result->currentdatacolumn]->data)
					exit(1);
			}

			strncpy(result->columns[result->currentdatacolumn]->data, result->lexer->value, result->lexer->valueactualsize + 1);	// +1 to copy null terminator
		}

		// Last column reached ?
		if (result->currentdatacolumn >= (int)result->columncount - 1)
		{
			result->currentdatacolumn = -1;

			// Call rowdata callback function
			result->dataavailable = true;
			if (result->write_callback_function)
				result->write_callback_function(result->client_object, (void*)result);
		}
	}
	//  Get URI's and state
	else if (result->lexer->tagorderactualsize == 1 &&
			 strcmp(result->lexer->name, "infoUri") == 0 )
	{
		alloc_copy(&result->lastinfouri, result->lexer->value);
	}
	else if (result->lexer->tagorderactualsize == 1 &&
			 strcmp(result->lexer->name, "nextUri") == 0 )
	{
		alloc_copy(&result->lastnexturi, result->lexer->value);
	}
	else if (result->lexer->tagorderactualsize == 1 &&
			 strcmp(result->lexer->name, "partialCancelUri") == 0 )
	{
		alloc_copy(&result->lastcanceluri, result->lexer->value);
	}
	else if (result->lexer->tagorderactualsize > 1 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 1], "stats") == 0 &&
			 strcmp(result->lexer->name, "state") == 0 )
	{
		alloc_copy(&result->laststate, result->lexer->value);
	}
	// Get error message
	else if (result->lexer->tagorderactualsize > 2 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 2], "error") == 0 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 1], "failureInfo") == 0 &&
			 strcmp(result->lexer->name, "type") == 0 )
	{
		alloc_add(&result->lasterrormessage, result->lexer->value);
	}
	else if (result->lexer->tagorderactualsize > 2 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 2], "error") == 0 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 1], "failureInfo") == 0 &&
			 strcmp(result->lexer->name, "message") == 0 )
	{
		alloc_add(&result->lasterrormessage, result->lexer->value);
	}
	// Extract column info
	else if (!result->columninfoavailable &&
			 result->lexer->tagorderactualsize > 2 &&
			 strcmp(result->lexer->tagordername[result->lexer->tagorderactualsize - 2], "columns") == 0 )
	{
		if (strcmp(result->lexer->name, "name") == 0 )
		{
			// Found a new column
			result->columncount++;

			// Reserve memory for column info
			if (result->columncount == 1)
				result->columns = (PRESTOCLIENT_FIELD**)calloc(result->columncount, sizeof(PRESTOCLIENT_FIELD*) );
			else
				result->columns = (PRESTOCLIENT_FIELD**)realloc((PRESTOCLIENT_FIELD**)result->columns, result->columncount * sizeof(PRESTOCLIENT_FIELD*) );

			if (!result->columns)
				exit(1);

			result->columns[result->columncount - 1] = new_prestofield();

			// Store columnname
			alloc_copy(&result->columns[result->columncount - 1]->name, result->lexer->value);
		}
		else if (result->columncount > 0 && strcmp(result->lexer->name, "type") == 0 )
		{
			// Store column type
			if      (strcmp(result->lexer->value, "bigint") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_BIGINT;
			else if (strcmp(result->lexer->value, "boolean") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_BOOLEAN;
			else if (strcmp(result->lexer->value, "double") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_DOUBLE;
			else if (strcmp(result->lexer->value, "date") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_DATE;
			else if (strcmp(result->lexer->value, "time") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME;
			else if (strcmp(result->lexer->value, "time with time zone") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE;
			else if (strcmp(result->lexer->value, "timestamp") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP;
			else if (strcmp(result->lexer->value, "timestamp with time zone") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE;
			else if (strcmp(result->lexer->value, "interval year to month") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH;
			else if (strcmp(result->lexer->value, "interval day to second") == 0)
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND;
			else
			{
				assert(strcmp(result->lexer->value, "varchar") == 0);
				result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_VARCHAR;
			}
		}
		//	else
			// An unknown field was encountered -> continue
	}

	// Cleanup
	result->lexer->name[0] = 0;
	result->lexer->value[0] = 0;
}
