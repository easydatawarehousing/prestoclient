/* This file is part of Easy to Oracle - Free Open Source Data Integration
*
* Copyright (C) 2014 Ivo Herweijer
*
* Easy to Oracle is free software: you can redistribute it and/or modify
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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifndef bool
#define bool	signed char
#define true	1
#define false	0
#endif

/*
 * Define a struct to hold the data for a client session. A pointer
 * to this struct will be passed in all callback functions. This
 * enables you to handle multiple queries simultaneously.
 */
typedef struct ST_QUERYDATA
{
	bool			 hdr_printed;
	char 			*cache;
	unsigned int 	 cache_size;
} QUERYDATA;

/*
 * The descibe callback function. This function will be called when the
 * column description data becomes available. You can use it to print header
 * information or examine column type info.
 */
static void describe_callback_function(void *in_querydata, void *in_result)
{
	QUERYDATA			*qdata  = (QUERYDATA*)in_querydata;
	PRESTOCLIENT_RESULT	*result = (PRESTOCLIENT_RESULT*)in_result;
	unsigned int		 i, columncount = prestoclient_getcolumncount(result);

	/*
	 * Print header row
	 */
	if (!qdata->hdr_printed && columncount > 0)
	{
		for (i = 0; i < columncount; i++)
			printf("%s%s", i > 0 ? ";" : "", prestoclient_getcolumnname(result, i) );

		printf("\n");

		qdata->hdr_printed = true;

		/*
		 * You culd also use: prestoclient_getcolumntype
		 * here to determine datatype of each column.
		 */
	}
}

/*
 * The write callback function. This function will be called for every row of
 * query data.
 */
static void write_callback_function(void *in_querydata, void *in_result)
{
	QUERYDATA			*qdata  = (QUERYDATA*)in_querydata;
	PRESTOCLIENT_RESULT	*result = (PRESTOCLIENT_RESULT*)in_result;
	unsigned int		 i, newdatalen, columncount = prestoclient_getcolumncount(result);

	/*
	 * Output one data row
	 */
	for (i = 0; i < columncount; i++)
	{
		/*
		 * Check cache size first
		 */
		newdatalen = strlen(prestoclient_getcolumndata(result, i) );

		if (qdata->cache_size < strlen(qdata->cache) + newdatalen + 3)
		{
			/*
			 * Add memory block of 1 Kb to cache
			 */
			qdata->cache_size = ( (qdata->cache_size + newdatalen + 1027) / 1024) * 1024;
			qdata->cache = (char*)realloc( (char*)qdata->cache, qdata->cache_size);
			if (!qdata->cache)
				exit(1);
		}

		/*
		 * Add field value as string, prestoclient doesn't do any type conversions (yet)
		 */
		strcat(qdata->cache, prestoclient_getcolumndata(result, i) );

		/*
		 * You can use prestoclient_getnullcolumnvalue here
		 * to test if value is NULL in the database
		 */

		/*
		 * Add a field separator
		 */
		if (i < columncount - 1)
			strcat(qdata->cache, ";");
	}

	/*
	 * Print rowdata and a row separator
	 */
	printf("%s\n", qdata->cache);

	/*
	 * Clear cache
	 */
	qdata->cache[0] = 0;
}

/*
 * Min function for a simple commandline application.
 */
int main(int argc, char **argv)
{
	QUERYDATA			*qdata;
	PRESTOCLIENT		*pc;
	PRESTOCLIENT_RESULT	*result;
	bool				 status = false;

	/*
	 * Read commandline parameters
	 */
	if (argc < 3)
	{
		printf("Usage: cprestoclient <servername> <sql-statement>\n");
		printf("Example:\ncprestoclient localhost \"select * from sample_07\"\n");
		exit(1);
	}

	/*
	 * Set up data
	 */
	qdata = (QUERYDATA*)malloc( sizeof(QUERYDATA) );
	qdata->hdr_printed		= false;
	qdata->cache_size		= 1;
	qdata->cache			= (char*)malloc(qdata->cache_size);
	qdata->cache[0]			= 0;

	/*
	 * Initialize prestoclient. We're using default values for everything but the servername
	 */
	pc = prestoclient_init(argv[1], NULL, NULL, NULL, NULL);

	if (!pc)
	{
		printf("Could not initialize prestoclient\n");
	}
	else
	{
		/*
		 * Execute query
		 */
		result = prestoclient_query(pc, argv[2], NULL, &write_callback_function, &describe_callback_function, (void*)qdata);

		if (!result)
		{
			printf("Could not start query '%s' on server '%s'\n", argv[2], argv[1]);
		}
		else
		{
			/*
			 * Print any errormessages
			 */
			if (result)
			{
				status = true;

				/*
				 * Messages from presto server
				 */
				if (prestoclient_getlastservererror(result) )
				{
					printf("%s\n", prestoclient_getlastservererror(result) );
					printf("Serverstate = %s\n", prestoclient_getlastserverstate(result) );
					status = false;
				}

				/*
				 * Messages from prestoclient
				 */
				if (prestoclient_getlastclienterror(result) )
				{
					printf("%s\n", prestoclient_getlastclienterror(result) );
					status = false;
				}

				/*
				 * Messages from curl
				 */
				if (prestoclient_getlastcurlerror(result) )
				{
					printf("%s\n", prestoclient_getlastcurlerror(result) );
				}
			}

			/*
			 * Cleanup
			 */
			prestoclient_close(pc);

			if (qdata->cache)
				free(qdata->cache);

			free(qdata);
		}
	}

	return (status ? 0 : 1);
}
