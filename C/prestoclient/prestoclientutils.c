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

#ifdef _WIN32
#include <windows.h>
#include <Lmcons.h>

// returnvalue must be freed by caller
char* get_username()
{
	DWORD namelength = UNLEN + 1;
	CHAR *username = (CHAR*)calloc(UNLEN + 1, sizeof(CHAR) );

	if (!username)
		exit(1);

	GetUserNameA( (CHAR*)username, &namelength);

	return (char*)username;
}

void util_sleep(const int sleeptime_msec)
{
	Sleep(sleeptime_msec);
}
#else
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// returnvalue must be freed by caller
char* get_username()
{
	char *username = (char*)calloc(strlen(getenv("USER") ) + 1, sizeof(char) );

	if (!username)
		exit(1);

	strcpy(username, getenv("USER") );

	return username;
}

void util_sleep(const int sleeptime_msec)
{
	sleep(sleeptime_msec / 1000);
}
#endif
