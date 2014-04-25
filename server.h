/*
 * myscanner.h
 *
 *  Created on: 2014-03-11
 *      Author: moham562
 */

#ifndef MYSCANNER_H_
#define MYSCANNER_H_

#include "utils.h"

#define MAX_THREADS 10

#define SERVER_HOST 1
#define SERVER_PORT 2
#define USERNAME 3
#define PASSWORD 4
#define TABLE 5
#define DIRECTORY 6
#define POLICY 7
#define CONCURRENCY 17

#define COLON 8
#define COMMA 9
#define OP 10
#define CP 11

#define IPSTRING 12
#define STRING 13
#define INTEGER 14
#define STORAGESTRING 15
#define TYPE 16

#define UNKNOWN 16


// Storage server constants.
#define MAX_TABLES 100		///< Max tables supported by the server.
#define MAX_RECORDS_PER_TABLE 1000 ///< Max records per table.
#define MAX_TABLE_LEN 20	///< Max characters of a table name.
#define MAX_KEY_LEN 20		///< Max characters of a key name.
#define MAX_CONNECTIONS 10	///< Max simultaneous client connections.

// Extended storage server constants.
#define MAX_COLUMNS_PER_TABLE 10 ///< Max columns per table.
#define MAX_COLNAME_LEN 20

struct tablei{
	int col;
	char* tableName;
	char** colNames;
	int*  maxChar;
}typedef table;

struct confiig{

	int port;
	int concurrency;
	boolean savetodisk;
	int numberOfTables;

	char* host;
	char* username;
	char* password;

	char* path;

	table* tablePointer;

}typedef config;



void pirntconfig(config*x);
void printtable(table*t, int numberOfTables);
boolean isalphastring(char* a);
boolean dupNames(int numberOfTables, table* tableArray);
boolean checkstringsize(char *str, int size);
boolean dupColNames(config* s);
void * t_func(void *arg);

#endif /* MYSCANNER_H_ */
