/**
 * @file
 * @brief This file implements the storage server.
 *
 * The storage server should be named "server" and should take a single
 * command line argument that refers to the configuration file.
 *
 * The storage server should be able to communicate with the client
 * library functions declared in storage.h and implemented in storage.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include "utils.h"
#include "string.h"
#include "data.h"
#include <stdint.h>
#include <errno.h>
#include "server.h"
#include <pthread.h>

#define MAX_KEY_LEN 20 ///< The maximum length of the key.
#define MAX_VALUE_LEN 800 ///< The maximum length of the value
#define MAX_CHAR_PER_LINE 2048 ///< The maximum number of characters allowed per line
#define MAX_LISTENQUEUELEN 20	///< The maximum number of queued connections.
#define servF 2		///< Defines whether its writing to the server file or the client file

struct _ThreadInfo {
	struct sockaddr_in clientaddr;
	socklen_t clientaddrlen;
	int clientsock;
	pthread_t theThread;
};
typedef struct _ThreadInfo *ThreadInfo;

void * threadCallFunction(void *arg);
void releaseThread(ThreadInfo me);
ThreadInfo getThreadInfo(void);

tree_DB directory; ///< Global Variable of a Tree Database directory
tree_DB* directoryPointer = &directory; ///< Global pointer variable to the tree database directory

int status = 0;

#define MAX 200

extern int yylex();
extern char* yytext;



boolean parHostSet = false;
boolean parServSet = false;
boolean parUserSet = false;
boolean parPassSet = false;
boolean parTableSet = false;
boolean parPolicySet = false;
boolean parDIRSet = false;
boolean parConcSet = false;

char* name[] = { NULL, "host", "port", "username", "password", "table",
		"directory", "policy", ":", ",", "[", "]", "ip", "str", "int",
		"storestring", "unkown" };

config a;
char** alltables;

int lineno = 0;
////threads////

/*  Thread buffer, and circular buffer fields */
ThreadInfo runtimeThreads[MAX_THREADS];
unsigned int botRT = 0, topRT = 0;

/* Mutex to guard print statements */
pthread_mutex_t  printMutex    = PTHREAD_MUTEX_INITIALIZER;

/* Mutex to guard condition -- used in getting/returning from thread pool*/
pthread_mutex_t conditionMutex = PTHREAD_MUTEX_INITIALIZER;
/* Condition variable -- used to wait for avaiable threads, and signal when available */
pthread_cond_t  conditionCond  = PTHREAD_COND_INITIALIZER;

/**
 * @brief Process a command from the client.
 *
 * @param sock The socket connected to the client.
 * @param cmd The command received from the client.
 * @return Returns 0 on success, -1 otherwise.
 */
int handle_command(int sock, char *cmd) {
	char* messageO = (char*) malloc(500);
	sprintf(messageO, "Processing command '%s'\n", cmd);
	logger(serverF, messageO, LOGGING);
	free(messageO);

	/**
	 * @parsing the message from the client library.
	 * the message should start with command.
	 * then followed by parameters separated  by space
	 */

	char check_command[10] = "AUTH";
	char check_command2[10] = "GET";
	char check_command3[10] = "SET";
	char check_command4[10] = "QUERY";

	char *command;
	char *tablename;
	char *keyname;
	char *number;
	char *isTrans;

	command = strtok(cmd, ":");
	tablename = strtok(NULL, ";");
	keyname = strtok(NULL, "@");

	number = strtok(NULL,"&");
	isTrans = strtok(NULL, "~");


	//!handle authentication
	if (strcmp(command, check_command) == 0) {
		if (checkvalidusername(tablename) == 1 && checkvalidpassword(keyname)
		== 1) {
			sendall(sock, "Successful", strlen("Successful"));
			sendall(sock, "\n", 1);
			status = 1;
		}
		//!check for invalid username
		else if (checkvalidusername(tablename) == 0) {
			errno = ERR_AUTHENTICATION_FAILED;
			sendall(sock, "Unsuccessful", strlen("Unsuccessful"));
			sendall(sock, "\n", 1);
			status = 0;
		}
		//!check for invalid password
		else if (checkvalidpassword(keyname) == 0) {
			errno = ERR_AUTHENTICATION_FAILED;
			sendall(sock, "Unsuccessful", strlen("Unsuccessful"));
			sendall(sock, "\n", 1);
			status = 0;
		}

	} else if (strcmp(command, check_command2) == 0) {
		//!handle get command
		hash_entry* node;
		char* val;
		if (status == 0) {
			val = "not authenticated";
		} else if (searchTableNames(tablename, directoryPointer) != NULL) {
			//!check if the table exists first
			node = get(tablename, keyname, directoryPointer);
			if (node != NULL) {
				char tempf[100];
				val = strdup(node->value);

				if (a.concurrency == 0){
					sprintf(tempf,"~%d~",-1);
					strcat(val,tempf);
				}else if (a.concurrency == 1){
					sprintf(tempf,"~%d~",node->counter);
					strcat(val,tempf);
				}

			} else {
				//!check if the key exists
				val = "failed with key";
			}
		} else {
			errno = ERR_TABLE_NOT_FOUND;
			logger(clientF, "table name not found\n", LOGGING);
			val = "failed with table name";
		}


		//! For now, just send back the command to the client.
		sendall(sock, val, strlen(val));
		sendall(sock, "\n", 1);

	} else if (strcmp(command, check_command3) == 0) {
		//!handle set command
		char *valuename;
		valuename = isTrans;

		char* val;
		hash_entry* node;
		if (status == 0) {
			val = "not authenticated";
		} else if (searchTableNames(tablename, directoryPointer) != NULL) {
			node = get(tablename, keyname, directoryPointer);
			if (node != NULL) {
				if (valuename == NULL) {
					//!if value is null, the program delete the entry
					delete(tablename, keyname, directoryPointer);
					printf("%s-%s-%s", tablename, keyname, valuename);
					val = "Successful";

				} else {

					///////////////////////////////////////////////////////////////////////////////////
					char * valdup;
					char* temp;
					char* temp2;
					boolean err_occured = false;
					int tableLocationInTableArray = 0;
					while (tableLocationInTableArray < a.numberOfTables) {
						if (strcmp(
								a.tablePointer[tableLocationInTableArray].tableName,
								tablename) == 0) {
							break;
						}
						tableLocationInTableArray++;
					} //error checking that should never happen
					if (tableLocationInTableArray == a.numberOfTables) {
						//! if table is invalid, set failed
						return -273;
					}
					//checking the value with config file!
					valdup = strdup(valuename);
					int z = 0;
					// checking first col! ---------------------------------------------------
					temp = strtok(valdup, " \n");
					if (temp != NULL) {
						// checking col name is correct
						// first delete end white space at end
						int numCar = strlen(temp);
						temp2 = &temp[numCar - 1];
						while (*temp2 == ' ') {
							*temp2 = '\0';
							temp2--;
						} // actual checking
						if (strcmp(
								a.tablePointer[tableLocationInTableArray].colNames[z],
								temp) != 0) {

							err_occured = true;
						}

						int
						mxCar =
								a.tablePointer[tableLocationInTableArray].maxChar[z];
						temp = strtok(NULL, ",\n");
						int numCar2 = strlen(temp);
						temp2 = &temp[numCar2 - 1];
						while (*temp2 == ' ') {
							*temp2 = '\0';
							temp2--;
						}
						// deleting white at beginning
						while (*temp == ' ') {
							temp++;
						} //checking the values
						if (mxCar < 0) {
							//it is an int =)
							char h = temp[0];
							if (h == '+' || h == '-') {

								temp++;

							}
							while (*temp != NULL) {
								if (isdigit(*temp) == 0) {
									err_occured = true;
								}
								temp++;
							}

						} else {
							// it is a  string
							if (checkstringsize(temp, mxCar) == false) {
								err_occured = true;
							}
						}

					} else {
						err_occured = true;
					}
					// end of first check ----------------------------------------------------
					z++;
					while (temp != NULL) {
						//same code as above
						temp = strtok(NULL, " \n");
						if (temp != NULL) {
							// checking col name is correct
							// first delete end white space at end
							int numCar = strlen(temp);
							temp2 = &temp[numCar - 1];
							while (*temp2 == ' ') {
								*temp2 = '\0';
								temp2--;
							} // actual checking
							if (strcmp(
									a.tablePointer[tableLocationInTableArray].colNames[z],
									temp) != 0) {
								err_occured = true;
							}

							int
							mxCar =
									a.tablePointer[tableLocationInTableArray].maxChar[z];
							temp = strtok(NULL, ",\n");
							int numCar2 = strlen(temp);
							temp2 = &temp[numCar2 - 1];
							while (*temp2 == ' ') {
								*temp2 = '\0';
								temp2--;
							}
							// deleting white at beginning
							while (*temp == ' ') {
								temp++;
							} //checking the values
							if (mxCar < 0) {
								//it is an int =)
								char h = temp[0];
								if (h == '+' || h == '-') {

									temp++;

								}
								while (*temp != NULL) {
									if (isdigit(*temp) == 0) {
										err_occured = true;
									}
									temp++;
								}

							} else {
								// it is a  string
								if (checkstringsize(temp, mxCar) == false) {
									err_occured = true;
								}
							}

						} else {
							//err_occured = true;
							break;
						}
						////////end of repe code
						z++;
					}

					if (z != a.tablePointer[tableLocationInTableArray].col
							|| err_occured == true) {
						val = "failed with value";
					} else {

						if (a.concurrency == 0){
							modify(tablename, keyname, valuename, directoryPointer);
							//printf("%s-%s-%s", tablename, keyname, valuename);
							val = "Successful";
						}else{
							int trans = (int) atoi(number);

							if(trans == 0){


								//!if key exists and value is not NULL, the program modify the entry
								modify(tablename, keyname, valuename, directoryPointer);
								node->counter++;
								//printf("%s-%s-%s", tablename, keyname, valuename);
								val = "Successful";
							}else{
								if (trans != node->counter){
									val = "Transaction abort";
								}else{
									modify(tablename, keyname, valuename, directoryPointer);
									//printf("%s-%s-%s", tablename, keyname, valuename);
									val = "Successful";
									node->counter++;
								}

							}
						}
					}
					///////////////////////////////////////////////////////////////////////////////////

				}

			} else {
				//!if key doesn't exist, the program inserts it
				if (valuename == NULL) {
					val = "failed with key";

				} else {
					///////////////////////////////////////////////////////////////////////////////////
					char * valdup;
					char* temp;
					char* temp2;
					boolean err_occured = false;
					int tableLocationInTableArray = 0;
					while (tableLocationInTableArray < a.numberOfTables) {
						if (strcmp(
								a.tablePointer[tableLocationInTableArray].tableName,
								tablename) == 0) {
							break;
						}
						tableLocationInTableArray++;
					} //error checking that should never happen
					if (tableLocationInTableArray == a.numberOfTables) {
						//! if table is invalid, set failed
						return -273;
					}
					//checking the value with config file!
					valdup = strdup(valuename);
					int z = 0;
					// checking first col! ---------------------------------------------------
					temp = strtok(valdup, " \n");
					if (temp != NULL) {
						// checking col name is correct
						// first delete end white space at end
						int numCar = strlen(temp);
						temp2 = &temp[numCar - 1];
						while (*temp2 == ' ') {
							*temp2 = '\0';
							temp2--;
						} // actual checking
						if (strcmp(
								a.tablePointer[tableLocationInTableArray].colNames[z],
								temp) != 0) {
							err_occured = true;
						}

						int
						mxCar =
								a.tablePointer[tableLocationInTableArray].maxChar[z];
						temp = strtok(NULL, ",\n");
						int numCar2 = strlen(temp);
						temp2 = &temp[numCar2 - 1];
						while (*temp2 == ' ') {
							*temp2 = '\0';
							temp2--;
						}
						// deleting white at beginning
						while (*temp == ' ') {
							temp++;
						} //checking the values
						if (mxCar < 0) {
							//it is an int =)
							char h = temp[0];
							if (h == '+' || h == '-') {

								temp++;

							}
							while (*temp != NULL) {
								if (isdigit(*temp) == 0) {
									err_occured = true;
								}
								temp++;
							}

						} else {
							// it is a  string
							if (checkstringsize(temp, mxCar) == false) {
								err_occured = true;
							}
						}

					} else {
						err_occured = true;
					}
					// end of first check ----------------------------------------------------
					z++;
					while (temp != NULL) {
						//same code as above
						temp = strtok(NULL, " \n");
						if (temp != NULL) {
							// checking col name is correct
							// first delete end white space at end
							int numCar = strlen(temp);
							temp2 = &temp[numCar - 1];
							while (*temp2 == ' ') {
								*temp2 = '\0';
								temp2--;
							} // actual checking
							if (strcmp(
									a.tablePointer[tableLocationInTableArray].colNames[z],
									temp) != 0) {
								err_occured = true;
							}

							int
							mxCar =
									a.tablePointer[tableLocationInTableArray].maxChar[z];
							temp = strtok(NULL, ",\n");
							int numCar2 = strlen(temp);
							temp2 = &temp[numCar2 - 1];
							while (*temp2 == ' ') {
								*temp2 = '\0';
								temp2--;
							}
							// deleting white at beginning
							while (*temp == ' ') {
								temp++;
							} //checking the values
							if (mxCar < 0) {
								//it is an int =)
								char h = temp[0];
								if (h == '+' || h == '-') {

									temp++;

								}
								while (*temp != NULL) {
									if (isdigit(*temp) == 0) {
										err_occured = true;
									}
									temp++;
								}

							} else {
								// it is a  string
								if (checkstringsize(temp, mxCar) == false) {
									err_occured = true;
								}
							}

						} else {
							break;
							//err_occured = true;
						}
						////////end of repe code
						z++;
					}

					if (z != a.tablePointer[tableLocationInTableArray].col
							|| err_occured == true) {
						val = "failed with value";
					} else {

						//!if key exists and value is not NULL, the program modify the entry
						set(tablename, keyname, valuename, directoryPointer);
						printf("%s-%s-%s", tablename, keyname, valuename);
						val = "insert successfully";

						hash_entry* te = get(tablename, keyname, directoryPointer);
						te ->counter =1;
					}
					///////////////////////////////////////////////////////////////////////////////////

				}
			}

		} else {
			//! if table is invalid, set failed
			val = "failed with table name";
		}

		//! For now, just send back the command to the client.
		sendall(sock, val, strlen(val));
		sendall(sock, "\n", 1);
	} else if (strcmp(command, check_command4) == 0) {
		//handle query command
		//parse the predicate
		char new_preds[MAX_CONFIG_LINE_LEN];
		char *preds[MAX_COLUMNS_PER_TABLE];
		int j;
		int i;
		int NumOfP = 0;
		char* p;
		char* name;
		char nameArray[MAX_COLNAME_LEN];
		char operator;
		char value_array[MAX_VALUE_LEN];
		char* value;
		char* val;
		int size = directory.root->tableSize;
		int isFirst = 0;
		int numOfKeys;
		int TableNum;
		int q;
		int x;
		int isFound = 0;
		int length;
		char c;
		char** colName;
		char*keys[size];
		char test[1000];
		int isBreak = 0;
		int isError = 0;

		TableNum = a.numberOfTables;
		table* array = a.tablePointer;
		table dest;
		strcpy(new_preds, keyname);

		for (i = 0; i < size; i++) {
			keys[i] = (char*) malloc((sizeof(char)) * MAX_KEY_LEN);
			//keys[i] = NULL;
		}

		for (i = 0; i < MAX_COLUMNS_PER_TABLE; i++) {
			preds[i] = (char*) malloc((sizeof(char)) * MAX_COLUMNS_PER_TABLE);
			//keys[i] = NULL;
		}

		for (j = 0; j < TableNum; j++) {
			if (strcmp(array[j].tableName, tablename) == 0) {
				dest = array[j];
				isFound = 1;
				break;
			}
		}
		if (status == 0) {
			isError = 1;
			val = "not authenticated";
		} else if (isFound == 0) {
			//errno = ERR_TABLE_NOT_FOUND;
			isError = 1;
			logger(serverF, "invalid table name\n", LOGGING);
			val = "failed with table name";
		} else {

			int colNum = dest.col;
			strcpy(new_preds, keyname);
			length = strlen(new_preds);

			for (p = strtok(new_preds, ","); p != NULL; p = strtok(NULL, ",")) {

				preds[NumOfP] = p;
				NumOfP += 1;
			}
			for(x = 0; x < NumOfP; x++){
				p = preds[x];
				j = 0;
				length = strlen(p);
				for (j = 0; j < length; j++) {
					c = p[j];
					if (isalnum(c) == 0 && isspace(c) == 0 && c != '-') {
						operator = c;
					}
				}

				name = strtok(p, "<>=");
				strcpy(nameArray, name);
				name = removeSpace(nameArray);

				//check if name is in the conf file
				for (j = 0; j < colNum; j++) {
					colName = dest.colNames;
					if (strcmp(colName[j], name) == 0) {
						isFound = 1;
						break;
					}
				}

				if (isFound == 0) {

					//errno = ERR_INVALID_PARAM;

					isError = 1;
					val = "failed with predicate";
					break;

				} else {

					value = strtok(NULL, "<>=");
					if (value == NULL) {
						isError = 1;
						val = "failed with predicate";
						break;
					} else {
						strcpy(value_array, value);
						value = removeSpace(value_array);
					}

					if (dest.maxChar[j] == -1) {
						length = strlen(value);
						for (q = 0; q < length; q++) {
							if (isdigit(value[q]) == 0 && value[q] != '-') {
								//errno = ERR_INVALID_PARAM;
								isError = 1;
								val = "failed with predicate";
								logger(serverF, "invalid predicate value\n",
										LOGGING);
								isBreak = 1;
								break;

							}
						}
					} else {
						length = strlen(value);
						for (q = 0; q < length; q++) {

							if (isalnum(value[q]) == 0 && isspace(value[q])
							== 0) {
								isError = 1;
								//errno = ERR_INVALID_PARAM;
								val = "failed with predicate";
								logger(serverF, "invalid predicate value\n",
										LOGGING);
								isBreak = 1;
								break;
							} else {

								if (length >= dest.maxChar[j]) {
									isError = 1;
									//errno = ERR_INVALID_PARAM;
									val = "failed with predicate";
									logger(serverF,
											"invalid predicate value\n",
											LOGGING);
									isBreak = 1;

									break;
								}
							}
						}
					}
					if (isBreak == 1) {
						break;
					}

					if (isFirst == 0) {
						numOfKeys = query_tree(tablename, name, operator,
								value, keys, directoryPointer, colNum);
						isFirst = 1;

					} else {
						numOfKeys = query(tablename, name, operator, value,
								keys, numOfKeys, directoryPointer, colNum);
					}

				}

				i += 1;
				isFound = 0;
			}

			if (numOfKeys > atoi(number) && isError == 0) {
				val = strdup(keys[0]);
				//strcpy(val, keys[0]);
				for (i = 1; i < atoi(number); i++) {
					sprintf(val, "%s;%s", val, keys[i]);
				}
			} else {
				if (numOfKeys < atoi(number) && numOfKeys > 0 && isError == 0) {
					//strcpy(val, keys[0]);
					val = strdup(keys[0]);
					for (i = 1; i < numOfKeys; i++) {
						sprintf(val, "%s;%s", val, keys[i]);
					}
				} else if(isError == 0 && numOfKeys == 0 ){
					val = "nothing found";
				}
			}
		}

		//! For now, just send back the command to the client.
		sendall(sock, val, strlen(val));
		sendall(sock, "\n", 1);
	}

	//free(res);
	return 0;
}

/**
 * @brief Start the storage server.
 *
 * This is the main entry point for the storage server.  It reads the
 * configuration file, starts listening on a port, and proccesses
 * commands from clients.
 */

int main(int argc, char *argv[]) {

	if (2 == LOGGING) {
		char* serverFName;
		serverFName = cSDateTime(servF);
		serverF = fopen(serverFName, "w");
	}

	//! Process command line arguments.
	//! This program expects exactly one argument: the config file name.
	assert(argc > 0);
	if (argc != 2) {
		printf("Usage %s <config_file>\n", argv[0]);
		exit(EXIT_FAILURE);
	}
	char *config_file = argv[1];
	extern FILE * yyin;
	FILE * f;
	f = fopen(argv[1], "r");

	if (!f) {
		return -1;
	}
	yyin = f;

	//! Read the config file.
	int ntoken, vtoken;
	char* temp;
	a.tablePointer = (table*) malloc(sizeof(table) * MAX_TABLES);
	a.numberOfTables = 0;
	ntoken = yylex();
	while (ntoken) {
		lineno++;
		switch (ntoken) {
		case SERVER_HOST:
			//!check for duplicated host
			if (parHostSet == true) {
				logger(serverF, "Error: duplicate server_host\n", LOGGING);
				//printf("duplicate local host\n");
				return -1;
			}
			parHostSet = true;
			vtoken = yylex();
			if (vtoken != STRING && vtoken != IPSTRING) {
				logger(serverF, "Error: Expected a string for host\n", LOGGING);
				//printf("Syntax error in line %d,"
				//		" Expected a string for host but found"
				//		" %s\n", lineno, yytext);
				return -1;
			} else if (vtoken == STRING) {
				if (isalphastring(yytext) == false) {
					logger(serverF, "Error: Expected a alphanumeric string\n",
							LOGGING);
					//printf("Syntax error in line %d,"
					//		" Expected a alphanumeric string for host but found"
					//		" %s\n", lineno, yytext);
					return -1;
				}
			}
			// mallocing string for config//
			a.host = (char*) malloc(sizeof(char) * MAX);
			strcpy(a.host, yytext);
			strcpy(params.server_host, yytext);
			ntoken = yylex();
			break;
		case SERVER_PORT:
			//!check for duplicated port
			if (parServSet == true) {
				logger(serverF, "Error: duplicate server_port", LOGGING);
				//printf("duplicate port\n");
				return -1;
			}
			parServSet = true;

			vtoken = yylex();
			if (vtoken != INTEGER) {
				//printf("Syntax error in line %d,"
				//		" Expected a INTEGER for port but found"
				//		" %s\n", lineno, yytext);
				return -1;
			}
			a.port = (int) atoi(yytext);
			params.server_port = (int) atoi(yytext);
			ntoken = yylex();
			break;
		case CONCURRENCY:
			//!check for duplicated port
			if (parConcSet == true) {
				logger(serverF, "Error: duplicate concurrency", LOGGING);
				//printf("duplicate port\n");
				return -1;
			}
			parConcSet = true;

			vtoken = yylex();
			if (vtoken != INTEGER) {
				logger(serverF, "Error: concurrency expected an integer", LOGGING);
				return -1;
			}
			a.concurrency = (int) atoi(yytext);
			params.concurrency = (int) atoi(yytext);
			if (a.concurrency <0 || a.concurrency>3){
				logger(serverF, "Error: concurrency expected an number between 0 and 4", LOGGING);
				return -1;
			}

			ntoken = yylex();
			break;
		case USERNAME:
			//!check for duplicated username
			if (parUserSet == true) {
				logger(serverF, "Error: duplicate username", LOGGING);
				//					printf("duplicate username\n");
				return -1;
			}
			parUserSet = true;
			vtoken = yylex();
			if (vtoken != STRING) {
				//					printf("Syntax error in line %d,"
				//							" Expected a string for username but found"
				//							" %s\n", lineno, yytext);
				return -1;
			} else if (vtoken == STRING) {
				if (isalphastring(yytext) == false) {
					//						printf("Syntax error in line %d,"
					//								" Expected a alphanumeric string for host but found"
					//								" %s\n", lineno, yytext);
					return -1;
				}
			}
			// mallocing string for config//
			a.username = (char*) malloc(sizeof(char) * MAX);
			strcpy(a.username, yytext);
			strcpy(params.username, yytext);
			ntoken = yylex();
			break;
		case PASSWORD:
			//!check for duplicated pass
			if (parPassSet == true) {
				logger(serverF, "Error: duplicate password", LOGGING);
				//					printf("duplicate pass\n");
				return -1;
			}
			parPassSet = true;
			vtoken = yylex();
			if (vtoken != STRING) {
				//					printf("Syntax error in line %d,"
				//							" Expected a stringpass for pass but found"
				//							" %s\n", lineno, yytext);
				return -1;
			}
			// mallocing string for config//
			a.password = (char*) malloc(sizeof(char) * MAX);
			strcpy(a.password, yytext);
			strcpy(params.password, yytext);
			ntoken = yylex();
			break;
		case DIRECTORY:
			//!check for duplicated dir
			if (parDIRSet == true) {
				logger(serverF, "Error: duplicate sorage_direcory", LOGGING);
				//printf("duplicate dir\n");
				return -1;
			}
			parDIRSet = true;
			vtoken = yylex();
			if (vtoken != STRING) {
				//					printf("Syntax error in line %d,"
				//							" Expected a string but found"
				//							" %s\n", lineno, yytext);
				return -1;
			}
			// mallocing string for config//
			a.path = (char*) malloc(sizeof(char) * MAX);
			strcpy(a.path, yytext);
			strcpy(params.data_directory, yytext);
			ntoken = yylex();
			break;
		case POLICY:
			//!check for duplicated pol
			if (parPolicySet == true) {
				logger(serverF, "Error: duplicate storage_policy", LOGGING);
				//printf("duplicate policy\n");
				return -1;
			}
			parPolicySet = true;
			vtoken = yylex();
			if (vtoken != STORAGESTRING) {
				//					printf("Syntax error in line %d,"
				//							" Expected a storage string but found"
				//							" %s\n", lineno, yytext);
				return -1;
			}
			if (strcmp(yytext, "on-disk") == 0) {
				a.savetodisk = true;
			} else {
				a.savetodisk = false;
			}
			ntoken = yylex();
			break;
		case TABLE:
			parTableSet = true;
			// reading table name
			vtoken = yylex();
			if (vtoken != STRING) {
				//					printf("Syntax error in line %d,"
				//							" Expected a string for tablename but found"
				//							" %s\n", lineno, yytext);
				return -1;
			} else if (vtoken == STRING) {
				if (isalphastring(yytext) == false) {
					//						printf("Syntax error in line %d,"
					//								" Expected a alphanumeric string for host but found"
					//								" %s\n", lineno, yytext);
					return -1;
				}
			} // if table name can only be alphanum

			a.tablePointer[a.numberOfTables].tableName = (char*) malloc(
					sizeof(char) * MAX);
			strcpy(a.tablePointer[a.numberOfTables].tableName, yytext);

			a.tablePointer[a.numberOfTables].col = 0;
			a.tablePointer[a.numberOfTables].colNames = (char**) malloc(
					sizeof(char*) * MAX_COLUMNS_PER_TABLE);
			a.tablePointer[a.numberOfTables].maxChar = (int*) malloc(
					sizeof(int) * MAX_COLUMNS_PER_TABLE);
			do {
				vtoken = yylex();
				if (vtoken != STRING) {
					//						printf("Syntax error in line %d,"
					//								" Expected a string for coloumn but found"
					//								" %s\n", lineno, yytext);
					return -1;
				} else if (vtoken == STRING) {
					if (isalphastring(yytext) == false) {
						//							printf("Syntax error in line %d,"
						//									" Expected a alphanumeric string for host but found"
						//									" %s\n", lineno, yytext);
						return -1;
					}
				}
				a.tablePointer[a.numberOfTables].colNames[a.tablePointer[a.numberOfTables].col]
				                                          = (char*) malloc(sizeof(char) * MAX);
				strcpy(
						a.tablePointer[a.numberOfTables].colNames[a.tablePointer[a.numberOfTables].col],
						yytext);
				vtoken = yylex();
				if (vtoken != COLON) {
					//						printf("Syntax error in line %d,"
					//							" Expected a ':' but found"
					//							" %s\n", lineno, yytext);
					return -1;
				}
				vtoken = yylex();
				if (vtoken != TYPE) {
					//printf("Syntax error in line %d,"
					//" Expected a ':' but found"
					//" %s\n", lineno, yytext);
					return -1;
				} else {
					int x;
					temp = yytext;
					if (strcmp(yytext, "char") == 0) {
						vtoken = yylex();
						if (vtoken != OP) {
							//printf("Syntax error in line %d,"
							//		" Expected a '[' but found"
							//		" %s\n", lineno, yytext);
							return -1;
						}
						vtoken = yylex();
						x = atoi(yytext);
						if (vtoken != INTEGER) {
							//printf("Syntax error in line %d,"
							//		" Expected an integer but found"
							//		" %s\n", lineno, yytext);
							return -1;
						} else if (x <= 0) {
							//printf("Syntax error in line %d,"
							//		" Expected a +ve integer but found"
							//		" %s\n", lineno, yytext);
							return -1;
						}
						a.tablePointer[a.numberOfTables].maxChar[a.tablePointer[a.numberOfTables].col]
						                                         = x;
						vtoken = yylex();
						if (vtoken != CP) {
							//printf("Syntax error in line %d,"
							//		" Expected a ']' but found"
							//		" %s\n", lineno, yytext);
							return -1;
						}

					} else {
						a.tablePointer[a.numberOfTables].maxChar[a.tablePointer[a.numberOfTables].col]
						                                         = -1;
					}
				}

				a.tablePointer[a.numberOfTables].col++;
				vtoken = yylex();
			} while (vtoken == COMMA);

			a.numberOfTables++;
			ntoken = vtoken;
			break;
		default:
			//printf("Syntax error in line %d %s\n", lineno-1, yytext);
			return -1;
			break;

		}
		//printf("%s %s\n", name[ntoken], yytext);
	}
	if (parHostSet == false) {
		logger(serverF, " No Host Specified in configuration file\n", LOGGING);
		//printf(" No Host Specified in configuration file\n");
		return -1;
	}
	if (parServSet == false) {
		logger(serverF, " No Server port specified in configuration file\n",
				LOGGING);
		//printf(" No Server port specified in configuration file\n");
		return -1;
	}
	if (parUserSet == false) {
		logger(serverF, " No User name found in configuration file\n", LOGGING);
		//printf(" No User name found in configuration file\n");
		return -1;
	}
	if (parPassSet == false) {
		logger(serverF, " No password set in configuration file\n", LOGGING);
		//printf(" No password set in configuration file\n");
		return -1;
	}if (parConcSet == false) {
		logger(serverF, " No concurrency parameter set in configuration file\n", LOGGING);
		//printf(" No password set in configuration file\n");
		return -1;
	}
	if ((parDIRSet == false && parPolicySet == true) || (parDIRSet == true
			&& parPolicySet == false)) {
		logger(serverF, " both of policy and directory have to"
				"exist or both of them doesnt exist  file\n", LOGGING);
		//printf(" both of policy and directory have to"
		//		"exist or both of them doesnt exist \n");
		return -1;
	}
	if (parTableSet == false) {
		logger(serverF, " No Tables found in configuration file\n", LOGGING);
		//printf(" No Tables found in configuration file\n");
		return -1;
	}
	if (dupNames(a.numberOfTables, a.tablePointer) == true) {
		logger(serverF, " Multiple tables with the same name in config File\n",
				LOGGING);
		//	printf(" Multiple tables with the same name in config File\n");
		return -1;
	}
	if (dupColNames(&a) == true) {
		logger(serverF, " table with the same col name in config File\n",
				LOGGING);
		//	printf(" Multiple tables with the same name in config File\n");
		return -1;
	}

	//pirntconfig(&a);

	//!struct config_params params;
	//int status = read_config(config_file, &params);
	if (status != 0) {
		printf("Error processing config file.\n");
		exit(EXIT_FAILURE);
	}
	char* messageA = (char*) malloc(500);
	sprintf(messageA, "Server on %s:%d\n", params.server_host,
			params.server_port);

	logger(serverF, messageA, LOGGING);
	free(messageA);

	/**
	 @creating all the data bases!
	 */
	alltables = (char**) malloc(sizeof(char*) * a.numberOfTables);
	int count = 0;
	while (count < a.numberOfTables) {
		alltables[count] = strdup(a.tablePointer[count].tableName);
		count++;
	}

	//!create the binary search tree
	createDB(a.numberOfTables, alltables, directoryPointer);
	/*	int xCounter;
	 //! populate the hash table
	 for (xCounter = 0; xCounter < numOfTableGl; xCounter++) {

	 char* currentTName = tableName[xCounter];

	 char currentTname1[MAX_TABLE_LEN + 4], currentTname2[MAX_TABLE_LEN + 4];

	 if (strcmp(currentTName, "census") == 0) {

	 strcpy(currentTname1, currentTName);
	 strcpy(currentTname2, currentTName);

	 char* fileName1 = strcat(currentTname1, ".csv");
	 char* fileName2 = strcat(currentTname2, ".conf");

	 FILE *file1 = fopen(fileName1, "r");
	 FILE *file2 = fopen(fileName2, "w");

	 //!parse the raw data and write the useful data into a new file
	 parsing(file1, file2);

	 fclose(file1);
	 fclose(file2);

	 FILE *file3 = fopen(fileName2, "r");

	 char line[MAX_CHAR_PER_LINE];

	 char *key;
	 char *value;

	 while (fgets(line, MAX_CHAR_PER_LINE, file3) != NULL) {

	 key = strtok(line, " ");
	 value = strtok(NULL, " ");

	 if (key != NULL && value != NULL) {
	 set(tableName[xCounter], key, value, directoryPointer);
	 }

	 }
	 }
	 }
	//	// teseting
	//	hash_entry* a =  get("census","Winnipeg",directoryPointer);
	//	if(a == NULL){
	//		printf("hello");
	//	}else{
	//		printf("%s",a->key);
	//	    printf("%s",a->value);
	//	}
	//printf("hello");
	//delete("census","Markham",directoryPointer);
	//set("census", "Markham", "222222", directoryPointer);
	//hash_entry* b =  get("census","Markham",directoryPointer);
	//printf("%s",b->key);
	//printf("%s",b->value);("Deleted");
	//}
	//!done uploading
	//! Create a socket.
	 */
	int listensock = socket(PF_INET, SOCK_STREAM, 0);
	if (listensock < 0) {
		printf("Error creating socket.\n");
		exit(EXIT_FAILURE);
	}

	//! Allow listening port to be reused if defunct.
	int yes = 1;
	status = setsockopt(listensock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
	if (status != 0) {
		printf("Error configuring socket.\n");
		exit(EXIT_FAILURE);
	}

	//! Bind it to the listening port.
	struct sockaddr_in listenaddr;
	memset(&listenaddr, 0, sizeof listenaddr);
	listenaddr.sin_family = AF_INET;
	listenaddr.sin_port = htons(params.server_port);
	inet_pton(AF_INET, params.server_host, &(listenaddr.sin_addr)); // bind to local IP address
	status
	= bind(listensock, (struct sockaddr*) &listenaddr,
			sizeof listenaddr);
	if (status != 0) {
		printf("Error binding socket.\n");
		exit(EXIT_FAILURE);
	}

	//! Listen for connections.
	status = listen(listensock, MAX_LISTENQUEUELEN);
	if (status != 0) {
		printf("Error listening on socket.\n");
		exit(EXIT_FAILURE);
	}

	// implementing multiple clients
	if (a.concurrency == 0){


		//! Listen loop.
		int wait_for_connections = 1;
		while (wait_for_connections) {
			//! Wait for a connection.
			struct sockaddr_in clientaddr;
			socklen_t clientaddrlen = sizeof clientaddr;
			int clientsock = accept(listensock, (struct sockaddr*) &clientaddr,
					&clientaddrlen);
			if (clientsock < 0) {
				printf("Error accepting a connection.\n");
				exit(EXIT_FAILURE);
			}

			char* messageB = (char*) malloc(5000);
			sprintf(messageB, "Got a connection from %s:%d.\n", inet_ntoa(
					clientaddr.sin_addr), clientaddr.sin_port);
			logger(serverF, messageB, LOGGING);
			free(messageB);

			//! Get commands from client.
			int wait_for_commands = 1;
			do {
				//! Read a line from the client.
				char cmd[MAX_CMD_LEN];
				int status = recvline(clientsock, cmd, MAX_CMD_LEN);
				if (status != 0) {
					//! Either an error occurred or the client closed the connection.
					wait_for_commands = 0;
				} else {
					//! Handle the command from the client.
					int status = handle_command(clientsock, cmd);
					if (status != 0)
						wait_for_commands = 0; // Oops.  An error occured.
				}
			} while (wait_for_commands);

			//! Close the connection with the client.
			close(clientsock);

			char* messageC = (char*) malloc(500);
			sprintf(messageC, "Closed connection from %s:%d.\n", inet_ntoa(
					clientaddr.sin_addr), clientaddr.sin_port);
			logger(serverF, messageC, LOGGING);
			free(messageC);

		}

	}else if(a.concurrency == 1){
		int i = 0;
		/* First, we allocate the thread pool */
		for (i = 0; i!=MAX_THREADS; ++i)
			runtimeThreads[i] = malloc( sizeof( struct _ThreadInfo ) );

		while (1) {
			ThreadInfo tiInfo = getThreadInfo();
			tiInfo->clientaddrlen = sizeof(struct sockaddr_in);
			tiInfo->clientsock = accept(listensock, (struct sockaddr*)&tiInfo->clientaddr, &tiInfo->clientaddrlen);
			if (tiInfo->clientsock <0) {
				pthread_mutex_lock( &printMutex );
				printf("ERROR in connecting to %s:%d.\n",
						inet_ntoa(tiInfo->clientaddr.sin_addr), tiInfo->clientaddr.sin_port);
				pthread_mutex_unlock( &printMutex );
			} else {
				pthread_create( &tiInfo->theThread, NULL, threadCallFunction, tiInfo );
			}
		}

		/* At the end, wait until all connections close */
		for (i=topRT; i!=botRT; i = (i+1)%MAX_THREADS)
			pthread_join(runtimeThreads[i]->theThread, 0 );

		/* Deallocate all the resources */
		for (i=0; i!=MAX_THREADS; i++)
			free( runtimeThreads[i] );

	}
	//! Stop listening for connections.
	close(listensock);
	fclose(serverF);
	return EXIT_SUCCESS;
}
void pirntconfig(config*x) {
	printf("host: %s #tables %d password: %s path: %s"
			" port: %d user: %s \n", x->host, x->numberOfTables, x->password,
			x->path, x->port, x->username);
	printtable(x->tablePointer, x->numberOfTables);
}
void printtable(table*t, int numberOfTables) {
	int x = 0;
	int y = 0;
	while (x < numberOfTables) {
		printf("table Name:%s number of cols:%d\n", t[x].tableName, t[x].col);
		int columns = t[x].col;
		y = 0;
		while (y < columns) {
			printf("col#:%d colname:%s", y, t[x].colNames[y]);
			printf(" maxchar:%d \n", t[x].maxChar[y]);
			y++;
		}
		x++;
		printf("---------------------------\n");
	}

}
boolean isalphastring(char* a) {
	int x = 0;
	while (a[x] != NULL) {
		if (!isalnum(a[x])) {
			return false;
		}
		x++;
	}
	return true;
}
boolean dupNames(int numberOfTables, table* tableArray) {
	int i, j;
	for (i = 0; i < numberOfTables; i++) {
		for (j = 0; j < numberOfTables; j++) {
			if (i == j) {

			} else {
				if (strcmp(tableArray[i].tableName, tableArray[j].tableName)
						== 0) {
					return true;
				}
			}
		}
	}
	return false;
}
boolean dupColNames(config* s) {
	int numberOfTables = a.numberOfTables;

	int i, j, k;
	// looping through all tables
	for (i = 0;i<numberOfTables;i++) {
		for (j = 0; j < a.tablePointer[i].col; j++) {
			for (k = 0; k < a.tablePointer[i].col; k++) {
				if (j == k) {

				} else {
					if (strcmp(a.tablePointer[i].colNames[k], a.tablePointer[i].colNames[j])
							== 0) {
						return true;
					}
				}
			}
		}
	}
	return false;
}
boolean checkstringsize(char *str, int size) {
	int sizeofstring = 0;
	int counter = 0;
	char c;
	while (str[sizeofstring] != NULL) {
		sizeofstring++;
	}

	while (str[counter] != NULL) {
		c = str[counter];
		if (isalpha(c) == 0 || sizeofstring > size - 1) {
			return false;
		}
		counter++;
	}
	return true;
}

////////////////////////thread functions/////////////////////////////

/* This function receives a string from clients and echoes it back --
   the thread is released when the thread is finished */
void * threadCallFunction(void *arg) {
	ThreadInfo tiInfo = (ThreadInfo)arg;

	/////////////////////////////
	//! Get commands from client.
	int wait_for_commands = 1;
	do {
		//! Read a line from the client.
		char cmd[MAX_CMD_LEN];
		int status = recvline( tiInfo->clientsock, cmd, MAX_CMD_LEN);
		if (status != 0) {
			//! Either an error occurred or the client closed the connection.
			wait_for_commands = 0;
		} else {
			//! Handle the command from the client.
			int status = handle_command( tiInfo->clientsock, cmd);
			if (status != 0)
				wait_for_commands = 0; // Oops.  An error occured.
		}
	} while (wait_for_commands);
	////////////////////////////


	if (close(tiInfo->clientsock)<0) {
		pthread_mutex_lock( &printMutex );
		printf("ERROR in closing socket to %s:%d.\n",
				inet_ntoa(tiInfo->clientaddr.sin_addr), tiInfo->clientaddr.sin_port);
		pthread_mutex_unlock( &printMutex );
	}
	releaseThread( tiInfo );
	return NULL;
}
ThreadInfo getThreadInfo(void) {
	ThreadInfo currThreadInfo = NULL;

	/* Wait as long as there are no more threads in the buffer */
	pthread_mutex_lock( &conditionMutex );
	while (((botRT+1)%MAX_THREADS)==topRT)
		pthread_cond_wait(&conditionCond,&conditionMutex);

	/* At this point, there is at least one thread available for us. We
take it, and update the circular buffer */
	currThreadInfo = runtimeThreads[botRT];
	botRT = (botRT + 1)%MAX_THREADS;

	/* Release the mutex, so other clients can add threads back */
	pthread_mutex_unlock( &conditionMutex );

	return currThreadInfo;
}

/* Function called when thread is about to finish -- unless it is
called, the ThreadInfo assigned to it is lost */
void releaseThread(ThreadInfo me) {
	pthread_mutex_lock( &conditionMutex );
	assert( botRT!=topRT );

	runtimeThreads[topRT] = me;
	topRT = (topRT + 1)%MAX_THREADS;

	/* tell getThreadInfo a new thread is available */
	pthread_cond_signal( &conditionCond );

	/* Release the mutex, so other clients can get new threads */
	pthread_mutex_unlock( &conditionMutex );
}


