/* @file
 * @brief This file contains the implementation of the storage server
 * interface as specified in storage.h.
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include "storage.h"
#include "utils.h"
#include <errno.h>
#include <ctype.h>

extern struct config_params params;

/**
 * @brief This is just a minimal stub implementation.  You should modify it 
 * according to your design.
 */

void* storage_connect(const char *hostname, const int port) {

	if (hostname == NULL) {
		errno = ERR_INVALID_PARAM;
		return NULL;
	} else {

		//! Create a socket.
		int sock = socket(PF_INET, SOCK_STREAM, 0);
		if (sock < 0)
			return NULL;

		//! Get info about the server.
		struct addrinfo serveraddr, *res;
		memset(&serveraddr, 0, sizeof serveraddr);
		serveraddr.ai_family = AF_UNSPEC;
		serveraddr.ai_socktype = SOCK_STREAM;
		char portstr[MAX_PORT_LEN];
		snprintf(portstr, sizeof portstr, "%d", port);
		int status = getaddrinfo(hostname, portstr, &serveraddr, &res);
		if (status != 0)
			return NULL;

		//! Connect to the server.
		status = connect(sock, res->ai_addr, res->ai_addrlen);
		if (status != 0)
			return NULL;

		return (void*) sock;
	}
}

/**
 * @brief This is just a minimal stub implementation.  You should modify it 
 * according to your design.
 */
int storage_auth(const char *username, const char *passwd, void *conn) {

	if (username == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (passwd == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	}

	//! Connection is really just a socket file descriptor.
	int sock = (int) conn;

	//! Send some data.

	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	char *encrypted_passwd = generate_encrypted_password(passwd, NULL );
	snprintf(buf, sizeof buf, "AUTH:%s;%s@\n", username, encrypted_passwd);
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf)
			== 0) {
		char check_input[20] = "Successful";
		if (strcmp(buf, check_input) == 0) {
			logger(clientF,
					"user successfully logged in with username and pass\n",
					LOGGING);
			return 0;
		} else {
			errno = ERR_AUTHENTICATION_FAILED;
			logger(clientF, "authentication failed\n", LOGGING);
			return -1;
		}

	}

	return -1;
}

/**
 * @brief This is just a minimal stub implementation.  You should modify it 
 * according to your design.
 */

int storage_get(const char *table, const char *key,
		struct storage_record *record, void *conn) {
	if (table == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (key == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (record == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	}

	//! check for invalid input
	int size;
	size = strlen(table);
	int i;
	char c;
	for (i = 0; i < size; i++) {
		c = table[i];
		if (isalnum(c) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid table name\n", LOGGING);
			return -1;
		}
	}

	size = strlen(key);
	for (i = 0; i < size; i++) {
		c = key[i];
		if (isalnum(c) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid key\n", LOGGING);
			return -1;
		}
	}

	if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		logger(clientF, "invalid connection\n", LOGGING);
		return -1;
	}

	//! Connection is really just a socket file descriptor.
	int sock = (int) conn;

	//! Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "GET:%s;%s@\n", table, key);

	//! make sure that right value was retrieved from table
	if (sendall(sock, buf, strlen(buf)) == 0
			&& recvline(sock, buf, sizeof(buf)) == 0) {

		if (*buf == NULL) {
			if (recvline(sock, buf, sizeof buf) != 0) {
				return -1;
			}
		}

		char check_input1[40] = "failed with table name";
		char check_input2[20] = "failed with key";
		char check_input3[20] = "not authenticated";

		if (strcmp(buf, check_input2) == 0) {
			//!key doesn't exist case
			errno = ERR_KEY_NOT_FOUND;
			logger(clientF, "key not found in the table\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check_input1) == 0) {
			//!table doesn't exist case
			errno = ERR_TABLE_NOT_FOUND;
			logger(clientF, "table name not found\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check_input3) == 0) {
			errno = ERR_NOT_AUTHENTICATED;
			logger(clientF, "client not authenticated\n", LOGGING);
			return -1;
		} else {
			//!copy the right value to record
			char* data;
			char* counter;
			int   count;

			data = strtok(buf,"~");
			counter = strtok(NULL,"~");
			count = (int) atoi(counter);

			if (count == -1){
				strncpy(record->value, data, sizeof record->value);
			}else if (count > 0){
				strncpy(record->value, data, sizeof record->value);
				*(record->metadata) = count;
			}


			return 0;
		}

	}

	return -1;
}

/**
 * @brief This is just a minimal stub implementation.  You should modify it 
 * according to your design.
 */
int storage_set(const char *table, const char *key,
		struct storage_record *record, void *conn) {

	if (table == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (key == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	} else if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		return -1;
	}

	//!check for invalid input
	int sizeoftable;
	sizeoftable = strlen(table);
	int i;
	char c;
	for (i = 0; i < sizeoftable; i++) {
		c = table[i];
		if (isalnum(c) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid table name\n", LOGGING);
			return -1;
		}
	}

	int sizeofkey;
	sizeofkey = strlen(key);
	for (i = 0; i < sizeofkey; i++) {
		c = key[i];

		if (isalnum(c) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid key\n", LOGGING);
			return -1;
		}
	}

	if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		logger(clientF, "invalid connection\n", LOGGING);
		return -1;
	}

	//int sizeofvalue;
	char value[MAX_VALUE_LEN];
	if (record == NULL) {
		record = malloc(sizeof(struct storage_record));
		*(record->value) = NULL;
	}
	strncpy(value, record->value, sizeof record->value);
	int counter = 0;
	while (value[counter] != NULL && value[counter] != '\n') {
		counter++;
	}

	//char counterstring[800];
	//nprintf(counterstring, 800, "%d", counter);

	for (i = 0; i < counter; i++) {
		c = value[i];

		//if (isalnum(c) == 0 && isspace(c) == 0) {
		if (ispunct(c)) {

			if (c == ' ' || c == ',' || c == '+' || c == '-') {
			} else {
				errno = ERR_INVALID_PARAM;
				logger(clientF, "invalid value\n", LOGGING);
				return -1;
			}
		}
	}

	//! Connection is really just a socket file descriptor.
	int sock = (int) conn;



	//! Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "SET:%s;%s@%d&%s~\n", table, key, *(record->metadata), record->value);
	logger(clientF, buf, LOGGING);
	//!make sure that value was set in table
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf)
			== 0) {

		if (*buf == NULL) {
			if (recvline(sock, buf, sizeof buf) != 0) {
				return -1;
			}
		}

		char check_input[20] = "Successful";
		char check_input3[20] = "not authenticated";
		char check_input2[20] = "failed with key";
		char check_input1[40] = "failed with table name";
		char chack[50] = "failed with value";
		char check[50] =  "Transaction abort";
		char check1[50] = "insert successfully";

		if (strcmp(buf, check_input2) == 0) {
			//!key doesn't exist case
			errno = ERR_KEY_NOT_FOUND;
			logger(clientF, "key not found in the table\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check_input1) == 0) {
			//!table doesn't exist case
			errno = ERR_TABLE_NOT_FOUND;
			logger(clientF, "table name not found\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check_input3) == 0) {
			errno = ERR_NOT_AUTHENTICATED;
			logger(clientF, "client not authenticated\n", LOGGING);
			return -1;
		} else if (strcmp(buf, chack) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid value\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check) == 0) {
			errno = ERR_TRANSACTION_ABORT;
			logger(clientF, "transaction abort\n", LOGGING);
			return -1;
		} else if(strcmp(buf,"insert successfully") == 0){
			memset(record->metadata,0, sizeof record->metadata);
			return 0;
		}
		else {
			//!copy the right value to record
			return 0;
		}

	}
	logger(clientF, "invalid value\n", LOGGING);
	return -1;
}

/**
 * @brief This is just a minimal stub implementation.  You should modify it 
 * according to your design.
 */
int storage_disconnect(void *conn) {

	//! Cleanup
	int sock = (int) conn;
	if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		logger(clientF, "invalid connection\n", LOGGING);
		return -1;
	} else {
		close(sock);
		logger(clientF, "Disconnected from storage\n", LOGGING);
		//! make sure we succeffuly disconnected from storage.

		return 0;
	}
}

int storage_query(const char *table, const char *predicates, char **keys,
		const int max_keys, void *conn) {

	//parsing the predicates
	char* p;
	char* name[MAX_COLUMNS_PER_TABLE];
	char new_preds[MAX_COLUMNS_PER_TABLE * MAX_COLNAME_LEN];
	char *pred[MAX_COLUMNS_PER_TABLE];
	char** colName;
	char operator[MAX_COLUMNS_PER_TABLE];
	char* value[MAX_COLUMNS_PER_TABLE];
	char value_array[MAX_VALUE_LEN];
	int length;
	int TableNum;
	int colNum;
	char c;
	char names[MAX_COLNAME_LEN];
	char* nameP;

	int NumOfP = 0;
	int x;
	char* preds;
	int i = 0;
	int j;
	int q;
	int NumOfOp = 0;
	int isFound = 0;

	if(predicates != NULL){
	strcpy(new_preds, predicates);}
	else{
		errno =ERR_INVALID_PARAM;
		logger(clientF, "predicate name is empty \n", LOGGING);
		return -1;
	}
	for (i = 0; i < MAX_COLUMNS_PER_TABLE; i++) {
		pred[i] = (char*) malloc((sizeof(char)) * MAX_COLNAME_LEN);
		//keys[i] = NULL;
	}

	if (conn == NULL) {
		errno = ERR_INVALID_PARAM;
		logger(clientF, "invalid connection\n", LOGGING);
		return -1;
	}

	if (max_keys == 0) {
		keys = NULL;
		return 0;
	}
	preds = removeSpace(new_preds);
	if(preds != NULL){
	strcpy(new_preds, preds);}else{
		errno = ERR_INVALID_PARAM;
					logger(clientF, "predicate name is just space\n", LOGGING);
					return -1;
	}

	for (p = strtok(new_preds, ","); p != NULL; p = strtok(NULL, ",")) {
		pred[NumOfP] = p;
		NumOfP += 1;
	}
	i = 0;
	for (x = 0; x < NumOfP; x++) {
		p = pred[x];
		NumOfOp = 0;
		j = 0;
		length = strlen(p);
		for (j = 0; j < length; j++) {
			c = p[j];
			if (isalnum(c) != 0 && isspace(c) != 0 && c != "-") {
				if (NumOfOp != 0) {
					errno = ERR_INVALID_PARAM;
					logger(clientF, "multiple operator per predicate\n",
							LOGGING);
					return -1;
				} else {
					if (c != "<" && c != ">" && c != "=") {
						errno = ERR_INVALID_PARAM;
						logger(clientF, "invalid predicate\n", LOGGING);
						return -1;

					} else {
						NumOfOp = 1;
						operator[i] = c;
					}
				}
			}
		}

		name[i] = strtok(p, "<>=");
		if(name[i] != NULL){
		nameP = strdup(name[i]);
		strcpy(names, nameP);
		}
		else{
			errno = ERR_INVALID_PARAM;
			logger(clientF, "invalid name for predicates\n", LOGGING);
			return -1;

		}
		//remove space at the beginning and end of the name
		nameP = removeSpace(names);
		name[i] = strdup(nameP);
		if(name[i] == NULL){
			errno = ERR_INVALID_PARAM;
			logger(clientF, "predicate name not in conf file\n", LOGGING);
						return -1;
		}


		//check if name is valid: not in the list and in the conf file
		for (j = 0; j < i; j++) {

			if (strcmp(name[j], name[i]) == 0) {
				errno = ERR_INVALID_PARAM;
				logger(clientF, "duplicated name for predicates\n", LOGGING);
				return -1;
			}
		}

		isFound = 0;

		i += 1;

	}

	//! Connection is really just a socket file descriptor.
	int sock = (int) conn;

	//! Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "QUERY:%s;%s@%d&\n", table, predicates, max_keys);
	logger(clientF, buf, LOGGING);
	//!make sure that value was set in table
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf)
			== 0) {

		if (*buf == NULL) {
			if (recvline(sock, buf, sizeof buf) != 0) {
				return -1;
			}
		}

		char check_input3[20] = "not authenticated";
		char check_input2[40] = "failed with predicate";
		char check_input1[40] = "failed with table name";
		char check_input4[40] = "nothing found";

		if (strcmp(buf, check_input2) == 0) {
			errno = ERR_INVALID_PARAM;
			logger(clientF, "predicate name not in conf file\n", LOGGING);
			return -1;

		} else if (strcmp(buf, check_input3) == 0) {
			errno = ERR_NOT_AUTHENTICATED;
			logger(clientF, "client not authenticated\n", LOGGING);
			return -1;

		} else if (strcmp(buf, check_input1) == 0) {
			errno = ERR_TABLE_NOT_FOUND;
			logger(clientF, "table name not found\n", LOGGING);
			return -1;
		} else if (strcmp(buf, check_input4) == 0) {
			return 0;
		}

		else {
			i = 0;
			for (p = strtok(buf, ";"); p != NULL; p = strtok(NULL, ";")) {
				keys[i] = p;
				i += 1;
			}
			return i;
		}
	}
	logger(clientF, "invalid value\n", LOGGING);
	return -1;

}
