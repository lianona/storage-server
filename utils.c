/**
 * @file
 * @brief This file implements various utility functions that are
 * can be used by the storage server and client library. 
 */

#define _XOPEN_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include "utils.h"
#include <errno.h>

//extern struct config_params params;


int sendall(const int sock, const char *buf, const size_t len) {
    size_t tosend = len;
    while (tosend > 0) {
        ssize_t bytes = send(sock, buf, tosend, 0);
        if (bytes <= 0)
            break; // send() was not successful, so stop.
        tosend -= (size_t) bytes;
        buf += bytes;
    };

    return tosend == 0 ? 0 : -1;
}

/**
 * In order to avoid reading more than a line from the stream,
 * this function only reads one byte at a time.  This is very
 * inefficient, and you are free to optimize it or implement your
 * own function.
 */
int recvline(const int sock, char *buf, const size_t buflen) {
    int status = 0; // Return status.
    size_t bufleft = buflen;

    while (bufleft > 1) {
        // Read one byte from scoket.
        ssize_t bytes = recv(sock, buf, 1, 0);
        if (bytes <= 0) {
            // recv() was not successful, so stop.
            status = -1;
            break;
        } else if (*buf == '\n') {
            // Found end of line, so stop.
            *buf = 0; // Replace end of line with a null terminator.
            status = 0;
            break;
        } else {
            // Keep going.
            bufleft -= 1;
            buf += 1;
        }
    }
    *buf = 0; // add null terminator in case it's not already there.

    return status;
}

/**
 * @brief Parse and process a line in the config file.
 */

// list of possible errors for configuration file!!
/*boolean parHostSet = false;
boolean parServSet = false;
boolean parUserSet = false;
boolean parPassSet = false;
boolean parTableSet = false;*/

/**
 * @brief if there is more than one table, we reassign space for table name.
 *
 */
/*
void checkTableSize(){
	if(*(tableName[numOfTableGl-1])!=NULL ){
		//! copying tableName values into a new array
		char** temp = (char**) realloc(tableName,sizeof(char*)*numOfTableGl*2);
		tableName = temp;
	}
	return;
}
*/

/**
 * @process the configuration file
 * @param line the line read from the configuration file
 * @param params global parameters holding all the configuration information
 */
/*int process_config_line(char *line, struct config_params *params) {
    // Ignore comments.
    if (line[0] == CONFIG_COMMENT_CHAR ||line[0] == '\n')
        return 0;

    //! Extract config parameter name and value.
    char name[MAX_CONFIG_LINE_LEN];
    char value[MAX_CONFIG_LINE_LEN];
    int items = sscanf(line, "%s %s\n", name, value);

    //! Line wasn't as expected.
    if (items != 2)
        return -1;

    //! Process this line.
    if (strcmp(name, "server_host") == 0) {
    	if (parHostSet == true){
    		//!check for duplicated host
    		logger(serverF,"Error: duplicate server_host",LOGGING);
    		return -1;
    	}
    	parHostSet = true;
        strncpy(params->server_host, value, sizeof params->server_host);

    } else if (strcmp(name, "server_port") == 0) {
    	if (parServSet == true){
    		//!check for duplicated port
    	    		logger(serverF,"Error: duplicate server_port",LOGGING);
    	    		return -1;
    	    	}
    	parServSet = true;
        params->server_port = atoi(value);
    } else if (strcmp(name, "username") == 0) {
    	if (parUserSet == true){
    		//!check for duplicated username
    	    	    		logger(serverF,"Error: duplicate username",LOGGING);
    	    	    		return -1;
    	    	    	}
    	    	parUserSet = true;
        strncpy(params->username, value, sizeof params->username);
    } else if (strcmp(name, "password") == 0) {
    	if (parPassSet == true){
    		//!check for duplicated password
    	    	    		logger(serverF,"Error: duplicate password",LOGGING);
    	    	    		return -1;
    	    	    	}
    	    	parPassSet = true;
        strncpy(params->password, value, sizeof params->password);
    }
    else if(strcmp(name, "table") == 0){

    	//!handle table names. there may be more than one table name
    	if (parTableSet ==false ){
    		numOfTableGl = 0;
    		tableName = (char **)malloc(sizeof(char *)*10);
    		parTableSet = true;
    	}else{
    		checkTableSize();
    	}

    	tableName[numOfTableGl] = (char*) malloc(sizeof(char)* MAX_CONFIG_LINE_LEN);
    	strcpy(tableName[numOfTableGl], value);
    	numOfTableGl++;
    }
    else{

    }//! Ignore unknown config parameters.


    return 0;
}*/

/**
 * @open the configuration file and get lines from it
 * @param config_file the name of the configuration file
 * @param global variable holding all the configuration information
 */
/*int read_config(const char *config_file, struct config_params *params) {
    int error_occurred = 0;


    //! Open file for reading.
    FILE *file = fopen(config_file, "r");
    if (file == NULL)
        error_occurred = 1;

    //! Process the config file.
    while (!error_occurred && !feof(file)) {
        //! Read a line from the file.
        char line[MAX_CONFIG_LINE_LEN];
        char *l = fgets(line, sizeof line, file);

        //! Process the line.
        if (l == line)
        	error_occurred = process_config_line(line, params) * -1;
        else if (!feof(file))
            error_occurred = 1;
    }
    if (parHostSet == false){
    	logger(serverF," No Host Specified in configuration file\n",LOGGING);
    	error_occurred = 1;
    }
    if (parServSet == false){
    	logger(serverF," No Server port specified in configuration file\n",LOGGING);
    	error_occurred = 1;
    }if (parUserSet == false){
    	logger(serverF," No User name found in configuration file\n",LOGGING);
    	error_occurred = 1;
    }if (parPassSet == false){
    	logger(serverF," No password set in configuration file\n",LOGGING);
    	error_occurred = 1;
    }if (parTableSet == false){
    	logger(serverF," No Tables found in configuration file\n",LOGGING);
    	error_occurred = 1;
    }if (dupNames() == true){
    	logger(serverF," Multiple tables with the same name in config File",LOGGING);
    	error_occurred = 1;
    }

    return error_occurred ? -1 : 0;
}*/

/**
 *@ check for duplicated names
 */




char* dateT;

/**
 * @check for system time
 */
char *cSDateTime(int clientOrServer) {

    time_t timeN;
    time(&timeN);
    
    if (dateT != 0){
    	free (dateT);
    }
    
    struct tm *currentTime = localtime(&timeN);
    dateT = (char*) malloc(50);

    if (clientOrServer==1){
		sprintf(dateT,"Client-%04d-%02d-%02d-%02d-%02d-%02d.log"
				,currentTime->tm_year + 1900,currentTime->tm_mon + 1
				,currentTime->tm_mday ,currentTime->tm_hour,
				currentTime->tm_min,currentTime->tm_sec);
    }else if(clientOrServer==2){
		sprintf(dateT,"Server-%04d-%02d-%02d-%02d-%02d-%02d.log"
					,currentTime->tm_year + 1900,currentTime->tm_mon + 1
					,currentTime->tm_mday ,currentTime->tm_hour,
					currentTime->tm_min,currentTime->tm_sec);
    }
    return dateT;
    
    
}




void logger(FILE *file, char *message, int logging) {
    //!logging write to a file
    if (logging == 2) {


    fprintf(file,"%s",message);
	fflush(file);


    } else if (logging == 1) {
        printf("%s\n", message);
    } else {
        if (logging != 0) {
            printf("Error LOGGING value!\n");
        }
    }

}
/**
 * @encrypt the password
 */
char *generate_encrypted_password(const char *passwd, const char *salt) {
    if (salt != NULL)
        return crypt(passwd, salt);
    else
        return crypt(passwd, DEFAULT_CRYPT_SALT);
}

int checkvalidusername(char *username) {
	if (strcmp(username, params.username) == 0) {
		return 1;
	}
	else {
		return 0;
	}
}

int checkvalidpassword(char *password) {
	if (strcmp(password, params.password) == 0) {
		return 1;
	}
	else {
		return 0;
	}
}
