
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include "storage.h"
#include "utils.h"
#include <termios.h>
#include <unistd.h>

#define ClinF 1
#define nCharac 100

int main(int argc, char** argv) {

	char command[nCharac];
	char hostName[nCharac];
	int port;
	char pass[nCharac];
	char username[nCharac];
	char table[nCharac];
	char key[nCharac];
	char value[nCharac];
	char answer[nCharac];
	char maxNum[nCharac];
	int i = 0;
	int maxKeys;

	void *conn;
	struct storage_record r;

	if (2 == LOGGING){
		char* clientFName;
		clientFName = cSDateTime(ClinF);
		clientF = fopen(clientFName,"w");
	}
	boolean exit = false;
	boolean connect = false;
	boolean auth = false;

	//parsing input
	printf("Welcome to ECE server\nText interface:\n");
	while (exit != true) {
		printf("> ");
		scanf("%s", command);
		if (strcasecmp(command, "help") == 0) {
			printf("----Command List:----\n"
								"1. \"Connect\"\n"
								"2. \"Login\"\n"
								"3. \"Get\"\n"
								"4. \"Set\"\n"
								"5. \"Delete\"\n"
								"6. \"Query\"\n"
								"7. \"Disconnect\"\n"
								"8. \"Exit\"\n"
								" ---------------------\n");
		} else if (strcasecmp(command, "connect") == 0) {

			if (connect == true){
				printf("you are already connected!\n");
			}else{
				printf("Host Name: ");
				scanf("%s", hostName);

				printf("Port: ");
				scanf("%d", &port);

				// connect code
				conn = storage_connect(hostName, port);
				if (!conn) {
					printf("Cannot connect to server @ %s:%d. Error code: %d.\n",
							hostName, port, errno);
					return -1;
				} else {
					// if connection is successful and established
					connect = true;
					printf("Successfully connected to server.\n");
					logger(clientF,"Client sucessfully connected to server.\n",LOGGING);
				}
			}
		} else if (strcasecmp(command, "Login") == 0) {
			if (connect == false) {
				printf("Please connect to server first!\n");
			} else if (auth == true){
				printf("You are already logged in\n");
			} else {
				printf("UserName: ");
				scanf("%s", username);

				printf("Password: ");
				scanf("%s", pass);

				int status = storage_auth(username, pass, conn);
				if (status != 0) {
					printf("storage_auth failed with username '%s' "
							"and password '%s'. " "Error code: %d.\n"
							, username, pass, errno);
					storage_disconnect(conn);
					return status;
				} else {
					printf("Authentication: successful.\n");
					auth = true;
				}
			}
		} else if (strcasecmp(command, "Set") == 0) {
			if (connect == false) {
				printf("Error: Please connect to a server first\n");
			} else if (auth == false) {
				printf("Error: Please login first\n");
			} else {

				printf("Enter Table Name: ");
				scanf("%s", table);
				printf("Enter key: ");
				scanf("%s", key);
				printf("Enter value: ");
				fgets(value, nCharac, stdin);
				fgets(value, nCharac, stdin);
				fflush(stdout);
				fflush(stdin);

				strncpy(r.value, value, sizeof r.value);

				////////////////////////////////////////////////
				//struct timeval tvalBefore, tvalAfter;
				//gettimeofday(&tvalBefore, NULL);

				int status = storage_set(table, key, &r, conn);

				//gettimeofday(&tvalAfter, NULL);
				//printf("Time in microseconds(client side): %ld microseconds\n",
				//		((tvalAfter.tv_sec - tvalBefore.tv_sec)*1000000L + tvalAfter.tv_usec) - tvalBefore.tv_usec);

				////////////////////////////////////////////////


				if (status != 0) {
					printf("storage_set failed. Error code: %d.\n", errno);
					storage_disconnect(conn);
					return status;

				} else {
					printf("storage_set: successful.\n");
				}

			}
		} else if (strcasecmp(command, "Get") == 0) {
			if (connect == false) {
				printf("Error: Please connect to a server first\n");
			} else if (auth == false) {
				printf("Error: Please login first\n");
			} else {
				printf("Enter Table Name: ");
				scanf("%s", table);
				printf("Enter key Value: ");
				scanf("%s", key);

				////////////////////////////////////////////////
				//struct timeval tvalBefore, tvalAfter;
				//gettimeofday(&tvalBefore, NULL);

				int status = storage_get(table, key, &r, conn);

				//gettimeofday(&tvalAfter, NULL);
				//printf("Time in microseconds(client side): %ld microseconds\n",
				//		((tvalAfter.tv_sec - tvalBefore.tv_sec)*1000000L + tvalAfter.tv_usec) - tvalBefore.tv_usec);

				////////////////////////////////////////////////

				if (status != 0) {
					printf("storage_get failed. Error code: %d.\n", errno);
					storage_disconnect(conn);
					return status;
				} else {
					printf("storage_get: the value returned for key '%s' is '%s'.\n",
							key, r.value);

				}
			}

		} else if (strcasecmp(command, "Disconnect") == 0) {

			if (connect==false){
				printf("No connection to terminate!\n");
			}else{
				int status = storage_disconnect(conn);
				if (status != 0) {
					printf("storage_disconnect failed. Error code: %d.\n", errno);
				} else {
					printf("Connection Terminated.\n");
					logger(clientF,"Client susccefully disconnected to server.\n",LOGGING);
					connect = false;
					auth = false;
				}
			}
		}else if(strcasecmp(command, "Delete") == 0){
			if (connect == false) {
				printf("Error: Please connect to a server first\n");
			}
			else if (auth == false) {
				printf("Error: Please login first");
			} else {

				printf("Enter Table Name: ");
				scanf("%s", table);
				printf("Enter key: ");
				scanf("%s", key);

				strncpy(r.value, "", sizeof (r.value));
				int status = storage_set(table, key, &r, conn);
				if (status != 0) {
					printf("storage_delete failed. Error code: %d.\n", errno);
					storage_disconnect(conn);
					return status;

				} else {
					printf("storage_delete: successful.\n");
				}

			}
		}  else if(strcasecmp(command, "Exit") == 0) {
			if (connect == true){
				printf("A connection to the server is detected."
						" Are you sure you want to exit the client? ");
				scanf("%s",answer);
				if (strcasecmp(answer, "yes") == 0){
					exit = true;
				}else if(strcasecmp(answer, "no") == 0){
				}else{
					while(1){
						printf("Please enter 'yes' or 'no': ");
						scanf("%s",answer);
						if (strcasecmp(answer, "yes") == 0
								||strcasecmp(answer, "no") == 0){
							break;
						}
					}
					if (strcasecmp(answer, "yes") == 0){
						exit = true;
					}
				}

			}else{
				exit = true;
			}
		}else if (strcasecmp(command, "Query") == 0) {
			if (connect == false) {
				printf("Error: Please connect to a server first\n");
			} else if (auth == false) {
				printf("Error: Please login first");
			} else {

				int isValid = 0;
				printf("Enter Table Name: ");
				scanf("%s", table);
				printf("Enter predicates: ");
				fgets(key, nCharac, stdin);
				fgets(key, nCharac, stdin);
				int len = strlen(key);
				key[len-1] = 0;
				while (isValid == 0) {
					printf("Enter max number of keys:");
					scanf("%s", maxNum);
					isValid = 1;
					int length = strlen(maxNum);
					for (i = 0; i < length; i++) {
						if (isdigit(maxNum[i]) == 0) {
							printf("invalid number, please re-enter the number.\n");
							isValid = 0;
							break;
						}
					}
				}

				maxKeys = atoi(maxNum);
				char* keys[maxKeys];
				for(i = 0; i < maxKeys; i++){
					keys[i] = (char*)malloc((sizeof(char))* MAX_KEY_LEN);
					*keys[i] = NULL;
				}

				int status = storage_query(table, key, keys, maxKeys, conn);
				if (status == -1) {
					printf("storage_query failed. Error code: %d.\n", errno);
					storage_disconnect(conn);
					return status;

				} else {
					printf("the number of keys found: %d\n", status);
					printf("The keys are: \n");
					for (i = 0; i < maxKeys; i++) {
						if(*keys[i] != NULL){
							printf("%s\n", keys[i]);
						}
					}

				}

			}

		}

		else {
			printf("Error: For a list of commands please type help.\n");
		}
	}
	if (LOGGING == 2){
		fclose (clientF);
	}
	return (EXIT_SUCCESS);
}



