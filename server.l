%{
#include "server.h"
%}
%%
"server_host"							return SERVER_HOST;
"server_port"							return SERVER_PORT;
"username"								return USERNAME;
"password"								return PASSWORD;
"table"									return TABLE;
"data_directory"						return DIRECTORY;
"storage_policy"						return POLICY;
"concurrency"							return CONCURRENCY;


"in-memory"|"on-disk"					return STORAGESTRING;
"char"|"int"							return TYPE;


										
[\[]			      					return OP;										
[\]]			      					return CP;										
[:]			      						return COLON;										
[,]			      						return COMMA;
([1-9][0-9]*)([.][1-9][0-9]*){3}		return IPSTRING;
([1-9][0-9]*)|[0-9]*					return INTEGER;
										
[a-zA-Z0-9._/]+							return STRING;
[\t\n ]									;
.										return UNKNOWN;


%%
int yywrap(void){
	return 1;
}