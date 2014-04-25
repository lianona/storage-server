/*
 * database.c
 *
 *  Created on: Feb 16, 2014
 *      Author: moham562
 */

#include "data.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#define MAX_CHAR_PER_LINE 2048

//hash function:
unsigned long hash(unsigned char *key) {
	unsigned long hash = 5381;
	int c;

	while (c = *key++)
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

	return hash;
}
// create database function:

void createDB(int numberOfTables, char ** table_names, tree_DB* dataB) {

	//tree_Node* temp = dataB->root;

	// creating root
	dataB->root = (tree_Node*) malloc(sizeof(tree_Node));

	strcpy(dataB->root->name, table_names[0]);
	dataB->root->tableSize = MAX_RECORDS_PER_TABLE;
	dataB->root->tableArray = (struct entry_s**) malloc(
			sizeof(struct entry_s*) * (dataB->root->tableSize));
	int c;
	for (c = 0; c < dataB->root->tableSize; c++) {
		dataB->root->tableArray[c] = NULL;
	}
	dataB->root->right = NULL;
	dataB->root->left = NULL;

	//until here is correct!!
	int counter;
	for (counter = 1; counter < numberOfTables; counter++) {
		insertTableToTree(table_names[counter], dataB->root);
	}

}
//helper set tree node function:
void insertTableToTree(char* tablNam, tree_Node* root) {
	if (strcmp(tablNam, root->name) < 0) {

		if (root->left == NULL ) {

			root->left = (tree_Node*) malloc(sizeof(tree_Node));
			root = root->left;
			strcpy(root->name, tablNam);
			root->tableSize = MAX_RECORDS_PER_TABLE;
			root->tableArray = (struct entry_s**) malloc(
					sizeof(struct entry_s*) * (root->tableSize));
			int c;
			for (c = 0; c < root->tableSize; c++) {
				root->tableArray[c] = NULL;
			}
			root->right = NULL;
			root->left = NULL;

			return;

		} else
			insertTableToTree(tablNam, root->left);

	} else if (strcmp(tablNam, root->name) > 0) {

		if (root->right == NULL ) {

			root->right = (tree_Node*) malloc(sizeof(tree_Node));
			root = root->right;
			strcpy(root->name, tablNam);
			root->tableSize = MAX_RECORDS_PER_TABLE;
			root->tableArray = (struct entry_s**) malloc(
					sizeof(struct entry_s*) * (root->tableSize));
			int c;
			for (c = 0; c < root->tableSize; c++) {
				root->tableArray[c] = NULL;
			}
			root->right = NULL;
			root->left = NULL;

			return;

		} else

			insertTableToTree(tablNam, root->right);

	}

}

// implement database functions:
hash_entry* get(char* tableName, char* key, tree_DB* dataB) {
	// searching treeDB

	tree_Node* tableLocation = searchTableNames(tableName, dataB);
	if (tableLocation == NULL ) {
		return NULL ;
	}

	// searching hash table
	hash_entry* pointerToHashTablePointers =tableLocation->tableArray[hash(key)%tableLocation->tableSize];

	// searching the linked list
	while (pointerToHashTablePointers != NULL ) {
		if (strcmp(pointerToHashTablePointers->key, key) == 0) {
			return pointerToHashTablePointers;
		}
		pointerToHashTablePointers = pointerToHashTablePointers->next;
	}
	return NULL ;

}

//insert
void set(char*tableName, char*key, char* value, tree_DB* dataB) {
	// searching treeDB
	tree_Node* tableLocation = searchTableNames(tableName, dataB);
	// searching hash table
	hash_entry* pointerToHashTablePointers;



	if (tableLocation->tableArray[hash(key)%tableLocation->tableSize] == NULL) {
		pointerToHashTablePointers = (hash_entry*) malloc(sizeof(hash_entry));
		pointerToHashTablePointers->next = NULL;
		strcpy(pointerToHashTablePointers->key, key);
		strcpy(pointerToHashTablePointers->value, value);
		tableLocation->tableArray[hash(key)%tableLocation->tableSize] = pointerToHashTablePointers;

	} else {
		pointerToHashTablePointers =tableLocation->tableArray[hash(key)%tableLocation->tableSize];
		do {
			if (pointerToHashTablePointers->next == NULL ) {
				pointerToHashTablePointers->next = (hash_entry*) malloc(
						sizeof(hash_entry));
				pointerToHashTablePointers = pointerToHashTablePointers->next;
				pointerToHashTablePointers->next = NULL;
				strcpy(pointerToHashTablePointers->key, key);
				strcpy(pointerToHashTablePointers->value, value);
				//tableLocation->tableArray[hash(key)%tableLocation->tableSize] = pointerToHashTablePointers;
				break;
			}
			pointerToHashTablePointers = pointerToHashTablePointers->next;
		} while (pointerToHashTablePointers != NULL );
	}
}

//
int delete(char* tableName, char* key, tree_DB* dataB) {
	// searching treeDB
	tree_Node* tableLocation = searchTableNames(tableName, dataB);
	// searching hash table
	hash_entry* pointerToHashTablePointers =
			tableLocation->tableArray[hash(key)%tableLocation->tableSize];
	hash_entry* temp1;
	hash_entry* temp2;
	if (pointerToHashTablePointers == NULL ) {
		return -1;
	} else if (pointerToHashTablePointers->next == NULL ) {

		if (strcmp(pointerToHashTablePointers->key, key) == 0) {
			free(pointerToHashTablePointers);
			tableLocation->tableArray[hash(key)%tableLocation->tableSize] = NULL;
			return 0;
		} else {
			return -1;
		}

	} else if (strcmp(pointerToHashTablePointers->key, key) == 0) {
		temp1 = pointerToHashTablePointers->next;

		free(pointerToHashTablePointers);
		tableLocation->tableArray[hash(key)%tableLocation->tableSize] = temp1;
		return 0;
	} else {

		temp1 = pointerToHashTablePointers;
		pointerToHashTablePointers = pointerToHashTablePointers->next;

		// searching the linked list
		while (pointerToHashTablePointers != NULL ) {
			if (strcmp(pointerToHashTablePointers->key, key) == 0) {
				temp2 = pointerToHashTablePointers->next;
				temp1->next = temp2;
				free(pointerToHashTablePointers);
				return 0;
			}
			pointerToHashTablePointers = pointerToHashTablePointers->next;
			temp1 = temp1->next;

		}
		return -1;
	}
}
//
int modify(char*tableName, char*key, char* value, tree_DB* dataB) {
	hash_entry* node = get(tableName, key, dataB);

	if (node != NULL ) {
		strcpy(node->value, value);
		return 0;
	} else {
		return -1;

	}
}

// helper functions
tree_Node* searchTableNames(char* name, tree_DB* dataB) {
	tree_Node* temp = dataB->root;

	return searchTableNameRec(name, temp);

}

tree_Node* searchTableNameRec(char* name, tree_Node* node) {
	if (node == NULL ) {
		return NULL ;
	} else if (strcmp(node->name, name) == 0) {
		return node;
	} else if (strcmp(name, node->name) < 0) {
		return searchTableNameRec(name, node->left);
	} else {
		return searchTableNameRec(name, node->right);
	}

}

hash_entry* searchHashTable(hash_entry** tableArray, char*key, int table_size) {
	return tableArray[hash(key) % table_size];
}

int parsing(FILE *file1, FILE *file2) {

	char* city1;

	char c;
	char *population;
	int i = 0, num, size;
	char* temp;

	if (file1 == NULL ) {
		return -1;

	}

	char line[MAX_CHAR_PER_LINE];
	if (fgets(line, MAX_CHAR_PER_LINE, file1) == NULL ) {
	}

	while (fgets(line, MAX_CHAR_PER_LINE, file1) != NULL ) {

		if (isdigit(line[0]) == 0) {
			break;
		}

		//sscanf(line,"%*d,\"%s", city);
		temp = strtok(line, ",");
		city1 = strtok(NULL, ",");
		temp = strtok(NULL, ",");
		temp = strtok(NULL, ",");
		temp = strtok(NULL, ",");
		population = strtok(NULL, ",");
		num = atoi(population);

		char city2[100];
		city2[0] = '\0';

		city1 = strtok(city1, "(");

		for (i = 0; i < strlen(city1); i++) {
			c = city1[i];
			if (isalpha(c)) {
				size = strlen(city2);
				city2[size] = c;
				city2[size + 1] = '\0';

			}

		}

		fprintf(file2, city2);
		fprintf(file2, "   ");
		fprintf(file2, "%d", num);
		fprintf(file2, "\n");
		fflush(file2);

	}

}

int query_tree(const char *table, const char *name, const char operator,
		const char* value, char **keys, tree_DB* dataB, int NumOfC) {

	tree_Node* tableLocation = searchTableNames(table, dataB);
	if (tableLocation == NULL ) {
		return NULL ;
	}

	int i = 0;
	int j = 0;
	int Num; //the number of columns
	char column[MAX_VALUE_LEN + MAX_COLNAME_LEN];
	char *col;
	char colName[MAX_COLNAME_LEN];
	char colValue[MAX_VALUE_LEN];
	int nameFound = 0;
	int NumOfKeys = 0;
	int isFound = 0;
	//	int TableNum = a.numberOfTables;
	//	table dest;
	//	table* array = a.tablePointer;
	//	// searching hash table
	hash_entry** pointerToHashTable = tableLocation->tableArray;
	//
	//	for (j = 0; j < TableNum; j++) {
	//		if (strcmp(array[j].tableName, table) == 0) {
	//			dest = array[j];
	//			isFound = 1;
	//			break;
	//		}
	//	}
	//
	//	if(isFound == 0){
	//		return -1;
	//	}else{
	//		Num = dest.col;
	//	}
	//
	for (i = 0; i < tableLocation->tableSize; i++) {
		hash_entry* pointerToHashTablePointers = pointerToHashTable[i];
		// searching the linked list
		while (pointerToHashTablePointers != NULL ) {
			//parsing the value

			col = strtok(pointerToHashTablePointers->value, ",");
			int len = strlen(col);
			strcpy(column, col);
			col = removeSpace(column);
			strcpy(column, col);
			sscanf(column, "%s %s,", colName, colValue);
			pointerToHashTablePointers->value[len] = ',';
			if (strcmp(name, colName) == 0) {
				nameFound = 1;
			} else {

				for (j = 0; j < NumOfC - 1; j++) {
					col = strtok(NULL, ",");
					len += strlen(col);
					len += 1;
					strcpy(column, col);
					col = removeSpace(column);
					strcpy(column, col);
					sscanf(column, "%s %s", colName, colValue);
					pointerToHashTablePointers->value[len] = ',';
					if (strcmp(name, colName) == 0) {
						nameFound = 1;
						break;
					}
				}
			}
			//compare the value
			if (nameFound == 1) {
				if (operator == '=' && strcmp(colValue, value) == 0) {
					strcpy(keys[NumOfKeys], pointerToHashTablePointers->key);
					NumOfKeys += 1;
				} else if (operator == '>' && atoi(colValue) > atoi(value)) {
					strcpy(keys[NumOfKeys], pointerToHashTablePointers->key);
					NumOfKeys += 1;
				} else if (operator == '<' && atoi(colValue) < atoi(value)) {
					strcpy(keys[NumOfKeys], pointerToHashTablePointers->key);
					NumOfKeys += 1;
				}
			}

			pointerToHashTablePointers = pointerToHashTablePointers->next;
		}

	}

	return NumOfKeys;
}

int query(const char *table, const char *name, const char operator,
		const char* value, char **keys, int num, tree_DB* dataB, int NumOfC) {

	int i;
	hash_entry* entry;
	char column[MAX_VALUE_LEN + MAX_COLNAME_LEN];
	char *col;
	char colName[MAX_COLNAME_LEN];
	char colValue[MAX_VALUE_LEN];
	int nameFound = 0;
	int isFound = 0;
	int j;
	//	int TableNum = a.numberOfTables;
	//	table dest;
	//	table* array = a.tablePointer;
	int NumOfKeys = 0;

	//	for (j = 0; j < TableNum; j++) {
	//		if (strcmp(array[j].tableName, table) == 0) {
	//			dest = array[j];
	//			isFound = 1;
	//			break;
	//		}
	//	}
	//
	//	if (isFound == 0) {
	//		return -1;
	//	} else {
	//		NumOfCol = dest.col;
	//	}

	for (i = 0; i < num; i++) {

		entry = get(table, keys[i], dataB);
		col = strtok(entry->value, ",");
		int len = strlen(col);
		strcpy(column, col);
		col = removeSpace(column);
		strcpy(column, col);
		sscanf(column, "%s %s", colName, colValue);
		entry->value[len] = ',';
		if (strcmp(name, colName) == 0) {
			nameFound = 1;
		} else {

			for (j = 0; j < NumOfC - 1; j++) {
				col = strtok(NULL, ",");
				len += strlen(col);
				len += 1;
				strcpy(column, col);
				col = removeSpace(column);
				strcpy(column, col);
				sscanf(column, "%s %s", colName, colValue);
				entry->value[len] = ',';
				if (strcmp(name, colName) == 0) {
					nameFound = 1;
					break;
				}
			}
		}


		//compare the value
		if (nameFound == 1) {
			if (operator == '=' && strcmp(colValue, value) == 0) {
				strcpy(keys[NumOfKeys], entry->key);
				NumOfKeys += 1;
			} else if (operator == '>' && atoi(colValue) > atoi(value)) {
				strcpy(keys[NumOfKeys], entry->key);
				NumOfKeys += 1;
			} else if (operator == '<' && atoi(colValue) < atoi(value)) {
				strcpy(keys[NumOfKeys], entry->key);
				NumOfKeys += 1;
			}
		}

	}

	return NumOfKeys;

}

//remove the space at the beginning and end of the old string
char *removeSpace(char *str) {
	char *new;

	//remove the space from the beginning of the space
	while (isspace(*str))
		str++;

	//check if the string is all space
	if (*str == 0)
		return str;

	//remove the space at the end of the string
	new = str + strlen(str) - 1;
	while (new > str && isspace(*new))
		new--;

	// Write new null terminator
	*(new + 1) = 0;

	return str;
}

