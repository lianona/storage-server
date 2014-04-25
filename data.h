/*
 * hash.h
 *
 *  Created on: Feb 15th
 *      Author: Abdelrahman
 */

#ifndef DATABASE_H_
#define DATABASE_H_

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>
#include "storage.h"

//table bst tree
typedef struct TreeDataBase{
	struct tree_DBN* root;
}tree_DB;

typedef struct TreeDataBase tree_DB;

struct tree_DBN{

	char name[MAX_TABLE_LEN];
	int tableSize;

	struct entry_s **tableArray;

	struct tree_DBN* right;
	struct tree_DBN* left;

};
typedef struct tree_DBN tree_Node;

// hash table
//entry:
struct entry_s {
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
	struct entry_s *next;
	int counter;
};

typedef struct entry_s hash_entry;

//hash function:
unsigned long hash(unsigned char *key);

// create database function:
void createDB(int numberOfTables,char ** table_names , tree_DB* dataB);
void insertTableToTree(char* tablNam,tree_Node* root);

int parsing(FILE *filename, FILE *new_filename);

// implement database functions:
hash_entry* get(char* tableName,char* key,tree_DB* dataB);
void set(char*tableName, char*key, char* value, tree_DB* dataB);
//return 0 if it is successful
int delete(char* tableName,char* key,tree_DB* dataB);
int modify(char*tableName, char*key, char* value, tree_DB* dataB);

int query_tree(const char *table, const char *name, const char operator,
	const char* value, char **keys, tree_DB* dataB, int NumOfC);
int query(const char *table, const char *name, const char operator, const char* value, char **keys, int num, tree_DB* dataB, int NumOfC);
char *removeSpace(char *str);

// helper functions
tree_Node* searchTableNames(char* name,tree_DB* dataB);
hash_entry* searchHashTable(hash_entry** tableArray,char*key, int table_size);
tree_Node* searchTableNameRec(char* name,tree_Node* node);


#endif /* HASH_H_ */
