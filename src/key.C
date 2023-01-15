/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t){
	if (t==attrString){
		char* str1=(char*)key1;
		char* str2=(char*)key2;
		if (strcmp(str1,str2)<0)
			return -1;
		else if (strcmp(str1,str2)>0)
			return 1;
		else if (strcmp(str1,str2)==0)
			return 0;
		else
			return -2;
	}
	else if (t==attrInteger){
		int* int1=(int*)key1;
		int* int2=(int*)key2;
		if (*int1<*int2)
			return -1;
		else if (*int1>*int2)
			return 1;
		else if (*int1==*int2)
			return 0;
		else
			return -2;
	}
	else
		return -2;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target, AttrType key_type, const void *key, nodetype ndtype, Datatype data,int *pentry_len){
	
	if (key_type == attrInteger){
		// we first add the key into our data entry
		memcpy(target, (int *)key, sizeof(int));
		// we then check if our data entry is a data record or not and then fill in accordingly
		if (ndtype == INDEX){
			memcpy(((char *)target + 4), &(data.pageNo), sizeof(int));
			*pentry_len = 2 * sizeof(int);
		}
		else if (ndtype == LEAF){
			memcpy(((char *)target + 4), &(data.rid), 2 * sizeof(int));
			*pentry_len = 3 * sizeof(int);
		}
  	}

	if (key_type == attrString){

		int keyLen = strlen((char*)key);
		// we first add the key into our data entry
		strcpy((char *)target, (char *)key);
		// we then check if our data entry is a data record or not and then fill in accordingly
		if (ndtype == INDEX){
			memcpy(((char *)target + keyLen), &(data.pageNo), sizeof(int));
			*pentry_len = sizeof(int) + keyLen;
		}
		else if (ndtype == LEAF){
			memcpy(((char *)target + keyLen), &(data.rid), 2 * sizeof(int));
			*pentry_len = 2 * sizeof(int) + keyLen;
		}
  	}
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,KeyDataEntry *psource, int entry_len, nodetype ndtype){
	
	if (ndtype == INDEX){	
		int indexKey = entry_len - sizeof(PageId);
		//If size equals int the key type is int.
		if (indexKey == sizeof(int)){
			memcpy(targetkey, (void *)psource, sizeof(int));
			memcpy(targetdata, (int *)psource + 1, sizeof(int));
		}
		//If size does not equals int the key type is string.
		else{
			memcpy(targetkey, (char *)psource, indexKey);
			memcpy(targetdata, ((char *)psource + indexKey), sizeof(int));
		}
  	}
  
  	if (ndtype == LEAF){
		int leafKey = entry_len - sizeof(RID);
		if (leafKey == sizeof(int)){
			memcpy(targetkey, (void *)psource, sizeof(int));
			memcpy(targetdata, ((int *)psource + 1), sizeof(int) * 2);
		}
		else{
			memcpy(targetkey, (char *)psource, leafKey);
			memcpy(targetdata, ((char *)psource + leafKey), sizeof(int) * 2);
		}
  	}

}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type){

	if (key_type==attrString)
		return strlen((char*)key);
		
	return sizeof(int);
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, const nodetype ndtype){

	int length=0;
	if (ndtype==INDEX && key_type==attrInteger)
		length = sizeof(PageId) + sizeof(int);
	else if (ndtype==LEAF && key_type==attrInteger)
		length = sizeof(RID) + sizeof(int);
	else if (ndtype==INDEX && key_type==attrString)
		length = sizeof(PageId) + strlen((char*)key);
	else if (ndtype==LEAF && key_type==attrString)
		length = sizeof(RID) + strlen((char*)key);

 	return length;
}
