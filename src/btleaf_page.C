/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"
#include <string.h>
const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key, AttrType key_type, RID dataRid, RID& rid){
	
	KeyDataEntry* recordEntry=new KeyDataEntry;
	Datatype data;
	data.rid=dataRid;
	int* recLen=new int;
	// we combine the key,data pair 
	make_entry(recordEntry,key_type,key,LEAF,data,recLen);
	Status status;
	// we insert the record
	status=SortedPage::insertRecord(key_type,(char*)recordEntry,*recLen,rid);
	if (status!=OK){
		return status;
	}
  	return OK;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(const void *key, AttrType key_type, RID &dataRid){

	RID rid;
	Keytype* curkey=new Keytype;
	Status status;
	status=get_first(rid,curkey,dataRid);
	// we loop to find the required rid
	while (keyCompare(key,curkey,key_type)!=0){
		status=get_next(rid,curkey,dataRid);
		if (status==NOMORERECS){
			return FAIL;
		}
	}
	return DONE;
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid, void *key, RID & dataRid){

	Status status;
	HFPage::firstRecord(rid);
	KeyDataEntry* dataentry;
	char* entryptr;
	int entry_len;
	// we retrieve the pointer to the first record
	status=HFPage::returnRecord(rid,entryptr,entry_len);
	if (status!=OK){
		return status;
	}
	dataentry=(KeyDataEntry*)entryptr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	// we then retrieve the record's key and rid
	get_key_data(keytype,datatype,dataentry,entry_len,LEAF);
	memcpy((char*)key,(char*)keytype,entry_len-sizeof(RID));
	dataRid=datatype->rid;
  	return OK;
}

Status BTLeafPage::get_next (RID& rid, void *key, RID & dataRid){
								 
	RID nextRID;
	Status status;
	status=HFPage::nextRecord(rid,nextRID);
	if (status!=OK){
		return NOMORERECS;
	}
	rid=nextRID;
	char* recPtr;
	int recLen;
	// we retrieve the pointer to the next record
	HFPage::returnRecord(rid,recPtr,recLen);
	KeyDataEntry* nextEntry=(KeyDataEntry*)recPtr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	// we then retrieve the record's key and rid
	get_key_data(keytype,datatype,nextEntry,recLen,LEAF);
	memcpy((char*)key,(char*)keytype,recLen-sizeof(RID));
	dataRid=datatype->rid;
  	return OK;
}
