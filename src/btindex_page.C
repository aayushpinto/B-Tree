/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include <string.h>

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key, AttrType key_type, PageId pageNo, RID& rid){

	KeyDataEntry* keyDataPairInsert=new KeyDataEntry; //Create keyDataPair
	Datatype data;
	data.pageNo=pageNo;
	int* pentry_len=new int;
	make_entry(keyDataPairInsert,key_type,key,INDEX,data,pentry_len);
	// we enter record in respective index page
	SortedPage::insertRecord(key_type,(char*)keyDataPairInsert,*pentry_len,rid);
  	return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid){

	RID ridToDelete;
	Keytype* iteratingKey=new Keytype;
	PageId curpageno;
	get_first(ridToDelete,iteratingKey,curpageno);
	// we find the record to be deleted
	while (keyCompare(key,iteratingKey,key_type)!=0){
		get_next(ridToDelete,iteratingKey,curpageno);
	}
	SortedPage::deleteRecord(ridToDelete);
	return OK;
}

Status BTIndexPage::get_page_no(const void *key, AttrType key_type, PageId & pageNo){

	RID tempRID;
	Keytype* comapairingKey=new Keytype;
	Status status;
	get_first(tempRID,comapairingKey,pageNo);
	// we loop to find the key and retrive the record's pageno
	while (keyCompare(key,comapairingKey,key_type)!=0){
		status=get_next(tempRID,comapairingKey,pageNo);
		if (status==NOMORERECS){
			return FAIL;
		}
	}
	return DONE;
}

Status BTIndexPage::get_first(RID& rid,void *key,PageId & pageNo){

	HFPage::firstRecord(rid);
	KeyDataEntry* firstEntry;
	char* firstEntryPtr;
	int recLen;
	// we retrieve the pointer to the first record
	HFPage::returnRecord(rid,firstEntryPtr,recLen);
	firstEntry=(KeyDataEntry*)firstEntryPtr;
	Keytype* keytype=new Keytype;
	Datatype* datatype=new Datatype;
	// we then retrieve the record's key and pageno
	get_key_data(keytype,datatype,firstEntry,recLen,INDEX);
	strcpy((char*)key,(char*)keytype);
	pageNo=datatype->pageNo;
  	return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo){

	RID nextRID;
	Status status;
	status=HFPage::nextRecord(rid,nextRID);
	if (status==DONE){ 
		return NOMORERECS;
	}
	else{
		rid=nextRID;
		char* recPtr;
		int recLen;
		// we retrieve the pointer to the next record
		if (HFPage::returnRecord(rid,recPtr,recLen)!=OK){
			return FAIL;
		}
		else{
			KeyDataEntry* nextEntryInPage=(KeyDataEntry*)recPtr;
			Keytype* nextKey=new Keytype;
			Datatype* nextData=new Datatype;
			// we then retrieve the record's key and rid
			get_key_data(nextKey,nextData,nextEntryInPage,recLen,INDEX);
			memcpy((char*)key,(char*)nextKey,recLen-sizeof(PageId));
			pageNo=nextData->pageNo;	
		}
	}
	return OK;
}