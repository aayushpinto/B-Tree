/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
*/

BTreeFileScan::~BTreeFileScan(){
}

Status BTreeFileScan::get_next(RID & rid, void* keyptr){

	Status status;
	int reclen;
	//If first record flag.
	if (firstFlag){
		firstFlag= false;
		currRID.pageNo=currPageID;
		status=curpage->get_first(currRID,keyptr,rid);
		if (keytype==attrInteger){
			while(status==OK){
				if (inRange(keyptr)){
					return OK;
				}
				status=curpage->get_next(currRID,keyptr,rid);
			}
		}
			
		if (keytype==attrString){
			while(true){
				if (status!=OK){
					return DONE;
				}
				if (status==OK){
					char* recptr;
					curpage->returnRecord(currRID,recptr,reclen);
					Keytype* keytype=new Keytype;
					Datatype* datatype=new Datatype;
					get_key_data(keytype,datatype, (KeyDataEntry*&)recptr,reclen,LEAF);
					strcpy((char*)keyptr,(char *)keytype);
					rid=datatype->rid;
				}
				if (inRange(keyptr)){
					return OK;
				}
				status=curpage->nextRecord(currRID,currRID);
			}
		}
	}
	//If keytype is string.
	if (keytype==attrString){
		status=curpage->nextRecord(currRID,currRID);
		if (status!=OK){
			status=NOMORERECS;
		}
		if (status==OK){
			char* recptr;
			curpage->returnRecord(currRID,recptr,reclen);
			Keytype* keytype=new Keytype;
			Datatype* datatype=new Datatype;
			get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
			strcpy((char*)keyptr,(char *)keytype);
			rid=datatype->rid;
		}
		if (status==OK){
			if (inRange(keyptr)){
				return OK;
			}
			else{
				return DONE;
			}
		}
	}
	//if key type is int.
	if (keytype==attrInteger){
		status=curpage->get_next(currRID,keyptr,rid);
		if (status==OK){
			if (inRange(keyptr)){
				return OK;
			}
			else{
				return DONE;
			}
		}
	}

	if (status==NOMORERECS){
		int nextpageid=curpage->getNextPage();
		if (nextpageid==-1){ //No More Pages
			return DONE;
		}
		else{
			currPageID=nextpageid;
			MINIBASE_BM->pinPage(currPageID,(Page*&)curpage);
			if (keytype==attrInteger){
				status=curpage->get_first(currRID,keyptr,rid);
			}
			else if (keytype==attrString){
				status=curpage->firstRecord(currRID);
				if (status!=OK){
					status=NOMORERECS;
				}
				if (status==OK){
					char* recptr;
					curpage->returnRecord(currRID,recptr,reclen);
					Keytype* keytype=new Keytype;
					Datatype* datatype=new Datatype;
					get_key_data(keytype,datatype,(KeyDataEntry*&)recptr,reclen,LEAF);
					strcpy((char*)keyptr,(char *)keytype);
					rid=datatype->rid;
				}
			}
			if (!inRange(keyptr)){
				return DONE;
			}
			return OK;
		}
	}
	return FAIL;
}

Status BTreeFileScan::delete_current(){
	return curpage->deleteRecord(currRID);
}

int BTreeFileScan::keysize() {
  return keySize;
}

bool BTreeFileScan::inRange(const void* keyptr){
	//check to see if the ptr is between the scan range.
	if(lokey == NULL && hikey==NULL){
		return true;
	}
	else{
		if (lokey==NULL && hikey!=NULL){
			if (keyCompare(keyptr,hikey,keytype)<=0){ 
				return true;
			}
		}
		else if (hikey==NULL && lokey!=NULL){
			if (keyCompare(keyptr,lokey,keytype)>=0) {
				return true;
			}
		}
		else if (hikey!=NULL && lokey!=NULL){
			if (keyCompare(keyptr,hikey,keytype)<=0){
				if (keyCompare(keyptr,lokey,keytype)>=0){
					return true;
				}
				return false;
			}
			return false;
		}
		return false;
	}
	return false;
}