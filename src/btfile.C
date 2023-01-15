/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename){
	//Copy filename
	fileName = (char*) malloc(strlen(filename) + 1);
	strcpy(fileName, filename);

	// if it doesn't exist we create a new one, else we work on an existing one
	if (MINIBASE_DB->get_file_entry(filename,headerPageId)!=OK){
		rootPage=new BTIndexPage;
		headerpage=new HeaderPage;
		return;
	}
	else{
		headerpage = new HeaderPage;
		rootPage = new BTIndexPage;
		MINIBASE_BM->pinPage(headerPageId,(Page*&)headerpage);
		MINIBASE_BM->pinPage(headerpage->rootpageid,(Page*&)rootPage);
		rootPageId=headerpage->rootpageid;
		returnStatus=OK;
		return;
	}
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, const AttrType keytype, const int keysize){
	
	// Acceptable keytypes are string and integer. 
	if (keytype==attrString || keytype==attrInteger){
		if (keytype==attrString && keysize!=MAX_KEY_SIZE1){
			returnStatus=DONE;
			return;
		}
		if (keytype==attrInteger && keysize!=sizeof(int)){
			returnStatus=DONE;
			return;
		}
	}
	else{
		returnStatus=DONE;
		return;
	}

	fileName = (char*) malloc(strlen(filename) + 1);
	strcpy(fileName, filename);
	
	PageId headerPageID;
	//Check if file exists if not create a new one.
	if (MINIBASE_DB->get_file_entry(filename,headerPageID)!=OK){

		headerpage=new HeaderPage;
		PageId headerpageid;
		MINIBASE_BM->newPage(headerpageid,(Page*&)headerpage);

		headerpage->type=keytype;
		indexType=keytype;

		rootPage=new BTIndexPage;
		MINIBASE_BM->newPage(rootPageId,(Page*&)rootPage);
		rootPage->init(rootPageId);
		headerpage->rootpageid=rootPageId;
		if (keytype==attrString){
			keySize=MAX_KEY_SIZE1;
		}
		else if (keytype==attrInteger){
			keySize=sizeof(int);
		}
		MINIBASE_DB->add_file_entry(filename,headerpageid);
		headerPageId=headerpageid;
		returnStatus=OK;
		return;
	}
	
	//Get existing details from the file.
	headerpage=new HeaderPage;
	MINIBASE_BM->pinPage(headerPageID,(Page*&)headerpage);
	rootPage=new BTIndexPage;

	PageId rootPage = headerpage->rootpageid;
	MINIBASE_BM->pinPage(rootPage,(Page*&)rootPage);

	rootPageId=headerpage->rootpageid;
	headerPageId=headerPageID;

	returnStatus=OK;
	return;
}

BTreeFile::~BTreeFile (){
}

Status BTreeFile::destroyFile (){
	// Free the Header Page and delete the file entry from database.
	Status status;
	status = MINIBASE_BM->freePage(headerPageId);
	status = MINIBASE_DB->delete_file_entry(fileName);
	return status;
}

Status BTreeFile::insert(const void *key, const RID rid) {
 
	Keytype* insertKey = new Keytype;
	//copy key to insert key
	if (indexType==attrString){
		size_t len = strlen((char*)key)-1;
		memcpy((char*)insertKey,(char*)key,len);
	}
	else{
		insertKey->intkey= *((int*)key);
	}

	Status status;
	// Create a new root page to insert, if root is empty.
	if (rootPage->empty()){
		BTLeafPage* leafpage=new BTLeafPage;
		PageId leafpageid;
		MINIBASE_BM->newPage(leafpageid,(Page*&)leafpage);
		leafpage->init(leafpageid);
		RID currid;
		leafpage->insertRec(insertKey,indexType,rid,currid);
		rootPage->insertKey(insertKey,indexType,leafpageid,currid);
		MINIBASE_BM->unpinPage(leafpageid);
		return OK;
	}

	// Add data to existing page.
	else{
		vector<PageId> listOfPage;
		BTLeafPage* leafpage=new BTLeafPage;
		PageId leafid;
		RID curRID;
		//Find the appropriate leafnode to insert data.
		status=findLeafNode(insertKey,leafid,listOfPage,leafpage);
		PageId leafId=leafid; //leafid is the PageId where data should be inserted.
		RID rid2,datarid2;
		Keytype* firstKey=new Keytype;
		// leafpage->get_first(rid2,firstKey,datarid2);

		// //Iterate until last record. If match exit as it is present.
		// while(status!=NOMORERECS){
		// 	status=leafpage->get_next(rid2,firstKey,datarid2);
		// 	if (keyCompare(firstKey,insertKey,indexType)==0){
		// 		MINIBASE_BM->unpinPage(leafid);
		// 		return OK;
		// 	}
		// }

		BTLeafPage* newleaf=new BTLeafPage;
		PageId newleafid;

		Keytype* newkey=new Keytype;
		Keytype* curkey=new Keytype;

		status=leafpage->insertRec(insertKey,indexType,rid,curRID);
		if (status==FAIL){
			MINIBASE_BM->unpinPage(leafid);
			return FAIL;
		}
		//When the page is full we will have to create a new page.
		if (status==DONE){

			//If status is DONE it means there is no available space so move half records to new page.
			status=halfNode(leafid, leafpage,(void*&)curkey,newleafid,newleaf,(void*&)newkey,insertKey,rid,indexType);
			if (status!=OK){
				MINIBASE_BM->unpinPage(leafid);
				MINIBASE_BM->unpinPage(newleafid);
				return FAIL;
			}
			int index=1;
			//The newly created page.
			PageId pageInsertedTo=newleafid;
			
			Keytype* keyInserted=newkey;
			Keytype* insertkey2=curkey;
			
			PageId insertpageno2=leafid;
			
			while(true){
				
				BTIndexPage* curindex=new BTIndexPage;
				PageId curindexid=listOfPage[index]; //Assign the previous page parent indexid as current.

				if (curindexid==rootPageId){
					status=rootPage->insertKey(keyInserted,indexType,pageInsertedTo,curRID);
					if (status==OK){
						if (rootPage->numberOfRecords()!=0){
							break;
						}
						if (rootPage->numberOfRecords()==0){
							status=rootPage->insertKey(insertkey2,indexType,insertpageno2,curRID);
							if (status==OK){
								break;
							}
						}
					}
				}
				else{
					
					MINIBASE_BM->pinPage(curindexid,(Page*&)curindex);
					status=curindex->insertKey(keyInserted,indexType,pageInsertedTo,curRID); //insert the key to new page.
					if (status==OK){
						MINIBASE_BM->unpinPage(curindexid);
						break;
					}
					//if index page is full.
					else if(status==DONE){
					
						PageId idOfNewPage;
						//Done return split the page
						PageId newPageNo =0;
						
						BTIndexPage* newpage = new BTIndexPage;
						MINIBASE_BM->newPage(idOfNewPage,(Page*&)newpage);
						newpage->init(idOfNewPage);

						BTIndexPage* splitpage=new BTIndexPage;
						MINIBASE_BM->pinPage(curindexid,(Page*&)splitpage);

						int numOfRecords=splitpage->numberOfRecords();

						RID rid;
						Keytype* key=new Keytype;
						PageId pageno;
						//Divide and Add data into respective pages.
						for (int i=0;i!=numOfRecords/2;i++){
							splitpage->get_first(rid,key,pageno);
							splitpage->deleteRecord(rid);
							newpage->insertKey(key,indexType,pageno,rid);
							
						}
						//Get new
						splitpage->get_first(rid,key,pageno);

						RID rid2;

						if (keyCompare(key,newkey,indexType)>0){
							newpage->insertKey(newkey,indexType,newPageNo,rid2);
						}
						else{
							splitpage->insertKey(newkey,indexType,newPageNo,rid2);
						}

						splitpage->get_first(rid,curkey,pageno);
						newpage->get_first(rid,newkey,pageno);

						newpage->setPrevPage(splitpage->getPrevPage());
						newpage->setNextPage(curindexid);

						splitpage->setPrevPage(idOfNewPage);
						if (status!=OK){
							return FAIL;
						}
						index++;
						pageInsertedTo=idOfNewPage;
						keyInserted=newkey;
					}
				}
			}
		}

		RID datarid;
		curkey=new Keytype;

		MINIBASE_BM->pinPage(leafId,(Page*&)leafpage);
		leafpage->get_first(curRID,curkey,datarid);

		KeyDataEntry* entry=new KeyDataEntry;
		Datatype data;

		data.pageNo=leafId;
		
		int* entryLen=new int;
		make_entry(entry,indexType,curkey,INDEX,data,entryLen);

		PageId curpageno=leafId;
		int index=1;
		MINIBASE_BM->unpinPage(leafId);
		
		while(status==OK){

			Keytype* curkey2=new Keytype;
			BTIndexPage* indexpage=new BTIndexPage;
			PageId indexid=listOfPage[index];
			PageId dataPageId;

			MINIBASE_BM->pinPage(indexid,(Page*&)indexpage);
			status=indexpage->get_first(curRID,curkey2,dataPageId);

			while(dataPageId!=curpageno){
				curkey2=new Keytype;
				indexpage->get_next(curRID,curkey2,dataPageId);
			}

			char* recPtr;
			int recLen;
			indexpage->returnRecord(curRID,recPtr,recLen);

			if (indexType==attrInteger){
				memcpy(recPtr,entry,*entryLen);
			}
			if (indexType==attrString){
				indexpage->deleteRecord(curRID);
				status=indexpage->insertKey(curkey,indexType,data.pageNo,curRID);
				if (status!=OK){
					return FAIL;
				}
			}
			entry = new KeyDataEntry;
			data.pageNo = indexid;
			make_entry(entry,indexType,curkey,INDEX,data,entryLen);
			curpageno=listOfPage[index];
			index++;

			MINIBASE_BM->unpinPage(indexid);
			if (index==(int)listOfPage.size()){
				break;
			}

		}
		return OK;
	}
}

Status BTreeFile::Delete(const void *key, const RID rid) {
	
	Status status = DONE;

	//If root is empty return done
	if (rootPage->empty()){
		return DONE;
	}
	else{
		PageId leafid;
		
		vector<PageId> indexlist;
		Keytype* keytype=new Keytype;
		
		memcpy((char*)keytype,(char*)key,sizeof(Keytype));
		
		BTLeafPage* leafpage=new BTLeafPage;

		findLeafNode(keytype,leafid,indexlist,leafpage); 

		RID recrid;
		Keytype* curkey=new Keytype;
		RID datarid;
		Status status;

		status = leafpage->get_first(recrid,curkey,datarid);
		
		//Iterate over page to find the record to delete.
		while(status!=NOMORERECS){
			if (indexType==attrString){
				char* keym=new char[MAX_KEY_SIZE1];
				memcpy(keym,key,strlen((char*)key)-1);
				if (keyCompare(curkey,keym,(AttrType)indexType)==0){
					leafpage->deleteRecord(recrid);
					MINIBASE_BM->unpinPage(leafid);
					return OK;
				}
			}
			else if(indexType==attrInteger){
				if (keyCompare(curkey,key,(AttrType)indexType)==0){
					leafpage->deleteRecord(recrid);
					MINIBASE_BM->unpinPage(leafid);
					return OK;
				}
			}
			status = leafpage->get_next(recrid,curkey,datarid);
		}
	}
	return status;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {

	BTreeFileScan* btscan=new BTreeFileScan;
	btscan->firstFlag = true;
	btscan->keytype=indexType;

	if (indexType==attrInteger){
		btscan->keySize=sizeof(int);
	}
	else if (indexType==attrString){
		btscan->keySize=MAX_KEY_SIZE1;
	}

	if (lo_key!=NULL){
		btscan->lokey=new Keytype;
		if (indexType==attrString){
			memcpy(btscan->lokey,lo_key,strlen((char*)lo_key)-1);
		}
		if (indexType==attrInteger){
			memcpy(btscan->lokey,lo_key,sizeof(int));
		}
	}

	if (hi_key!=NULL){
		btscan->hikey=new Keytype;
		if (indexType==attrString){
			strcpy((char *)btscan->hikey,(char *)hi_key);
		}
		if (indexType==attrInteger){
			memcpy(btscan->hikey,hi_key,sizeof(int));
		}
	}

	PageId leafid;
	
	vector<PageId> indexlist; //store pageids of indexes
	BTLeafPage* leafpage=new BTLeafPage;
	
	Keytype* searchkey=new Keytype;

	if (lo_key==NULL){
		RID rid;
		PageId pageno;
		rootPage->get_first(rid,searchkey,pageno);
	}
	else{
		memcpy(searchkey,lo_key,sizeof(Keytype));
	}
	findLeafNode(searchkey, leafid, indexlist,leafpage);
	btscan->currPageID=leafid;
	btscan->curpage=leafpage;
	btscan->currRID.pageNo=leafid;
	return btscan;
}

int BTreeFile::keysize(){
	return keySize;
}

Status BTreeFile::halfNode(PageId leafid, BTLeafPage* leafpage, void* &curkey, PageId& newleafid,
        		BTLeafPage*& newleaf, void*& newkey, const void* insertKey, RID rid, AttrType indexType){

	MINIBASE_BM->newPage(newleafid,(Page*&)newleaf);
	newleaf->init(newleafid);
	int numOfRecords=(leafpage->numberOfRecords())/2;
	RID rid123, datarid;
	Keytype* key=new Keytype;
	for (int i = 0 ; i != numOfRecords; i++){
		key=new Keytype;
		leafpage->get_first(rid123,key,datarid);
		leafpage->deleteRecord(rid123);
		newleaf->insertRec(key,indexType,datarid,rid123);	
	}
	leafpage->get_first(rid123,key,datarid);
	RID rid2;
	if (keyCompare(key,insertKey,indexType)>0){
		newleaf->insertRec(insertKey,indexType,rid,rid2);
	}
	else{
		leafpage->insertRec(insertKey,indexType,rid,rid2);
	}
	newleaf->get_first(rid123,newkey,datarid);
	leafpage->get_first(rid123,curkey,datarid);
	
	newleaf->setPrevPage(leafpage->getPrevPage());
	newleaf->setNextPage(leafid);
	
	leafpage->setPrevPage(newleafid);
	leafpage->removeHole();

	//Set the links right by linking new page and previous page.
	BTLeafPage* prevPrevPage=new BTLeafPage;
	MINIBASE_BM->pinPage(newleaf->getPrevPage(),(Page*&)prevPrevPage);
	prevPrevPage->setNextPage(newleafid);
	return OK;
}

Status BTreeFile::findLeafNode(const void* key,PageId& leafid, vector<PageId>& visIndex, BTLeafPage*& leafpage){
	
	Status status;
	RID currid;
	PageId curpageid;
	Keytype* curkey=new Keytype;
	BTIndexPage* curpage=rootPage;
	vector<PageId> pageidlist;
	curpage->get_first(currid,curkey,curpageid);
	int index=0;
	pageidlist.push_back(curpageid);

	while(status!=NOMORERECS){
		if (keyCompare(curkey,key,(AttrType)indexType)>0){
			break;
		}
		status=curpage->get_next(currid,curkey,curpageid);
		pageidlist.push_back(curpageid);
		index++;
	}

	index=index-1;
	if (index==-1){
		index=0;
	}
	
	MINIBASE_BM->unpinPage(curpageid);
	curpageid=pageidlist[index];
	MINIBASE_BM->pinPage(curpageid,(Page*&)curpage);
	visIndex.insert(visIndex.begin(),rootPageId);
	if (curpage->get_type()==LEAF){
		leafid=curpageid;
		visIndex.insert(visIndex.begin(),curpageid);
		leafpage=(BTLeafPage*)curpage;
		return OK;
	}
	return status;
}