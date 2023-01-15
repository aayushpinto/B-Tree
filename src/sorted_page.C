/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"
#include <cstring>

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type, char * recPtr, int recLen, RID& rid){
	Status status;
	//Insert Record into Page and sort the page by compairing and inserting.
	status = HFPage::insertRecord(recPtr,recLen,rid);
	if (status==OK){
		Keytype* curkey;
		int i;
		for (i =0; i!=slotCnt-1;i++){
			if (slot[i].length==-1){
				continue;
			}
			curkey=(Keytype*)(data+slot[i].offset);
			// we break when we find the the postition the record needs to be inserted at
			if (keyCompare(curkey,recPtr,key_type)>=0){
				break;
			}
		}
		slot_t lastSlot;
		lastSlot=slot[slotCnt-1];

		// this loop will help push the inserted record from the end to higher up the page
		for (int j=slotCnt-1; j!=i; j--){
			slot[j]=slot[j-1];
		}
		slot[i]=lastSlot;
		return OK;
	}
	return status;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid){
	return HFPage::deleteRecord(rid);
}

int SortedPage::numberOfRecords(){
	int numRecs=0;
	for (int i=0; i != slotCnt; ++i){
		if (slot[i].length!=-1){
			numRecs++;
		}
	}
  	return numRecs;
}

Status SortedPage::removeHole(){
	
	//Delettion causes holes so we compress to remove them.
	int slotcnt=slotCnt, i =0, j =0, cnt=0;
	for (i=0; i!=slotCnt;++i){
		if (slot[i].length!=-1){
			cnt++;
		}
		if (slot[i].length==-1){
			for (j=i+1;j!=slotCnt;++j){
				if (slot[j].length!=-1){
					slot[i]=slot[j];
					slot[j].offset=-1;
					slot[j].length=-1;
					cnt++;
					break;
				}
			}
		}
	}
	freeSpace=freeSpace+sizeof(int)*(slotcnt-cnt);
	slotCnt=cnt;
	return OK;
}

