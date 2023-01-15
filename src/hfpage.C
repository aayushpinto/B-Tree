#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
  nextPage = INVALID_PAGE;
  prevPage = INVALID_PAGE;
  curPage = pageNo;
  slotCnt = 0;

  freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t);
  usedPtr = MAX_SPACE - DPFIXED;
  
  slot[0].length = EMPTY_SLOT;
  slot[0].offset = usedPtr;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;
    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
	return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
	prevPage=pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
	return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
	nextPage=pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
	int spaceNeeded = recLen + sizeof(slot_t);
	if (spaceNeeded > freeSpace){
		return DONE;
	}
	else{
		int offset = usedPtr-recLen;
		slot[slotCnt].offset=offset;
		slot[slotCnt].length=recLen;
		for (int i =0 ; i != recLen; ++i){
			data[offset+i]=recPtr[i];
		}
		usedPtr -= recLen;
		freeSpace -= spaceNeeded;
		rid.pageNo=curPage;
		rid.slotNo=slotCnt;
		slotCnt++;
		return OK;
	}
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid){
	int slotNo = rid.slotNo;
    int i;
    if ((slotNo >= 0) && (slotNo < slotCnt)) {
        int offset = slot[slotNo].offset; 
        int recLen = slot[slotNo].length; 
        char* area = &(data[usedPtr + recLen]);
        int sze = offset - usedPtr;
        memmove(area, &(data[usedPtr]), sze); 
        for (i = 0; i < slotCnt; i++) {
            if ((slot[i].length >= 0)
                   && (slot[i].offset < slot[slotNo].offset))
                slot[i].offset += recLen;
        }
	    usedPtr += recLen;   
        freeSpace += recLen;
        slot[slotNo].length = EMPTY_SLOT;
        slot[slotNo].offset =  0;
        i=slotCnt-1;
        
	while(i>=0){
            if(slot[i].length == EMPTY_SLOT){
                slotCnt--;
                freeSpace += sizeof(slot_t);
            }
            else{
                break;
            }
            i--;
        }
        return OK;
    } 
    else {
        return DONE;
    }
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    int i;
    for (i=0; i < slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT){
            break;
        }
    }
    if ((i == slotCnt) || (slot[i].length == EMPTY_SLOT)) {
        return DONE;
    }
    firstRid.pageNo = curPage;
    firstRid.slotNo = i;
    return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
	int i; 
    if (curRid.slotNo < 0 || curRid.slotNo >= slotCnt){
        return FAIL;
    }
    for (i=curRid.slotNo+1; i < slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT){
            break;
        }
    }
    if ((i >= slotCnt) || (slot[i].length == EMPTY_SLOT)) {
        return DONE;
    }
    nextRid.pageNo = curPage;
    nextRid.slotNo = i;
    return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    int slotNo = rid.slotNo;
    int offset;
    if ((slotNo < slotCnt) && (slot[slotNo].length > 0)) {
        offset = slot[slotNo].offset;
        recLen = slot[slotNo].length;
        memcpy(recPtr, &(data[offset]), recLen);
        return OK;
    } else {
        return DONE;
    }
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    int slotNo = rid.slotNo;
    int offset;
    if ((slotNo < slotCnt) && (slot[slotNo].length > 0)) {
        offset = slot[slotNo].offset;
        recLen = slot[slotNo].length;  
        recPtr = &(data[offset]);      
        return OK;
    }
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    return freeSpace - sizeof(slot_t);
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
   for (int i=0; i < slotCnt; i++){
        if (slot[i].length != EMPTY_SLOT){
            return 0;
        }
    }
    return 1;
}
