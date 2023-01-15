/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "buf.h"

static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************
BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  numBuffers=numbuf;
  hateList = new int[numBuffers];
  frameDesc = new FrameDesc[numBuffers];
  hashTable = new HashTable[HTSIZE];
  bufPool = new Page[numBuffers]; 
  for (int i = 0; i < numBuffers; i++) 
    hateList[i]=-1;
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  for (int i = 0; i < numBuffers; i++) 
    if (frameDesc[i].dirty) 
      flushPage(frameDesc[i].pageNo);
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {

  Page* pg = new Page;
  if (MINIBASE_DB->read_page(PageId_in_a_DB, pg) != OK) return FAIL;
  
  int i=0;
  while (i!=numBuffers){
    if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == PageId_in_a_DB){
      frameDesc[hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo].pinCount++;
      page = &bufPool[hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo];
      return OK; 
    }
    i++;
  }

  if (getNumUnpinnedBuffers()==0) return FAIL;

  int validcount=0;
  
  for (int i = 0; i < numBuffers; i++) if (frameDesc[i].pageNo != INVALID_PAGE) validcount++;
  
  if (validcount==numBuffers){
   
    int toReplace, i = numBuffers-1;
    while (i >= 0){
      if (hateList[i] != -1 && frameDesc[hateList[i]].pinCount == 0){
        toReplace=hateList[i];
        break;
      }
      i--;
    }

    if (frameDesc[toReplace].dirty) flushPage(frameDesc[toReplace].pageNo);

    i = 0;
    while (i < numBuffers){
      if (hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].pageNo == frameDesc[toReplace].pageNo) {
        hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].pageNo=-1;
        hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].frameNo=-1;
        break;
      }
      i++;
    }

    i = 0;
    while (i < numBuffers){
      if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == -1){
        hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo = PageId_in_a_DB;
        hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo = toReplace;
        break;
      }
      i++;
    }
    page = &bufPool[toReplace];

    if (!emptyPage) MINIBASE_DB -> read_page(PageId_in_a_DB, &bufPool[toReplace]);

    frameDesc[toReplace].pageNo=PageId_in_a_DB;
    frameDesc[toReplace].pinCount=1;
    frameDesc[toReplace].dirty=FALSE;
    frameDesc[toReplace].hate=0;
    //frameDesc[toReplace].pinCount++;
  }

  else{
    int freeFrameNo;
    
    i=0;
    while (i < numBuffers){
      if (frameDesc[i].pageNo == -1){
        freeFrameNo=i;
        break;
      }
      i++;
    }

    i = 0;
    while (i < numBuffers){
      if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == INVALID_PAGE){
        hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo = PageId_in_a_DB;
        hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo = freeFrameNo;
        break;
      }
      i++;
    }

    page = &bufPool[freeFrameNo];
    if (emptyPage==FALSE) MINIBASE_DB -> read_page(PageId_in_a_DB, page);

    frameDesc[freeFrameNo].pageNo=PageId_in_a_DB;
    frameDesc[freeFrameNo].pinCount=0;
    frameDesc[freeFrameNo].dirty=FALSE;
    frameDesc[freeFrameNo].pinCount += 1;
    frameDesc[freeFrameNo].hate=0;
    
  }
  
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty, int hate ){

  Page* page = new Page;
  if (MINIBASE_DB->read_page(page_num, page) != OK) return FAIL;

  int frameToUnpin;
  for (int i = 0; i < numBuffers; i++){
    if (hashTable[hashFunc(page_num)].entries[i].pageNo == page_num){
      frameToUnpin=hashTable[hashFunc(page_num)].entries[i].frameNo;;
      break;
    }
  }

  if (frameDesc[frameToUnpin].pinCount!=0){
    frameDesc[frameToUnpin].pinCount--;
    if (frameDesc[frameToUnpin].pinCount<0) frameDesc[frameToUnpin].pinCount=0;
  }
  else return FAIL;

  if (!frameDesc[frameToUnpin].dirty) frameDesc[frameToUnpin].dirty = dirty;

  int numEmptyHate=0, numDuplicateHate=-1;
  for (int i = 0; i < numBuffers; i++) if (hateList[i]==-1) numEmptyHate++;

  if (numEmptyHate!=0){

    int numNotEmpty=0;
    for (int i = 0; i < numBuffers; i++){
      if (hateList[i] != -1){
        numNotEmpty++;
      }
    }

    for (int i = 0; i < numNotEmpty; ++i){
      if (hateList[i]==frameToUnpin){
        numDuplicateHate=i;
        break;
      } 
    }

    if (numDuplicateHate != -1){
      if (hate){

        if (frameDesc[frameToUnpin].hate == 1){
          int temp = hateList[numDuplicateHate];
          for (int i = numDuplicateHate; i > 0; i--) hateList[i]=hateList[i-1];
          hateList[0]=temp;
        }

        else{
          int temp = hateList[numDuplicateHate];
          for (int i = numDuplicateHate; i < numBuffers-1; i++) hateList[i]=hateList[i+1];
          hateList[numBuffers-1]=temp;
        }

      }
      else{
        frameDesc[frameToUnpin].hate = 1;
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i > 0; i--) hateList[i]=hateList[i-1];
        hateList[0]=temp;
      }
    }
    
    else{

      if (hate==FALSE){
        frameDesc[frameToUnpin].hate = 1;
        for (int i = numNotEmpty; i > 0; i--) hateList[i]=hateList[i-1];
        hateList[0]=frameToUnpin;
      } else hateList[numNotEmpty]=frameToUnpin;
      
    }
  }
  else{
    
    for (int i = 0; i < numBuffers; i++){
      if (hateList[i]==frameToUnpin){
        numDuplicateHate=i;
        break;  
      }    
    }

    if (hate){
      if (frameDesc[frameToUnpin].hate == 1){
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i > 0; i--) {
          hateList[i]=hateList[i-1];
        }
        hateList[0]=temp;    
      }

      else{
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i < numBuffers-1; i++) hateList[i]=hateList[i+1];
        hateList[numBuffers-1]=temp;
      }
    }
    else{  
      frameDesc[frameToUnpin].hate = 1;
      int temp = hateList[numDuplicateHate];
      for (int i = numDuplicateHate; i > 0; i--) hateList[i]=hateList[i-1];
      hateList[0]=temp;
    }
  }
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {

  int flg=0;
  for (int i = 0; i < numBuffers; i++) if (frameDesc[i].pinCount != 0) flg++;

  MINIBASE_DB -> allocate_page(firstPageId);
  if (flg==numBuffers){
    MINIBASE_DB -> deallocate_page(firstPageId);
    return FAIL;
  }
  else pinPage(firstPageId, firstpage);
  
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  int frameNo, entryindex;
  for (int i = 0; i < (int) numBuffers; i++){
    if (hashTable[hashFunc(globalPageId)].entries[i].pageNo == globalPageId) {
      frameNo=hashTable[hashFunc(globalPageId)].entries[i].frameNo;
      entryindex=i;
      break; 
    }
  }
  if (frameDesc[frameNo].pinCount==0){
    frameDesc[frameNo].pageNo=INVALID_PAGE;
    frameDesc[frameNo].dirty=FALSE;
    frameDesc[frameNo].hate=0;
    hashTable[hashFunc(globalPageId)].entries[entryindex].pageNo=INVALID_PAGE;
    hashTable[hashFunc(globalPageId)].entries[entryindex].frameNo=INVALID_PAGE;
    MINIBASE_DB -> deallocate_page(globalPageId);
  }else return FAIL;
  
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  for (int i=0; i < (int) numBuffers; i++){
    if (hashTable[hashFunc(pageid)].entries[i].pageNo == pageid) {
      frameDesc[hashTable[hashFunc(pageid)].entries[i].frameNo].dirty=FALSE;
      MINIBASE_DB -> write_page(pageid, &bufPool[hashTable[hashFunc(pageid)].entries[i].frameNo]);
      break;
    }
  }
  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  for (int i = 0; i < numBuffers; i++) if (frameDesc[i].pageNo != INVALID_PAGE) flushPage(frameDesc[i].pageNo);
  return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){

  if (MINIBASE_DB->read_page(PageId_in_a_DB, (Page*)(new Page)) != OK) return FAIL;
  
  else{ 
    
    for (int i = 0; i < numBuffers; i++){
      if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == PageId_in_a_DB){
        frameDesc[hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo].pinCount++;
        page = &bufPool[hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo];
        return OK;
      }
    }

    if (getNumUnpinnedBuffers()==0) return FAIL;
    
    int validcount=0;
    for (int i = 0; i < numBuffers; i++) if (frameDesc[i].pageNo != INVALID_PAGE)  validcount++;

    if (validcount==numBuffers){
      int toReplace;
      for (int i = numBuffers-1; i >= 0; i--){
        if (hateList[i] != -1 && frameDesc[hateList[i]].pinCount == 0){
          toReplace=hateList[i];
          break;
        }
      } 
      if (frameDesc[toReplace].dirty) flushPage(frameDesc[toReplace].pageNo);
      
      for (int i = 0; i < numBuffers; i++){
        if (hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].pageNo == frameDesc[toReplace].pageNo) {
          hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].pageNo=-1;
          hashTable[hashFunc(frameDesc[toReplace].pageNo)].entries[i].frameNo=-1;
          break;
        } 
      }
      for (int i = 0; i < numBuffers; i++){
        if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == -1){
          hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo = PageId_in_a_DB;
          hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo = toReplace;
          break;
        }
      }

      if (emptyPage==FALSE) MINIBASE_DB -> read_page(PageId_in_a_DB, &bufPool[toReplace]);

      frameDesc[toReplace].pageNo=PageId_in_a_DB;
      frameDesc[toReplace].pinCount=1;
      frameDesc[toReplace].dirty=FALSE;
      frameDesc[toReplace].hate=0;
      //frameDesc[toReplace].pinCount++;  
    
    }
    
    else{
      int freeFrameNo;
      for (int i = 0; i < numBuffers; i++){
        if (frameDesc[i].pageNo == -1){
          freeFrameNo=i;
          break;
        }
      }
      for (int i = 0; i < numBuffers; i++){
        if (hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo == -1){
          hashTable[hashFunc(PageId_in_a_DB)].entries[i].pageNo = PageId_in_a_DB;
          hashTable[hashFunc(PageId_in_a_DB)].entries[i].frameNo = freeFrameNo;
          break;
        }
      }
      page = &bufPool[freeFrameNo];
      if (emptyPage==FALSE) MINIBASE_DB -> read_page(PageId_in_a_DB, page);

      frameDesc[freeFrameNo].pageNo=PageId_in_a_DB;
      frameDesc[freeFrameNo].pinCount=1;
      frameDesc[freeFrameNo].dirty=FALSE;
      frameDesc[freeFrameNo].hate=0;
      //frameDesc[freeFrameNo].pinCount++;
    }
    return OK;
  }
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){

  
  if (MINIBASE_DB->read_page(globalPageId_in_a_DB, (Page *) (new Page)) != OK) return FAIL;
  
  int hate=FALSE, frameToUnpin, i=0;

  while (i < numBuffers){
    if (hashTable[hashFunc(globalPageId_in_a_DB)].entries[i].pageNo == globalPageId_in_a_DB) {
      frameToUnpin=hashTable[hashFunc(globalPageId_in_a_DB)].entries[i].frameNo;
      break; 
    }
    i++;
  }

  if (frameDesc[frameToUnpin].pinCount==0) return FAIL;
  else{
    frameDesc[frameToUnpin].pinCount--;
    if (frameDesc[frameToUnpin].pinCount<0) frameDesc[frameToUnpin].pinCount=0;
  }

  if (!frameDesc[frameToUnpin].dirty) frameDesc[frameToUnpin].dirty = dirty;

  int numEmptyHate=0, numDuplicateHate=-1;

  for (i = 0; i < numBuffers; i++) if (hateList[i]==-1) numEmptyHate++;

  if (numEmptyHate!=0){
    int numNotEmpty=0;
    for (int i = 0; i < numBuffers; i++) if (hateList[i] != -1) numNotEmpty++;

    for (int i = 0; i < numNotEmpty; ++i){
      if (hateList[i]==frameToUnpin){
        numDuplicateHate=i;
        break;
      }
    }

    if (numDuplicateHate != -1){
      if (hate){
        if (frameDesc[frameToUnpin].hate == 1){
          int temp = hateList[numDuplicateHate];
          for (int i = numDuplicateHate; i > 0; i--) hateList[i]=hateList[i-1];
          hateList[0]=temp;
        }
        else{
          int temp = hateList[numDuplicateHate];
          for (int i = numDuplicateHate; i < numBuffers-1; i++) hateList[i]=hateList[i+1];
          hateList[numBuffers-1]=temp;
        }
      }
      
      else{
        frameDesc[frameToUnpin].hate = 1;
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i > 0; i--)hateList[i]=hateList[i-1];
        hateList[0]=temp;
      }
    }
    
    else{
      if (!hate){
        frameDesc[frameToUnpin].hate = 1;
        for (int i = numNotEmpty; i > 0; i--) hateList[i]=hateList[i-1];
        hateList[0]=frameToUnpin;
      }
      else hateList[numNotEmpty]=frameToUnpin;
    }
  }
  
  else{
    
    for (int i = 0; i < numBuffers; i++){
      if (hateList[i]==frameToUnpin){
        numDuplicateHate=i;
        break;
      }
    }

    if (hate){
      if (frameDesc[frameToUnpin].hate == 1){
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i > 0; i--) hateList[i]=hateList[i-1];
        hateList[0]=temp;
      }
      else{
        int temp = hateList[numDuplicateHate];
        for (int i = numDuplicateHate; i < numBuffers-1; i++)hateList[i]=hateList[i+1];
        hateList[numBuffers-1]=temp;
      }
    }

    else{
      frameDesc[frameToUnpin].hate = 1;
      int temp = hateList[numDuplicateHate];
      for (int i = numDuplicateHate; i > 0; i--)hateList[i]=hateList[i-1];
      hateList[0]=temp;
    }

  }
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  int count=0;
  for (int i = 0; i < numBuffers; i++) if (frameDesc[i].pinCount == 0) count += 1;
  return count;
}