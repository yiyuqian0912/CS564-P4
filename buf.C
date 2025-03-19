#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

const Status BufMgr::allocBuf(int & frame) 
{
    int startHand = clockHand;
    bool checkedAll = false;

    while (true) {
        BufDesc &buf = bufTable[clockHand];

        if (buf.refbit) {
            // Clear reference bit and give the frame another chance
            buf.refbit = false;
        } else if (buf.pinCnt == 0) {
            // Found an unpinned frame; allocate it
            frame = clockHand;

            // Flush and clear existing page if necessary
            if (buf.valid) {
                hashTable->remove(buf.file, buf.pageNo);
                if (buf.dirty) {
                    Status status = buf.file->writePage(buf.pageNo, &(bufPool[frame]));
                    if (status != OK) 
                        return UNIXERR;
                }
            }

            buf.Clear(); 
            advanceClock();
            return OK;
        }

        advanceClock();

        // After a full pass, check if all frames are pinned
        if (clockHand == startHand) {
            if (checkedAll) {
                // All frames are pinned; return error
                return BUFFEREXCEEDED;
            } else {
                checkedAll = true;  // Mark that we've checked all frames once
            }
        }
    }
}

/*
 * First check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.  
 * There are two cases to be handled depending on the outcome of the lookup() call:
 * Case 1) Page is not in the buffer pool.  
 *	Call allocBuf() to allocate a buffer frame and then call the method file->readPage() to read the page from disk into the buffer pool frame. 
 * 	Next, insert the page into the hashtable. Finally, invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for the page set to 1.  
 * 	Return a pointer to the frame containing the page via the page parameter.
 * Case 2)  Page is in the buffer pool.  
 * 	In this case set the appropriate refbit, increment the pinCnt for the page, and then return a pointer to the frame containing the page via the page parameter.
 * 
 * 	Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
 */
	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
       
    if (status == HASHNOTFOUND)  // case 1 (page not in buffer pool)
    {
      //allocate buffer frame  
      status = allocBuf(frameNo);
      if (status != OK) return status;
    
      //read page from disk into buffer pool
      status = file->readPage(PageNo, &bufPool[frameNo]); 
      if (status == HASHNOTFOUND) return UNIXERR;
    
      //insert page into hashtable
      status = hashTable->insert(file, PageNo, frameNo);
      if (status == HASHTBLERROR) return status; 
      
      //set up frame properly
      bufTable[frameNo].Set(file, PageNo);
      
      //return pointer to frame containing page
      page = &bufPool[frameNo];
    } else // case 2 (page is in buffer pool)
    {
      //return pointer to frame containing page
      page = &bufPool[frameNo];
      //set refbit and increment pincnt if successfully read
      bufTable[frameNo].refbit = true;
      bufTable[frameNo].pinCnt++;
    }
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    //David Wolske's Section.
    Status unpinPageStatus = OK;
    int unPinFrameNo;

    // Check if (file,pageNo) is currently in the buffer pool (ie. in
    // the hash table.  If so, return the corresponding frameNo via the frameNo
    // parameter.  Else, return HASHNOTFOUND
    unpinPageStatus = hashTable->lookup(file, PageNo, unPinFrameNo);

    if (unpinPageStatus != OK) {
        //Returns: HASHNOTFOUND if the page is not in the buffer pool hash table, 
        return HASHNOTFOUND;
    }

    //bufTable[frameNo].Clear(); format I used for accessing pinCnt and dirty.

    if (bufTable[unPinFrameNo].pinCnt == 0) {
        //Returns: PAGENOTPINNED if the pin count is already 0.
        return PAGENOTPINNED;
    } else {
        //Decrements the pinCnt of the frame containing (file, PageNo)
        bufTable[unPinFrameNo].pinCnt--;
    }

    if (dirty) {
        //if dirty == true, sets the dirty bit.
        bufTable[unPinFrameNo].dirty = true;
    }
 
    //Returns: OK if no errors occurred,
    return OK;
}

/*This call is kind of weird.  The first step is to to allocate an empty page in the specified file by invoking the 
file->allocatePage() method. This method will return the page number of the newly allocated page.  Then allocBuf() 
is called to obtain a buffer pool frame.  Next, an entry is inserted into the hash table and Set() is invoked on 
the frame to set it up properly.  The method returns both the page number of the newly allocated page to the caller
via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter. 
Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned 
and HASHTBLERROR if a hash table error occurred. */

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    //allocate a new page in the file
    Status status = file->allocatePage(pageNo);
    if (status != OK) return status;

    //allocate a buffer frame
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK) return status;

    //insert the page into the hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if (status == HASHTBLERROR) return status;

    //set up the buffer frame
    bufTable[frameNo].Set(file, pageNo);

    //return the allocated page pointer
    page = &bufPool[frameNo];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
