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
    int startHand = clockHand; // Save the starting point

    do {
        BufDesc &buf = bufTable[clockHand]; // Current candidate frame

        if (buf.refbit) {
            buf.refbit = false;
        } else if (buf.pinCnt == 0) { // Unppinned frame found
            found = true;
            frame = clockHand; // Store the allocated frame number

            if (buf.valid) {
                hashTable.remove(buf.file, buf.pageNo);

                if (buf.dirty) { // Write the dirty page to disk
                    if (buf.file->writePage(buf->pageNo, &(bufPool[frame])) != OK) {
                        return UNIXERR;
                    }
                    buf.dirty = false;
                }
            }
            buf.Clear();
            advanceClock();
            return OK;
        }

        advanceClock();
    } while (clockHand != startHand);

    return BUFFEREXCEEDED;

}







	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{





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
    unpinPageStatus = hashTable->lookup(file, pageNo, unPinFrameNo);

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

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







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


