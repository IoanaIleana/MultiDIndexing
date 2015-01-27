#include "QueryApplierFilter.h"
#include "WriteOp.h"
#include "IndexDescription.h"
#include "Iterator.h"
#include "Allocator.h"
#include "constants.h"

#include <stdio.h>


extern uint64_t mod64mask;
extern uint64_t masks64[(int)64];
extern uint64_t max64;

uint64_t maxDataSize = (((uint64_t)48)*((uint64_t)1<<30)) /10;

QueryApplierFilter::QueryApplierFilter(const IndexDescription& desc)
: m_desc(desc)
{
    m_pageCount = 0;

    uint64_t maxRecCount = maxDataSize/(8*m_desc.m_dimCount);
    m_maxPageCount = 2*maxRecCount/PAGE_SIZE;

    m_pageBitMaskSizeInts = PAGE_SIZE/64;
    m_pageBitMaskSizeBytes = m_pageBitMaskSizeInts*sizeof(uint64_t);

    m_allocator = Allocator::get(m_desc.m_allocIndice);

    m_pages = (Page**)m_allocator->getPage(m_maxPageCount*sizeof(Page*));
    m_firstKeys = (SerialKey**)m_allocator->getPage(m_maxPageCount*sizeof(SerialKey*));

    m_keys = m_allocator->getPage(m_desc.m_keySize*ALLOC_KEY_COUNT);
    m_keyIndice = 0;

    pthread_rwlock_init(&m_globalLock, NULL);
    pthread_mutex_init(&m_insertLock, NULL);

}

QueryApplierFilter::~QueryApplierFilter()
{
    pthread_mutex_destroy(&m_insertLock);
    pthread_rwlock_destroy(&m_globalLock);
}


Page* QueryApplierFilter::allocNewPage(bool first)
{
    Page* pg = (Page*)m_allocator->getPage(sizeof(Page));

    pg->m_records = (RecordInfo*)m_allocator->getPage(PAGE_SIZE*sizeof(RecordInfo));
    pg->m_indices = (Indice**)m_allocator->getPage(sizeof(Indice*)*m_desc.m_dimCount);
    pg->m_values = (IntAndChar**)m_allocator->getPage(sizeof(IntAndChar*)*m_desc.m_dimCount);

    for (int i = 0; i<m_desc.m_dimCount; ++i)
    {
        pg->m_indices[i] = (Indice*)m_allocator->getPage(sizeof(Indice)*PAGE_SIZE);
        pg->m_values[i] = (IntAndChar*)m_allocator->getPage(sizeof(IntAndChar)*PAGE_SIZE);
    }
    pg->m_recCount = 0;
    pthread_rwlock_init(&pg->m_pageLock, NULL);

    return pg;
}

ErrorCode QueryApplierFilter::doWriteOperation(WriteOp& op)
{
    switch(op.m_type)
    {
        case kInsert: {return insertRec(op.m_key, op.m_indexPayload); break;}
        case kDelete: {deleteRec(op.m_key, &op.m_queryPayload, op.m_flags); return kOk; break;}
        case kUpdate: {updateRec(op.m_key, &op.m_queryPayload, op.m_indexPayload, op.m_flags); return kOk; break;}
        default: return kOk;
    }
}

void QueryApplierFilter::deleteRec(SerialKey* key, QueryPayload* payload, uint8_t flags)
{
    pthread_rwlock_rdlock(&m_globalLock);
    int start = getFirstPageContainingKey(key);
    if (start < 0)
    {
        pthread_rwlock_unlock(&m_globalLock);
        return; //not found
    }

    int countMatching = deleteRecInPage(key, payload, flags, m_pages[start]);
    pthread_rwlock_unlock(&m_globalLock);
}

int QueryApplierFilter::deleteRecInPage(SerialKey* key, QueryPayload* payload, uint8_t flags, Page* page)
{
    pthread_rwlock_wrlock(&page->m_pageLock);
    int pos = getFirstGEGlobalOrderOnPage(key, page);

    int countMatched= 0;
    while (pos<page->m_recCount)
    {
        if (!page->m_records[pos].m_isDeletedFromIndex)
          if (m_desc.compareKeys(key, page->m_records[pos].m_key) == 0)
            if (flags&kIgnorePayload || m_desc.payloadsAreEqual(payload, page->m_records[pos].m_payload))
            {
                page->m_records[pos].m_isDeletedFromIndex  = true;
                countMatched++;
                if (!(flags&kMatchDuplicates)) break;
            }
       pos++;
    }
    pthread_rwlock_unlock(&page->m_pageLock);
    return countMatched;

}


void QueryApplierFilter::updateRec(SerialKey* key, QueryPayload* payload, Payload* newpayload, uint8_t flags)
{
    pthread_rwlock_rdlock(&m_globalLock);
    int start = getFirstPageContainingKey(key);
    if (start < 0)
    {
         pthread_rwlock_unlock(&m_globalLock);
         return;
    }

    int countMatching = updateRecInPage(key, payload ,newpayload, flags, m_pages[start]);
    pthread_rwlock_unlock(&m_globalLock);
}


int QueryApplierFilter::updateRecInPage(SerialKey* key, QueryPayload* payload, Payload* newpayload, uint8_t flags, Page* page)
{
    pthread_rwlock_wrlock(&page->m_pageLock);
    int pos = getFirstGEGlobalOrderOnPage(key, page);

    int countMatched= 0;
    while (pos<page->m_recCount)
    {
        if (!page->m_records[pos].m_isDeletedFromIndex)
          if (m_desc.compareKeys(key, page->m_records[pos].m_key) == 0)
            if (flags&kIgnorePayload || m_desc.payloadsAreEqual(payload, page->m_records[pos].m_payload))
            {
                //release page->m_records[pos].m_payload
                page->m_records[pos].m_payload  = newpayload;
                //newpayload->addRef();

                countMatched++;
                if (!(flags&kMatchDuplicates)) break;
            }
       pos++;
    }
    pthread_rwlock_unlock(&page->m_pageLock);
    return countMatched;
}



ErrorCode QueryApplierFilter::insertRec(SerialKey* key, Payload* payload)
{
    pthread_mutex_lock(&m_insertLock);

    if (m_keyIndice == ALLOC_KEY_COUNT)
    {
        m_keys = m_allocator->getPage(m_desc.m_keySize*ALLOC_KEY_COUNT);
        m_keyIndice = 0;
    }
    SerialKey* indexKey = (SerialKey*)m_keys;
    memcpy(indexKey, key, m_desc.m_keySize);
    m_keys +=m_desc.m_keySize;
    m_keyIndice++;

    if (!m_pageCount)
    {
        m_pages[0] = allocNewPage(true);
        if (!m_pages[0]) return kErrorOutOfMemory;
        m_pageCount = 1;
        pthread_rwlock_wrlock(&m_globalLock);
        insertRecInPage(indexKey, payload, m_pages[0], 0);
        m_firstKeys[0] = indexKey;
        pthread_rwlock_unlock(&m_globalLock);
        pthread_mutex_unlock(&m_insertLock);
        return kOk;
    }

    int start = getFirstPageContainingKey(key);
    if (start < 0) start++; //we have to insert!
    Page* page = m_pages[start];

    int doSplit = (page->m_recCount == PAGE_SIZE-1);
    int position = getFirstGEGlobalOrderOnPage(key, page);

    insertRecInPage(indexKey, payload, page, position);

    if (!position) //the insert will affect the global structure, lock globally
    {
         pthread_rwlock_wrlock(&m_globalLock);
         m_firstKeys[start] = indexKey;
         pthread_rwlock_unlock(&m_globalLock);
    }


    if (doSplit)
    {
        Page* newPage = allocNewPage();
        if (!newPage) return kErrorOutOfMemory;

        pthread_rwlock_wrlock(&m_globalLock);

        memmove(m_pages+start+2, m_pages+start+1, (m_pageCount - start -1) * sizeof(Page*));
        memmove(m_firstKeys+start+2, m_firstKeys+start+1, (m_pageCount - start -1) * sizeof(SerialKey*));
        split(page, newPage);
        m_pages[start+1] = newPage;

        m_firstKeys[start] = page->m_records[0].m_key;
        m_firstKeys[start+1] = newPage->m_records[0].m_key;

        pthread_rwlock_unlock(&m_globalLock);
    }

    pthread_mutex_unlock(&m_insertLock);
    return kOk;
}


void QueryApplierFilter::insertRecInPage(SerialKey* key, Payload* payload, Page* page, int position)
{
    pthread_rwlock_wrlock(&page->m_pageLock);

    memmove(page->m_records+position+1, page->m_records+position, (page->m_recCount-position)*sizeof(RecordInfo));

    page->m_records[position].m_key = key;
    page->m_records[position].m_payload = payload;
//    payload->addRef();
    page->m_records[position].m_isDeletedFromIndex = false; //this will normally copy key and payload

    for (int i = 0; i< m_desc.m_dimCount; ++i)
        insertRecInDim(i, key[i], position, page);

    page->m_recCount++;
    pthread_rwlock_unlock(&page->m_pageLock);
}

void QueryApplierFilter::insertRecInDim(int dim, IntAndChar val, Indice indice, Page* page)
{
    //update indices for all the others
    //because we shifted in the original array
    for (int i = 0; i< page->m_recCount; ++i)
        if (page->m_indices[dim][i]>=indice)
            page->m_indices[dim][i]++;

    int end = 0;
    if (page->m_recCount)
    {
        end = getFirstGEByDimOnPage(val, dim, page);
        memmove(page->m_indices[dim]+end+1, page->m_indices[dim]+end, (page->m_recCount-end)*sizeof(Indice));
        memmove(page->m_values[dim]+end+1, page->m_values[dim]+end, (page->m_recCount-end)*sizeof(IntAndChar));
    }

    page->m_indices[dim][end] = indice;
    page->m_values[dim][end] = val;
}

void QueryApplierFilter::split(Page* page, Page* newPage)
{
    memmove(newPage->m_records, page->m_records + PAGE_SIZE/2, PAGE_SIZE/2 * sizeof(RecordInfo));

    for (int dim = 0; dim< m_desc.m_dimCount; ++dim)
    {
        int crtIndexInPage = 0;
        int crtIndexInNewPage = 0;

        for (int i = 0; i< PAGE_SIZE; ++i)
        {
            int crtRecIndice = page->m_indices[dim][i];
            if (crtRecIndice < PAGE_SIZE/2)
            {
                page->m_indices[dim][crtIndexInPage] = crtRecIndice;
                page->m_values[dim][crtIndexInPage] = page->m_values[dim][i];
                crtIndexInPage++;
            }
            else
            {
                newPage->m_indices[dim][crtIndexInNewPage] = crtRecIndice - PAGE_SIZE/2;
                newPage->m_values[dim][crtIndexInNewPage] = page->m_values[dim][i];
                crtIndexInNewPage++;
            }
        }
    }


    page->m_recCount = PAGE_SIZE/2;
    newPage->m_recCount = PAGE_SIZE/2;

    m_pageCount++;

}



ErrorCode QueryApplierFilter::getRecords(Iterator* it)
{
    if (!m_pageCount) return kOk;
    if (it->m_isAllWildcards) getRecordsAllWildCards(it);
    else if (it->m_isPointQuery)
    {
        if (m_desc.m_dimCount == 1) getRecordsPointQueryDimCount1(it);
        else if (!it->m_hasWildcards) getRecordsPointQueryNoWildcards(it);
        else getRecordsPointQuery(it);
    }
    else
    {
        if (m_desc.m_dimCount == 1) getRecordsRangeQueryDimCount1(it);
        else getRecordsRangeQuery(it);
    }
    return kOk;
}





void QueryApplierFilter::getRecordsAllWildCards(Iterator* it)
{
    pthread_rwlock_rdlock(&m_globalLock);
    int pageStart = 0;
    if (it->m_startQueryVal)
    {
        pageStart = getFirstPageContainingKey(it->m_startQueryVal);
        if (pageStart < 0) return ; //well, not here
    }
    else pageStart = 0;

    for (int i = pageStart; i<m_pageCount; ++i)
    {
        pthread_rwlock_rdlock(&m_pages[i]->m_pageLock);
        for (int j = 0; j< m_pages[i]->m_recCount; ++j)
            if (!m_pages[i]->m_records[j].m_isDeletedFromIndex)
                if (it->insertRec(m_pages[i]->m_records[j].m_key, m_pages[i]->m_records[j].m_payload))
                {
                    pthread_rwlock_unlock(&m_pages[i]->m_pageLock);
                    pthread_rwlock_unlock(&m_globalLock);
                    return;
                }
        pthread_rwlock_unlock(&m_pages[i]->m_pageLock);
    }
    pthread_rwlock_unlock(&m_globalLock);
}


int QueryApplierFilter::getFirstPageContainingKey(SerialKey* key)
{
    //returns the index of the last page whose first record
    //is lower or equal than the argument
    int start = 0;
    int end = m_pageCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int compareToStartOfPage = m_desc.compareKeys(key, m_firstKeys[med]);
        if (compareToStartOfPage < 0) end = med-1;
        else start = med+1;
    }

    start--; //this is the page where we are inserting; it will always be in array bounds
    return start;
}


int QueryApplierFilter::getFirstGEGlobalOrderOnPage(SerialKey* key, Page* page)
{
    //return the index of the first element in the page
    //that is greater or equal to this element
    int start = 0;
    int end = page->m_recCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int compareToRecord =  m_desc.compareKeys(key, page->m_records[med].m_key);
        if (compareToRecord > 0) start=med+1;
        else end = med-1;
    }
    end++;
    return end;
}

int QueryApplierFilter::getFirstGEByDimOnPage(IntAndChar val, int dim, Page* page)
{
    int start = 0;
    int end = page->m_recCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int compareToRecord =  m_desc.m_compares[dim](val, page->m_values[dim][med]);
        if (compareToRecord > 0) start=med+1;
        else end = med-1;
    }
    end++;
    return end;
}


int QueryApplierFilter::getLastLEByDimOnPage(IntAndChar val, int dim, Page* page, int initstart)
{
    int start = initstart;
    int end = page->m_recCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int comp =  m_desc.m_compares[dim](val, page->m_values[dim][med]);
        if (comp >= 0) start=med+1;
        else end = med-1;
    }
    return start;
}


int QueryApplierFilter::getFirstPageByFirstDimension(IntAndChar val)
{
    int start = 0;
    int end = m_pageCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int compareToStartOfPage = m_desc.m_compares[0](val, m_firstKeys[med][0]);
        if (compareToStartOfPage <= 0) end = med-1;
        else start = med+1;
    }

    start--;
    return start;
}

int QueryApplierFilter::getLastPageByFirstDimension(IntAndChar val)
{
    int start = 0;
    int end = m_pageCount-1;
    while (start <= end)
    {
        int med = (start+end)/2;
        int compareToStartOfPage =  m_desc.m_compares[0](val, m_firstKeys[med][0]);
        if (compareToStartOfPage < 0) end = med-1;
        else start = med+1;
    }

    return start;
}




















