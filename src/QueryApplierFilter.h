#ifndef _QueryApplierFilter_H
#define _QueryApplierFilter_H

#include "IndexDescription.h"

#include <memory.h>
#include <stdio.h>

class Allocator;

typedef uint16_t Indice;

class RecordInfo
{
public:
    SerialKey* m_key;
    Payload* m_payload;
    uint64_t m_isDeletedFromIndex;
};

class Page
{
public:
    RecordInfo* m_records;
    Indice** m_indices;
    IntAndChar** m_values;

    int m_recCount;
    pthread_rwlock_t m_pageLock;
};


class QueryApplierFilter
{
public:
    QueryApplierFilter(const IndexDescription& desc);
    ~QueryApplierFilter();

public://iQueryAppplier
    ErrorCode getRecords(Iterator* it);
    ErrorCode doWriteOperation(WriteOp& op);

private:
    void getRecordsPointQueryDimCount1(Iterator* it);

    void getRecordsPointQuery(Iterator* it);
    void getRecordsPointQueryNoWildcards(Iterator* it);

    int processPagePoint(Page* page, Iterator* it, int dimStartTesting);
    int processPagePointNoWildcards(Page* page, Iterator* it, int dimStartTesting);

    int fillPagePointQueryBitMask(Page* page, Iterator* it, uint64_t* bitmask, int dimStartTesting);
    int fillPagePointQueryBitMaskNoWildcards(Page* page, Iterator* it, uint64_t* bitmask, int dimStartTesting);

private:
    void getRecordsRangeQueryDimCount1(Iterator* it);

    void getRecordsRangeQuery(Iterator* it);

    int processPageRange(Page* page, Iterator* it, int dimStartTesting);

    int fillPageRangeQueryBitMask(Page* page, Iterator* it, uint64_t* bitmask, int dimStartTesting);

private:
    void getRecordsAllWildCards(Iterator* it);



private:
    ErrorCode insertRec(SerialKey* key, Payload* payload);
    void deleteRec(SerialKey* key, QueryPayload* payload, uint8_t flags);
    void updateRec(SerialKey* key, QueryPayload* payload, Payload* newpayload, uint8_t flags);

private:
    void insertRecInPage(SerialKey* key, Payload* payload, Page* page, int position); //1 = must be splitted
    int deleteRecInPage(SerialKey* key, QueryPayload* payload, uint8_t flags, Page* page); //count deleted
    int updateRecInPage(SerialKey* key, QueryPayload* payload, Payload* newpayload, uint8_t flags, Page* page);//count updated

private:
    //returns the insertion position
    void insertRecInDim(int dim, IntAndChar val, Indice indice, Page* page);
    void split(Page* page, Page* newPage);

private:
    int getFirstPageContainingKey(SerialKey* key);
    int getFirstGEGlobalOrderOnPage(SerialKey* key, Page* page);

    int getFirstGEByDimOnPage(IntAndChar val, int dim, Page* page);
    int getLastLEByDimOnPage(IntAndChar val, int dim, Page* page, int initstart = 0);


    int getFirstPageByFirstDimension(IntAndChar val);
    int getLastPageByFirstDimension(IntAndChar val);

private:
    Page* allocNewPage(bool first = false);

private:



private:
    Page** m_pages;
    SerialKey** m_firstKeys;

    int m_pageCount;
    int m_maxPageCount;

    IndexDescription m_desc;

    int m_pageBitMaskSizeInts;
    int m_pageBitMaskSizeBytes;

    uint8_t *m_keys;
    int m_keyIndice;

    pthread_rwlock_t m_globalLock;
    pthread_mutex_t m_insertLock;

    Allocator* m_allocator;
};











#endif
