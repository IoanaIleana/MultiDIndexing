#include "QueryApplierFilter.h"
#include "IndexDescription.h"
#include "Iterator.h"
#include "constants.h"

#include <stdio.h>


extern uint64_t mod64mask;
extern uint64_t masks64[(int)64];
extern uint64_t max64;


void QueryApplierFilter::getRecordsRangeQueryDimCount1(Iterator* it)
{
    pthread_rwlock_rdlock(&m_globalLock);

    int pageStart = 0;
    int pageEnd=m_pageCount-1;
    if (it->m_startQueryVal)
    {
        pageStart = getFirstPageContainingKey(it->m_startQueryVal);
        if (pageStart < 0)
        {
            pthread_rwlock_unlock(&m_globalLock);
            return;
        }
    }

    if (!it->m_wildCardsMin[0])
    {
        int start=getFirstPageByFirstDimension(it->m_minRange[0]);
        if (start>pageStart) pageStart = start;
    }

    if (!it->m_wildCardsMax[0])
        pageEnd=getLastPageByFirstDimension(it->m_maxRange[0])-1;

    if (pageEnd<pageStart)
    {
        pthread_rwlock_unlock(&m_globalLock);
        return;
    }

    if (pageEnd == pageStart)
    {
        Page* page = m_pages[pageStart];
        pthread_rwlock_rdlock(&page->m_pageLock);
        int down = it->m_wildCardsMin[0]? 0: getFirstGEByDimOnPage(it->m_minRange[0], 0, page);
        if (down != page->m_recCount) //this test may be skipped if we also check the last record's value after the bin search
        {
            int up = it->m_wildCardsMax[0]? page->m_recCount: getLastLEByDimOnPage(it->m_maxRange[0], 0, page, down+1);
            for (int recIndice = down; recIndice<up; recIndice++)
                if (!page->m_records[recIndice].m_isDeletedFromIndex)
                    if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
                    {
                        break;
                    }
        }
        pthread_rwlock_unlock(&page->m_pageLock);
        pthread_rwlock_unlock(&m_globalLock);
        return;
    }

    //first page
    Page* page = m_pages[pageStart];
    pthread_rwlock_rdlock(&page->m_pageLock);
    int down = it->m_wildCardsMin[0]? 0:getFirstGEByDimOnPage(it->m_minRange[0], 0, page);
    for (int recIndice = down; recIndice<page->m_recCount; recIndice++)
        if (!page->m_records[recIndice].m_isDeletedFromIndex)
            if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
            {
                pthread_rwlock_unlock(&page->m_pageLock);
                pthread_rwlock_unlock(&m_globalLock);
                return; //iterator full
            }
    pthread_rwlock_unlock(&page->m_pageLock);

    //next pages
    for (int pageIndice = pageStart+1; pageIndice < pageEnd; pageIndice++)
    {
        page = m_pages[pageIndice];
        pthread_rwlock_rdlock(&page->m_pageLock);
        for (int recIndice = 0; recIndice<page->m_recCount; recIndice++)
            if (!page->m_records[recIndice].m_isDeletedFromIndex)
                if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
                {
                    pthread_rwlock_unlock(&page->m_pageLock);
                    pthread_rwlock_unlock(&m_globalLock);
                    return; //iterator full
                }
        pthread_rwlock_unlock(&page->m_pageLock);
    }


    //last page
    page = m_pages[pageEnd];
    pthread_rwlock_rdlock(&page->m_pageLock);
    int up = it->m_wildCardsMax[0]? page->m_recCount:getLastLEByDimOnPage(it->m_maxRange[0], 0, page);
    for (int recIndice = 0; recIndice<up; recIndice++)
        if (!page->m_records[recIndice].m_isDeletedFromIndex)
            if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
            {
                break;
            }
    pthread_rwlock_unlock(&page->m_pageLock);
    pthread_rwlock_unlock(&m_globalLock);
}



int QueryApplierFilter::processPageRange(Page* page, Iterator* it, int dimStartTesting)
{
    uint64_t prevbits[m_pageBitMaskSizeInts];
    int rez = fillPageRangeQueryBitMask(page, it, prevbits, dimStartTesting);

    if (!rez)
        return 0;

    if (rez == 1) //all in range
    {
        for (int recIndice = 0; recIndice<page->m_recCount; recIndice++)
            if (!page->m_records[recIndice].m_isDeletedFromIndex)
                if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
                    return 1; //iterator full
        return 0;
    }

    //partially in range
    for (int pageBlock = 0; pageBlock< m_pageBitMaskSizeInts; pageBlock++) if (prevbits[pageBlock])
        for (int pageBit =0; pageBit<64; pageBit++) if (prevbits[pageBlock]&masks64[pageBit])
        {
            int recIndice = (pageBlock<<6)+pageBit;
            if (!page->m_records[recIndice].m_isDeletedFromIndex)
                if (it->insertRec(page->m_records[recIndice].m_key, page->m_records[recIndice].m_payload))
                    return 1; //iterator full

        }

    return 0;
}

void QueryApplierFilter::getRecordsRangeQuery(Iterator* it)
{
    pthread_rwlock_rdlock(&m_globalLock);

    int pageStart = 0;
    int pageEnd=m_pageCount-1;
    if (it->m_startQueryVal)
    {
        pageStart = getFirstPageContainingKey(it->m_startQueryVal);
        if (pageStart < 0)
        {
            pthread_rwlock_unlock(&m_globalLock);
            return;
        }
    }

    if (!it->m_wildCardsMin[0])
    {
        int start=getFirstPageByFirstDimension(it->m_minRange[0]);
        if (start>pageStart) pageStart = start;
    }
    if (!it->m_wildCardsMax[0])
        pageEnd=getLastPageByFirstDimension(it->m_maxRange[0])-1;



    if (pageEnd<pageStart)
    {
        pthread_rwlock_unlock(&m_globalLock);
        return;
    }
    if (pageEnd == pageStart)
    {
        Page* page = m_pages[pageStart];
        pthread_rwlock_rdlock(&page->m_pageLock);
        processPageRange(page, it, 0);
        pthread_rwlock_unlock(&page->m_pageLock);
        pthread_rwlock_unlock(&m_globalLock);
        return;
    }

    //first page
    Page* page = m_pages[pageStart];
    pthread_rwlock_rdlock(&page->m_pageLock);
    int rez = processPageRange(page, it, 0);
    pthread_rwlock_unlock(&page->m_pageLock);
    if (rez == 1)
    {
        pthread_rwlock_unlock(&m_globalLock);
        return;
    }

    //other pages
    for (int p = pageStart+1; p<pageEnd; ++p)
    {
        page = m_pages[p];
        pthread_rwlock_rdlock(&page->m_pageLock);
        rez = (processPageRange(page, it, 1));
        pthread_rwlock_unlock(&page->m_pageLock);
        if (rez == 1)
        {
            pthread_rwlock_unlock(&m_globalLock);
            return;
        }
    }

    //last page
    page = m_pages[pageEnd];
    pthread_rwlock_rdlock(&page->m_pageLock);
    rez = processPageRange(page, it, 0);
    pthread_rwlock_unlock(&page->m_pageLock);
    pthread_rwlock_unlock(&m_globalLock);
}


int QueryApplierFilter::fillPageRangeQueryBitMask(Page* page, Iterator* it, uint64_t* bitmask, int dimStartTesting)
{

    int rez = 1; //all results in range
    uint64_t crtbits[m_pageBitMaskSizeInts];

    for (int dim = dimStartTesting; dim<m_desc.m_dimCount; dim++)
    {
        int down = it->m_wildCardsMin[dim]? 0: getFirstGEByDimOnPage(it->m_minRange[dim], dim, page);
        if (down == page->m_recCount) return 0; //no result

        int up = it->m_wildCardsMax[dim]? page->m_recCount : getLastLEByDimOnPage( it->m_maxRange[dim], dim, page, down+1);
        if (up <= down) return 0; //no result

        if (down>0 || up< page->m_recCount)
        {
            if (rez == 1)
            {
                 memset(bitmask, 0, m_pageBitMaskSizeBytes);
                 for (int r = down; r< up; ++r)
                 {
                    uint64_t indice = page->m_indices[dim][r];
                    uint64_t byte =indice>>6;
                    uint64_t bit = indice&mod64mask;
                    bitmask[byte]|=masks64[bit];
                 }
                 rez = 2;
            }
            else
            {
                memset(crtbits, 0, m_pageBitMaskSizeBytes);
                for (int r = down; r< up; ++r)
                {
                    uint64_t indice = page->m_indices[dim][r];
                    uint64_t byte =indice>>6;
                    uint64_t bit = indice&mod64mask;
                    crtbits[byte]|=masks64[bit];
                }
                for (int i = 0; i< m_pageBitMaskSizeInts; i++) bitmask[i]&=crtbits[i];
            }
        }
    }
    return rez;
}
