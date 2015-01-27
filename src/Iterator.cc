#include "Iterator.h"

#include "Transaction.h"
#include "Index.h"
#include "IndexManager.h"
#include "constants.h"
#include "IndexDescription.h"

#include <stdint.h>
#include <stdio.h>

# define INT64_MIN		    (-__INT64_C(9223372036854775807)-1)
# define INT64_MAX              (__INT64_C(9223372036854775807))
# define INT32_MAX		     (2147483647)
# define INT32_MIN		     (-2147483647-1)


ErrorCode Iterator::init(Transaction* tx, Index* idx,
                   Key min_keys, Key max_keys, bool ownsTransaction)
{
     m_transaction = tx;
     m_indexHandle = idx;
     m_manager = idx->m_indexManager;
     m_desc = idx->m_indexManager->m_desc;

     m_ownsTransaction = ownsTransaction;

     m_resultsCount = 0;
     m_crtPos = 0;
     m_outOfCapMinVal = 0;
     m_startQueryVal = 0;
     m_skipOnNext = 0;

     ErrorCode retCode = m_manager->m_desc.fillQueryKey(m_minRange, min_keys);
     if (retCode != kOk) return retCode;

     retCode = m_manager->m_desc.fillQueryKey(m_maxRange, max_keys);
     if (retCode != kOk) return retCode;

     m_isPointQuery = true;
     m_isAllWildcards = true;
     m_hasWildcards = false;
     for (int i = 0; i<m_manager->m_desc.m_dimCount; i++)
     {
         if (!min_keys.value[i] ||
             (m_manager->m_desc.m_types[i] == 0 && m_minRange[i].intval == INT64_MIN)
             || (m_manager->m_desc.m_types[i] == 1 && m_minRange[i].intval == INT32_MIN))
             {m_wildCardsMin[i] = true;m_hasWildcards = true;}
         else {m_wildCardsMin[i] = false; m_isAllWildcards = false;}

         if (!max_keys.value[i] ||
             (m_manager->m_desc.m_types[i] == 0 && m_maxRange[i].intval == INT64_MAX)
             || (m_manager->m_desc.m_types[i] == 1 && m_maxRange[i].intval == INT32_MAX))
             {m_wildCardsMax[i] = true;m_hasWildcards = true;}
         else {m_wildCardsMax[i] = false; m_isAllWildcards = false;}

         if (m_wildCardsMax[i]!=m_wildCardsMin[i]) m_isPointQuery = false;
         else if (!m_wildCardsMin[i] && m_manager->m_desc.compareKeysOnDim(m_minRange, m_maxRange, i) !=0)
            m_isPointQuery = false;
    }

    m_maxRecCount = (m_isPointQuery? MAX_RESULTS_POINT:MAX_RESULTS_RANGE);
    return kOk;
}

void Iterator::clear()
{
    m_desc.clearQueryKey(m_minRange);
    m_desc.clearQueryKey(m_maxRange);
}

ErrorCode Iterator::getNext(Record** record)
{
    while (m_crtPos<m_resultsCount && m_records[m_crtPos].m_isDeleted) m_crtPos++;

    if (m_crtPos >= m_resultsCount)
    {
        if (!m_outOfCapMinVal) return kErrorNotFound; //we don't need to redo the query

        //redo the query
        //find the records we will skip on next query (those equal to the first out of capacity value)
        int i = m_resultsCount -1;
        m_skipOnNext = 0;
        while (i>0 && m_desc.keysAreEqual(m_records[i].m_key, m_outOfCapMinVal))
        {
            if (m_records[i].m_comesFromIndex) m_skipOnNext++;
            i--;
        }

        m_startQueryVal = m_outOfCapMinVal; //next query starts here
        m_outOfCapMinVal = 0;
        m_resultsCount = 0;
        m_crtPos = 0;
        m_maxRecCount = m_isPointQuery? MAX_RESULTS_POINT_BIS:MAX_RESULTS_RANGE_BIS;

        m_manager->getRecords(this);
        return getNext(record);
    }

    m_manager->m_desc.createOrgaRecord(m_records[m_crtPos].m_key, m_records[m_crtPos].m_payload, record);
    //m_desc.releasePayload(m_records[m_crtPos].m_payload);

    m_crtPos++;
    return kOk;
}

int Iterator::insertRec(SerialKey* key, Payload* payload)
{
    if (m_startQueryVal) {
        if (m_manager->m_desc.compareKeys(key, m_startQueryVal) < 0) return 0; //skip it
        if (m_skipOnNext && m_manager->m_desc.keysAreEqual(key, m_startQueryVal))
        {
            m_skipOnNext--;
            return 0;
        }
    }

    if (m_resultsCount == m_maxRecCount)
    {
        if (!m_outOfCapMinVal) m_outOfCapMinVal = key;
        return 1; //iterator is full
    }

    m_records[m_resultsCount].m_key = key;
    m_records[m_resultsCount].m_payload = payload;
    //payload->addRef();

    m_records[m_resultsCount].m_isDeleted = false;
    m_records[m_resultsCount].m_comesFromIndex = true;

    m_resultsCount++;
    return 0;
}


void Iterator::adjustContentsFromIncomingWrites()
{
    for (int i = 0; i<m_transaction->m_countWriteOps; ++i)
    {
        WriteOp& op = m_transaction->m_writeOps[i];
        if (keyMatchesRange(op.m_key))
            switch (op.m_type)
            {
                case kInsert: {insertRecInternal(op.m_key, op.m_indexPayload);break;}
                case kUpdate: {updateRecInternal(op.m_key, &op.m_queryPayload, op.m_indexPayload, op.m_flags); break;}
                case kDelete: {deleteRecInternal(op.m_key, &op.m_queryPayload, op.m_flags); break;}
            }
    }
}

void Iterator::insertRecInternal(SerialKey* key, Payload* payload)
{
    if (m_resultsCount == MAX_ITERATOR_CAP)
    {
        printf("error: beyond maximum iterator capacity\n");fflush(stdout);
        return;
    }

    int i = 0;
    while (i<m_resultsCount && m_desc.compareKeys(m_records[i].m_key, key) <0) i++;
    for (int j = m_resultsCount; j>i; j--) m_records[j] = m_records[j-1];

    m_records[i].m_key = key;
    m_records[i].m_payload = payload;
    //payload->addRef();

    m_records[i].m_isDeleted = false;
    m_records[i].m_comesFromIndex = false;

    m_resultsCount++;
}

void Iterator::deleteRecInternal(SerialKey* key, QueryPayload* payload, uint8_t flags)
{
    for (int i = 0; i< m_resultsCount; ++i) if (!m_records[i].m_isDeleted)
        if (m_desc.keysAreEqual(key, m_records[i].m_key))
            if (flags&kIgnorePayload || m_manager->m_desc.payloadsAreEqual(payload, m_records[i].m_payload))
            {
                m_records[i].m_isDeleted = true;
//                m_desc.releasePayload(m_records[i].m_payload);
                m_records[i].m_payload = 0;

                if (!(flags & kMatchDuplicates)) break;
            }
}

void Iterator::updateRecInternal(SerialKey* key, QueryPayload* payload, Payload* newPayload, uint8_t flags)
{
    for (int i = 0; i< m_resultsCount; ++i) if (!m_records[i].m_isDeleted)
        if (m_desc.keysAreEqual(key, m_records[i].m_key))
            if (flags&kIgnorePayload || m_desc.payloadsAreEqual(payload, m_records[i].m_payload))
            {
//                m_desc.releasePayload(m_records[i].m_payload);
                m_records[i].m_payload = newPayload;
                //newPayload->addRef();

                if (!(flags & kMatchDuplicates)) break;
            }
}


bool Iterator::keyMatchesRange(SerialKey* key)
{
    for (int i=0; i<m_desc.m_dimCount; i++)
    {
        if (!m_wildCardsMin[i] && m_manager->m_desc.compareKeysOnDim(key, m_minRange, i) <0 ) return false;
        if (!m_wildCardsMax[i] && m_manager->m_desc.compareKeysOnDim(key, m_maxRange, i) >0 ) return false;
    }
    return true;
}

