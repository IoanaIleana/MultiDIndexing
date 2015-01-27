#ifndef _Iterator_H
#define _Iterator_H

#include "../include/contest_interface.h"
#include "IndexDescription.h"
#include "constants.h"

class IndexManager;
class Transaction;
class Index;
class Payload;

class IteratorEntry
{
public:
    SerialKey* m_key;
    Payload* m_payload;
    bool m_comesFromIndex;
    bool m_isDeleted;
};

struct Iterator
{
public:
   ErrorCode init (Transaction* tx,
             Index* idx,
             Key min_keys,
             Key max_keys,
             bool ownsTransaction);

   void clear();

public:
    ErrorCode getNext(Record** record);

public:
    void adjustContentsFromIncomingWrites();
    int insertRec(SerialKey* key, Payload* payload);

private:
    void insertRecInternal(SerialKey* key, Payload* payload);
    void deleteRecInternal(SerialKey* key, QueryPayload* queryPayload, uint8_t flags);
    void updateRecInternal(SerialKey* key, QueryPayload* queryPayload, Payload* indexPayload, uint8_t flags);

public:
    bool keyMatchesRange(SerialKey* key);

public:
    Transaction* m_transaction;
    Index* m_indexHandle;
    IndexManager* m_manager;
    IndexDescription m_desc;
    bool m_ownsTransaction;
    bool m_isPointQuery;
    bool m_isAllWildcards;
    bool m_hasWildcards;

    int m_resultsCount;
    int m_crtPos;
    int m_maxRecCount;
    SerialKey* m_outOfCapMinVal;
    SerialKey* m_startQueryVal;
    int m_skipOnNext;

    SerialKey m_minRange[32];
    SerialKey m_maxRange[32];
    bool m_wildCardsMin[32];
    bool m_wildCardsMax[32];

    IteratorEntry m_records[MAX_ITERATOR_CAP];
};





#endif
