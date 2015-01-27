#ifndef _ConnectionManager_H
#define _ConnectionManager_H

#include "../include/contest_interface.h"
#include <pthread.h>
#include <string>
#include <map>

class IndexManager;

typedef std::map<std::string, IndexManager*> IndexPool;

class ConnectionManager
{
public:
    ConnectionManager();

    ~ConnectionManager();

    static ConnectionManager& get();

public:

    ErrorCode BeginTransaction(Transaction** tx);

    ErrorCode AbortTransaction(Transaction **tx);

    ErrorCode CommitTransaction(Transaction **tx);

    ErrorCode CreateIndex(const char* name, uint8_t attribute_count, KeyType type);

    ErrorCode OpenIndex(const char* name, Index **idx);

    ErrorCode CloseIndex(Index** idx);

    ErrorCode DeleteIndex(const char* name);

    ErrorCode InsertRecord(Transaction *tx, Index *idx, Record *record);

    ErrorCode UpdateRecord(Transaction *tx, Index *idx, Record *record, Block *new_payload, uint8_t flags);

    ErrorCode DeleteRecord(Transaction *tx, Index *idx, Record *record, uint8_t flags);

    ErrorCode GetRecords(Transaction *tx, Index *idx, Key min_keys, Key max_keys, Iterator **it, bool ownsTransaction);

    ErrorCode GetNext(Iterator *it, Record** record);

    ErrorCode CloseIterator(Iterator** it);

private:
    int m_indexCount;
    IndexPool m_indexes;
    pthread_mutex_t m_indexesLock;
};

#endif
