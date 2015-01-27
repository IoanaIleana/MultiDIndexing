#include "ConnectionManager.h"
#include "Index.h"
#include "IndexManager.h"
#include "Transaction.h"
#include "Iterator.h"
#include "constants.h"

#ifndef _SIG_NO_NUMA_
#include <numa.h>
#endif
#include <stdio.h>

__thread int myNumaNodeForThisThread = 0;

ConnectionManager::ConnectionManager()
: m_indexCount(0)
{
    pthread_mutex_init(&m_indexesLock, NULL);
}

ConnectionManager::~ConnectionManager()
{
    pthread_mutex_destroy(&m_indexesLock);
}

ConnectionManager& ConnectionManager::get()
{
    static ConnectionManager manager;
    return manager;
}


ErrorCode CreateIndex(const char* name, uint8_t attribute_count, KeyType type)
{
    return ConnectionManager::get().CreateIndex(name, attribute_count, type);
}

ErrorCode ConnectionManager::CreateIndex(const char* name, uint8_t attribute_count, KeyType type)
{
    pthread_mutex_lock(&m_indexesLock);
    IndexPool::iterator it = m_indexes.find(name);
    if (m_indexes.end() != it) {
        pthread_mutex_unlock(&m_indexesLock);
        return kErrorIndexExists;
    }

    IndexManager* idxmanager = 0;
#ifndef _SIG_NO_NUMA_
    struct bitmask* nodes=numa_allocate_nodemask();;
    numa_bitmask_setbit(nodes, m_indexCount%2);
    numa_bind(nodes);
    numa_free_nodemask(nodes);
#endif

    ErrorCode retCode = IndexManager::createEmptyIndexManager(attribute_count, type, m_indexCount, &idxmanager);
    if (kOk != retCode)
    {
        pthread_mutex_unlock(&m_indexesLock);
        return retCode;
    }

    m_indexes[name] = idxmanager;
    m_indexCount++;

    pthread_mutex_unlock(&m_indexesLock);
    return kOk;
}


ErrorCode DeleteIndex(const char* name)
{
    return ConnectionManager::get().DeleteIndex(name);
}

ErrorCode ConnectionManager::DeleteIndex(const char* name)
{
    pthread_mutex_lock(&m_indexesLock);
    IndexPool::iterator it = m_indexes.find(name);
    if (m_indexes.end() == it) {
        pthread_mutex_unlock(&m_indexesLock);
        return kErrorUnknownIndex;
    }
    IndexManager* idxmanager = it->second;
    ErrorCode retCode = idxmanager->deleteIndex();
    if (kOk != retCode)
    {
        pthread_mutex_unlock(&m_indexesLock);
        return retCode;
    }

    m_indexes.erase(it);

    pthread_mutex_unlock(&m_indexesLock);
    return kOk;
}

ErrorCode OpenIndex(const char* name, Index **idx)
{
    return ConnectionManager::get().OpenIndex(name, idx);
}

ErrorCode ConnectionManager::OpenIndex(const char* name, Index **idx)
{
    pthread_mutex_lock(&m_indexesLock);
    IndexPool::iterator it = m_indexes.find(name);
    if (m_indexes.end() == it) {
        pthread_mutex_unlock(&m_indexesLock);
        return kErrorUnknownIndex;
    }


    IndexManager* idxmanager = it->second;
#ifndef _SIG_NO_NUMA_
    struct bitmask* nodes=numa_allocate_nodemask();;
    numa_bitmask_setbit(nodes, idxmanager->m_desc.m_allocIndice);
    numa_bind(nodes);
    numa_free_nodemask(nodes);
    myNumaNodeForThisThread = idxmanager->m_desc.m_allocIndice;
#endif

    ErrorCode retCode = idxmanager->createIndex(idx);

    pthread_mutex_unlock(&m_indexesLock);
    return retCode;
}

ErrorCode CloseIndex(Index **idx)
{
    return ConnectionManager::get().CloseIndex(idx);
}

ErrorCode ConnectionManager::CloseIndex(Index** idx)
{
    if (NULL == *idx) return kErrorUnknownIndex;

    ErrorCode retCode = (*idx)->m_indexManager->closeIndex(*idx);
    *idx = NULL;

    return retCode;
}


ErrorCode BeginTransaction(Transaction **tx)
{
    return ConnectionManager::get().BeginTransaction(tx);
}

ErrorCode ConnectionManager::BeginTransaction(Transaction** tx)
{
    //printf("%d\n", myNumaNodeForThisThread);
    return IndexManager::createEmptyTransaction(tx);
}

ErrorCode AbortTransaction(Transaction **tx)
{
    return ConnectionManager::get().AbortTransaction(tx);
}

ErrorCode ConnectionManager::AbortTransaction(Transaction **tx)
{
    if (NULL == *tx) return kErrorTransactionClosed;

    ErrorCode retCode = kOk;
    if ((*tx)->m_indexManager)
        retCode = (*tx)->m_indexManager->abortTransaction(*tx);
    *tx = NULL;

    return retCode;
}

ErrorCode CommitTransaction(Transaction **tx)
{
    return ConnectionManager::get().CommitTransaction(tx);
}

ErrorCode ConnectionManager::CommitTransaction(Transaction **tx)
{
    if (NULL == *tx) return kErrorTransactionClosed;

    ErrorCode retCode = kOk;
    if ((*tx)->m_indexManager)
        retCode = (*tx)->m_indexManager->commitTransaction(*tx);
    *tx = NULL;

    return retCode;
}


ErrorCode InsertRecord(Transaction *tx, Index *idx, Record *record)
{
    return ConnectionManager::get().InsertRecord(tx, idx, record);
}

ErrorCode ConnectionManager::InsertRecord(Transaction *tx, Index *idx, Record *record)
{
    if (NULL == idx) return kErrorUnknownIndex;

    ErrorCode retCode = kOk;
    IndexManager* manager = idx->m_indexManager;

    WriteOp op;
    retCode = manager->m_desc.fillWriteOp(op, kInsert, record->key, &record->payload, 0, 0);
    if (kOk != retCode) return retCode;

    if (!tx) return manager->commitSingleOp(op);

    retCode = tx->addWriteOp(op);
    if (kOk != retCode) return retCode;

    return manager->addTransaction(tx);
}

ErrorCode DeleteRecord(Transaction *tx, Index *idx, Record *record, uint8_t flags)
{
    return ConnectionManager::get().DeleteRecord(tx, idx, record, flags);
}

ErrorCode ConnectionManager::DeleteRecord(Transaction *tx, Index *idx, Record *record, uint8_t flags)
{
    if (NULL == idx) return kErrorUnknownIndex;

    ErrorCode retCode = kOk;
    IndexManager* manager = idx->m_indexManager;

    WriteOp op;
    retCode = manager->m_desc.fillWriteOp(op, kDelete, record->key, &record->payload, 0, flags);
    if (kOk != retCode) return retCode;

    if (!tx) return manager->commitSingleOp(op);

    retCode = tx->addWriteOp(op);
    if (kOk!= retCode) return retCode;

    return manager->addTransaction(tx);
}

ErrorCode UpdateRecord(Transaction *tx, Index *idx, Record *record, Block *new_payload, uint8_t flags)
{
    return ConnectionManager::get().UpdateRecord(tx, idx, record, new_payload, flags);
}


ErrorCode ConnectionManager::UpdateRecord(Transaction *tx, Index *idx, Record *record, Block *new_payload, uint8_t flags)
{
    if (NULL == idx) return kErrorUnknownIndex;

    ErrorCode retCode = kOk;
    IndexManager* manager = idx->m_indexManager;

    WriteOp op;
    retCode = manager->m_desc.fillWriteOp(op, kUpdate, record->key, &record->payload, new_payload, flags);
    if (kOk != retCode) return retCode;

    if (!tx) return manager->commitSingleOp(op);

    retCode = tx->addWriteOp(op);
    if (kOk != retCode) return retCode;

    return idx->m_indexManager->addTransaction(tx);
}


ErrorCode GetRecords(Transaction *tx, Index *idx, Key min_keys, Key max_keys, Iterator **it)
{
    if (NULL == tx)
    {
        Transaction* tr = NULL;
        ErrorCode retCode = BeginTransaction(&tr);
        if (kOk != retCode) return retCode;

        return ConnectionManager::get().GetRecords(tr, idx, min_keys, max_keys, it, true);
    }

    return ConnectionManager::get().GetRecords(tx, idx, min_keys, max_keys, it, false);
}

ErrorCode ConnectionManager::GetRecords(Transaction *tx, Index *idx, Key min_keys, Key max_keys, Iterator **it, bool ownsTransaction)
{
    if (NULL == idx) return kErrorUnknownIndex;

    ErrorCode retCode = kOk;
    IndexManager* manager = idx->m_indexManager;

    retCode = manager->createIterator(tx, idx, min_keys, max_keys, ownsTransaction, it);
    if (kOk != retCode) return retCode;

    retCode = manager->addTransaction(tx);
    if (kOk != retCode) return retCode;

    retCode = idx->addIterator(*it);
    if (kOk != retCode) return retCode;

    retCode = tx->addIterator(*it);
    if (kOk != retCode) return retCode;

    return manager->getRecords(*it);
}

ErrorCode GetNext(Iterator *it, Record** record)
{
    return ConnectionManager::get().GetNext(it, record);
}

ErrorCode ConnectionManager::GetNext(Iterator *it, Record** record)
{
    if (NULL == it) return kErrorIteratorClosed;

    return it->getNext(record);
}


ErrorCode CloseIterator(Iterator **it)
{
    return ConnectionManager::get().CloseIterator(it);
}

ErrorCode ConnectionManager::CloseIterator(Iterator** it)
{
    if (NULL == *it) return kErrorIteratorClosed;

    (*it)->m_manager->closeIterator(*it, true, true);
    (*it) = NULL;

    return kOk;
}




