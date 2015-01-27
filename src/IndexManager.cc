#include "IndexManager.h"

#include "Index.h"
#include "Transaction.h"
#include "Iterator.h"
#include "FixedSizePool.h"
#include "TransactionManagerBasic.h"
#include "TransactionsPool.h"
#include "constants.h"

#include <pthread.h>

extern __thread int myNumaNodeForThisThread;

IndexManager::IndexManager(uint8_t attribute_count, KeyType type, int indice)
#ifndef _SIG_NO_NUMA_
    : m_desc(type, attribute_count, indice%2)
#else
    : m_desc(type, attribute_count, 0)
#endif
    , m_indice(indice)
    , m_transactionCount(0)
    , m_isDeleted(false)
    , m_transactionManager (new TransactionManagerBasic(this))
{
    pthread_mutex_init(&m_transactions_mutex, NULL);

}

IndexManager::~IndexManager()
{
    if (NULL != m_transactionManager)
    {
        delete m_transactionManager;
        m_transactionManager = NULL;
    }
}

ErrorCode IndexManager::deleteIndex()
{
    if (m_isDeleted) return kErrorUnknownIndex;

    pthread_mutex_lock(&m_transactions_mutex); //from now on no new transactions may be registered
    if (m_transactionCount)
    {
        pthread_mutex_unlock(&m_transactions_mutex);
        return kErrorOpenTransactions;
    }
    m_isDeleted = true;
    pthread_mutex_unlock(&m_transactions_mutex);

    delete m_transactionManager;
    m_transactionManager = NULL;

    return kOk;
}


ErrorCode IndexManager::addTransaction(Transaction* tx)
{
    if (m_isDeleted) return kErrorUnknownIndex;
    if (tx->m_indexManager) return kOk;

    pthread_mutex_lock(&m_transactions_mutex);
    tx->m_indexManager = this;
    m_transactionCount++;
    pthread_mutex_unlock(&m_transactions_mutex);

    return kOk;
}

ErrorCode IndexManager::commitTransaction(Transaction* transaction)
{
    m_transactionManager->commitWrites(transaction->m_writeOps, transaction->m_countWriteOps);
    pthread_mutex_lock(&m_transactions_mutex);
    m_transactionCount--;
    pthread_mutex_unlock(&m_transactions_mutex);

    transaction->clear();
    TransactionsPool::getPool(myNumaNodeForThisThread)->freeChunk((uint8_t*)transaction);

    return kOk;
}

ErrorCode IndexManager::commitSingleOp(const WriteOp& op)
{
    m_transactionManager->commitWrites((WriteOp*)&op, 1);

    return kOk;
}

ErrorCode IndexManager::abortTransaction(Transaction* transaction)
{
    pthread_mutex_lock(&m_transactions_mutex);
    m_transactionCount--;
    pthread_mutex_unlock(&m_transactions_mutex);

    transaction->clear();
    TransactionsPool::getPool(myNumaNodeForThisThread)->freeChunk((uint8_t*)transaction);

    return kOk;
}

ErrorCode IndexManager::getRecords(Iterator *it)
{
    return m_transactionManager->getRecords(it);
}

ErrorCode IndexManager::createIterator(Transaction* tx, Index* idx, Key min_keys, Key max_keys, bool ownsTransaction, Iterator **it)
{
    ErrorCode retCode = m_desc.getIteratorsPool()->allocTransientChunk((uint8_t**)it);
    if (kOk != retCode) return retCode;

    return (*it)->init(tx, idx, min_keys, max_keys, ownsTransaction);
}

ErrorCode IndexManager::closeIterator(Iterator* it, bool removeFromIndex, bool removeFromTransaction)
{
    if (removeFromIndex) it->m_indexHandle->removeIterator(it);
    if (removeFromTransaction) it->m_transaction->removeIterator(it);

    if (it->m_ownsTransaction) commitTransaction(it->m_transaction);
    it->clear();
    m_desc.getIteratorsPool()->freeChunk((uint8_t*)it);

    return kOk;
}

ErrorCode IndexManager::createIndex(Index** index)
{
    ErrorCode retCode = m_desc.getHandlesPool()->allocTransientChunk((uint8_t**)index);
    if (kOk != retCode) return retCode;

    (*index)->init();
    (*index)->m_indexManager = this;

    return kOk;
}

ErrorCode IndexManager::closeIndex(Index* index)
{
    index->clear();

    m_desc.getHandlesPool()->freeChunk((uint8_t*)index);

    return kOk;
}


ErrorCode IndexManager::createEmptyTransaction(Transaction** transaction)
{
    ErrorCode retCode = TransactionsPool::getPool(myNumaNodeForThisThread)->allocTransientChunk((uint8_t**)transaction);
    if (kOk != retCode) return retCode;

    (*transaction) ->init();
    return kOk;
}


ErrorCode IndexManager::createEmptyIndexManager(uint8_t attribute_count, KeyType type, int indice, IndexManager** manager)
{
    return ((*manager) = new IndexManager(attribute_count, type, indice))? kOk:kErrorOutOfMemory;
}




