#ifndef _IndexManager_H
#define _IndexManager_H

#include "../include/contest_interface.h"

#include "IndexDescription.h"

#include <pthread.h>

class Index;
class TransactionManagerBasic;
class Transaction;
class WriteOp;

class IndexManager
{
public:
    IndexManager(uint8_t attribute_count, KeyType type, int indice);
    ~IndexManager();

public:
    ErrorCode deleteIndex();

public:
    ErrorCode addTransaction(Transaction* transaction);
    ErrorCode commitTransaction(Transaction* transaction);
    ErrorCode abortTransaction(Transaction* transaction);
    ErrorCode commitSingleOp(const WriteOp& op);

    ErrorCode createIterator(Transaction* tx, Index* idx, Key min_keys, Key max_keys, bool ownsTransaction, Iterator **it);
    ErrorCode closeIterator(Iterator* it, bool rmFromIndex, bool rmFromTrans);
    ErrorCode getRecords(Iterator *it);

    ErrorCode createIndex(Index** index);
    ErrorCode closeIndex(Index* index);


public:
    static ErrorCode createEmptyTransaction(Transaction** transaction);
    static ErrorCode createEmptyIndexManager(uint8_t attribute_count, KeyType type, int indice, IndexManager** manager);

public:
    IndexDescription m_desc;

private:
    int m_transactionCount;
    int m_indice;
    bool m_isDeleted;
    TransactionManagerBasic* m_transactionManager;
    pthread_mutex_t m_transactions_mutex;
};


#endif
