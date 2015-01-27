#ifndef _TransactionManagerBasic_H
#define _TransactionManagerBasic_H

#include <pthread.h>
#include "../include/contest_interface.h"

class IndexManager;
class QueryApplierFilter;
class WriteOp;
class Iterator;

class TransactionManagerBasic
{
public:
    TransactionManagerBasic(IndexManager* manager);
    ~TransactionManagerBasic();

public:
    ErrorCode commitWrites(WriteOp* ops, int countOps);
    ErrorCode getRecords(Iterator* it);

private:
    QueryApplierFilter* m_applier;
    //pthread_rwlock_t m_applierLock;
    IndexManager* m_indexManager;
};

#endif
