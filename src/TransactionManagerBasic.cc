#include "TransactionManagerBasic.h"
#include "Transaction.h"
#include "QueryApplierFilter.h"
#include "IndexManager.h"
#include "Iterator.h"
#include "constants.h"
#include "stdio.h"

extern uint64_t maxDataSize;
extern uint64_t maxPageSizes[32];


TransactionManagerBasic::TransactionManagerBasic(IndexManager* manager)
    :m_indexManager(manager)
{
    m_applier = new QueryApplierFilter(m_indexManager->m_desc);
}

TransactionManagerBasic::~TransactionManagerBasic()
{
    delete m_applier;
}

ErrorCode TransactionManagerBasic::commitWrites(WriteOp* ops, int countOps)
{
    //pthread_rwlock_wrlock(&m_applierLock);

    for (int i = 0; i<countOps; ++i)  m_applier->doWriteOperation(ops[i]);

    //pthread_rwlock_unlock(&m_applierLock);
    return kOk;

}

ErrorCode TransactionManagerBasic::getRecords(Iterator* it)
{
    //pthread_rwlock_rdlock(&m_applierLock);
    m_applier->getRecords(it);
    //pthread_rwlock_unlock(&m_applierLock);

    it->adjustContentsFromIncomingWrites();
    return kOk;
}

