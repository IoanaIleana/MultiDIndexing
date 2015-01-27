#include "TransactionsPool.h"
#include "Transaction.h"
#include "constants.h"
#include "FixedSizePool.h"

#include <pthread.h>
#include <stdlib.h>

#ifndef _SIG_NO_NUMA_
#include <numa.h>
#endif

static pthread_once_t trans_pools_are_initialized = PTHREAD_ONCE_INIT;

FixedSizePool* TransactionsPool::m_pools[2];

void TransactionsPool::initializePools()
{
#ifndef _SIG_NO_NUMA_
    for (int i = 0; i<2; ++i)
    {
        m_pools[i] = (FixedSizePool*)numa_alloc_onnode(sizeof(FixedSizePool), i);
        m_pools[i]->init(sizeof(Transaction), GLOBAL_TRANS_POOL_SIZE, i);
    }
#else
    m_pools[0] = (FixedSizePool*)malloc(sizeof(FixedSizePool));
    m_pools[0]->init(sizeof(Transaction), GLOBAL_TRANS_POOL_SIZE, 0);
    m_pools[1] =0;
#endif
}

FixedSizePool* TransactionsPool::getPool(int allocindice)
{
     pthread_once(&trans_pools_are_initialized, initializePools);
     return m_pools[allocindice];
}
