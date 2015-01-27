#include "Transaction.h"

#include "IndexManager.h"
#include "WriteOp.h"
#include "constants.h"

#include <stdio.h>

void Transaction::init()
{
    m_countIterators = 0;
    m_countWriteOps = 0;
    m_indexManager = 0;
}

void Transaction::clear()
{
    for (int i= 0 ;i<m_countIterators; ++i)
        m_indexManager->closeIterator(m_iterators[i], true, false);
    m_countIterators = 0;

    for (int i = 0; i<m_countWriteOps; ++i)
        m_indexManager->m_desc.releaseWriteOp(m_writeOps[i]);
    m_countWriteOps = 0;

}

ErrorCode Transaction::addIterator(Iterator* it)
{
    if (m_countIterators == MAX_IT_TRANS)
    {
        printf("beyond max number of iterators per transaction\n");
        fflush(stdout);
        return kErrorOutOfMemory;
    }

    m_iterators[m_countIterators] = it;
    m_countIterators++;

    return kOk;
}

void Transaction::removeIterator(Iterator* it)
{
    int i = 0;
    while (i<m_countIterators && m_iterators[i]!=it) i++;
    for (int j = i; j< m_countIterators-1;++j) m_iterators[j] = m_iterators[j+1];
    m_countIterators --;
}


ErrorCode Transaction::addWriteOp(const WriteOp& op)
{
    if (m_countWriteOps == MAX_OPS_TRANS)
    {
        printf("beyond max number of write operations per transaction\n");
        fflush(stdout);
        return kErrorOutOfMemory;
    }

    m_writeOps[m_countWriteOps] = op;
    m_countWriteOps++;

    return kOk;
}






