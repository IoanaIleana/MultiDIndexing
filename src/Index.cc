#include "Index.h"

#include "IndexManager.h"

#include <stdio.h>
#include "constants.h"

void Index::init()
{
    m_indexManager = 0;
    m_countIterators = 0;
}

void Index::clear()
{
    for (int i = 0; i<m_countIterators; ++i)
        m_indexManager->closeIterator(m_iterators[i], false, true);
    m_countIterators = 0;
}

ErrorCode Index::addIterator(Iterator* it)
{
    if (m_countIterators == MAX_IT_IDX)
    {
        printf("beyond max number of iterators per index handle\n");
        fflush(stdout);
        return kErrorOutOfMemory;
    }

    m_iterators[m_countIterators] = it;
    m_countIterators++;

    return kOk;
}

void Index::removeIterator(Iterator* it)
{
    int i = 0;
    while (i<m_countIterators && m_iterators[i]!=it) i++;
    for (int j = i; j< m_countIterators-1;++j) m_iterators[j] = m_iterators[j+1];
    m_countIterators --;
}


