#include "FixedSizePool.h"
#include "stdlib.h"
#include "memory.h"
#include "stdio.h"
#include "Allocator.h"
#include "constants.h"


void FixedSizePool::init(uint32_t chunkSize, int countChunks, int allocindice)
{
    m_allocator = Allocator::get(allocindice);
    m_countChunks = countChunks;
    m_chunkSize = chunkSize;
    m_first = 0;
    m_last = 0;
    pthread_mutex_init(&m_mutex, NULL);

    m_chunks = (uint8_t**)m_allocator->getPage(m_countChunks* sizeof(uint8_t*));

    for (int i = 0; i<m_countChunks; ++i)
        m_chunks[i] = m_allocator->getPage(m_chunkSize);

}

ErrorCode FixedSizePool::allocTransientChunk(uint8_t** pchunk)
{
    pthread_mutex_lock(&m_mutex);
    if (m_first < m_countChunks)
    {
        *pchunk = m_chunks[m_first];
        m_first++;
        pthread_mutex_unlock(&m_mutex);
        return kOk;
    }
    pthread_mutex_unlock(&m_mutex);

    *pchunk = m_allocator->getPage(m_chunkSize);
    if (NULL == *pchunk) return kErrorOutOfMemory;

    return kOk;
}

void FixedSizePool::freeChunk(uint8_t* pchunk)
{
    pthread_mutex_lock(&m_mutex);
    if (m_first > 0)
    {
        m_first--;
        m_chunks[m_first] = pchunk;
    }
    pthread_mutex_unlock(&m_mutex);
}

