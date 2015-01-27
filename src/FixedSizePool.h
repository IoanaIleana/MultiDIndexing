#ifndef _FixedSizePool_H
#define _FixedSizePool_H

#include "../include/contest_interface.h"
#include "pthread.h"

class Allocator;

class FixedSizePool
{
public:
    void init(uint32_t chunkSize, int countChunks, int allocindice);
public:
    ErrorCode allocTransientChunk(uint8_t** pchunk);
    void freeChunk(uint8_t* pchunk);

public:
    int m_countChunks;
    uint32_t m_chunkSize;
    int m_first;
    int m_last;
    uint8_t** m_chunks;
    pthread_mutex_t m_mutex;
    Allocator* m_allocator;
};

#endif

