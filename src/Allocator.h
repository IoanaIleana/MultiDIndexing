#ifndef _Allocator_H
#define _Allocator_H

#include "constants.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/mman.h>

#ifndef _SIG_NO_NUMA_
#include <numa.h>
#endif

class Allocator
{
private:
    void init(int allocindice);

public:
    inline uint8_t* getPage(uint64_t size);

public:
    static Allocator* get(int allocindice);

private:
    int m_allocindice;
    int m_goWithMalloc;
    uint8_t* m_memory;
    uint64_t m_size;
    uint8_t* m_crtPos;
    pthread_mutex_t m_mutex;
    uint8_t* m_maxBound;

private:

    static void initializeAllocators();

private:
    static Allocator* m_allocators[2];
};


inline uint8_t* Allocator::getPage(uint64_t size)
{
    if (m_goWithMalloc)
    {
        uint8_t* chunk = (uint8_t*)malloc(size);
        if (!chunk)
        {
            printf("no more memory available\n"); exit(0);
        }
        return chunk;
    }

    pthread_mutex_lock(&m_mutex);

    uint8_t* prev = m_crtPos;
    m_crtPos+=size;
    if (m_crtPos>m_maxBound)
    {
        //printf("wasted: %lld\n", m_maxBound+size-m_crtPos);
        if ((void*)-1 == (m_memory = (uint8_t*)mmap(0, m_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0)))
        {
           printf("going with malloc\n");
           m_goWithMalloc = 1;
           return getPage(size);
        }

#ifndef _SIG_NO_NUMA_
        numa_tonode_memory(m_memory, m_size, m_allocindice);
#endif
        m_maxBound = m_memory + m_size;

        prev = m_memory;
        m_crtPos = m_memory + size;
    }

    pthread_mutex_unlock(&m_mutex);
    return prev;
}


#endif
