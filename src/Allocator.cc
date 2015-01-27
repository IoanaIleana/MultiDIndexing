#include "Allocator.h"

#include "constants.h"
#include <sys/mman.h>
#include <numa.h>


void Allocator::init(int allocindice)
{
    m_allocindice = allocindice;
    m_goWithMalloc = 0;

    m_size = MEM_CHUNK_SIZE;
    m_memory = (uint8_t*) mmap(0, MEM_CHUNK_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);

    if ((uint8_t*)-1 == m_memory)
    {
       printf("error: memory map failed\n"); exit(0);
    }

#ifndef _SIG_NO_NUMA_
    numa_tonode_memory(m_memory, m_size, m_allocindice);
#endif

    m_crtPos = m_memory;
    m_maxBound = m_memory + m_size;


    pthread_mutex_init(&m_mutex, NULL);
}


static pthread_once_t allocators_are_initialized = PTHREAD_ONCE_INIT;

Allocator* Allocator::m_allocators[2];


void Allocator::initializeAllocators()
{
#ifndef _SIG_NO_NUMA_
    for (int i = 0; i<2; ++i)
    {
        m_allocators[i] = (Allocator*)numa_alloc_onnode(sizeof(Allocator), i);
        m_allocators[i]->init(i);
    }
#else
    m_allocators[0] = (Allocator*)malloc(sizeof(Allocator));
    m_allocators[0]->init(0);
    m_allocators[1] = 0;
#endif
}

Allocator* Allocator::get(int allocindice)
{
    pthread_once(&allocators_are_initialized, initializeAllocators);
    return m_allocators[allocindice];
}




