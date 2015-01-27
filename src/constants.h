#ifndef _constants_H
#define _constants_H

//#define _SIG_NO_NUMA_

//memory
#define MEM_CHUNK_SIZE  1073741824LL

//global pools
#define GLOBAL_TRANS_POOL_SIZE 65
#define GLOBAL_IT_POOL_SIZE 65
#define GLOBAL_HANDLES_POOL_SIZE 65

#define ALLOC_KEY_COUNT 50000

//maximal number of write operations per transaction
#define MAX_OPS_TRANS 64

//maximal number of simultaneously open iterators per transaction
#define MAX_IT_TRANS 256

//maximal number of simultaneously open iterators per index handle
#define MAX_IT_IDX 256

//iterator
#define MAX_ITERATOR_CAP 1000
#define MAX_RESULTS_RANGE 200
#define MAX_RESULTS_POINT 1
#define MAX_RESULTS_RANGE_BIS 1000
#define MAX_RESULTS_POINT_BIS 20
#define MAX_RESULTS_WILDCARD 1000


#define PAGE_SIZE 512

#endif
