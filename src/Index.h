#ifndef _Index_H
#define _Index_H

#include "../include/contest_interface.h"
#include "constants.h"

class IndexManager;
class Iterator;

struct Index
{

public:
    void init();
    void clear();

public:
    ErrorCode addIterator(Iterator* iterator);
    void removeIterator(Iterator* it);

public:
    IndexManager* m_indexManager;

public:
    Iterator* m_iterators[MAX_IT_IDX];
    int m_countIterators;
};


#endif
