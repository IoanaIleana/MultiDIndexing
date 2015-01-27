#ifndef _Transaction_H
#define _Transaction_H

#include "../include/contest_interface.h"
#include "WriteOp.h"
#include "constants.h"


class IndexManager;
class Iterator;

class Transaction
{

public:
    void init();
    void clear();

public:
    ErrorCode addWriteOp(const WriteOp& op);
    ErrorCode addIterator(Iterator* it);
    void removeIterator(Iterator* it);

public:
    IndexManager* m_indexManager;

public:
    WriteOp m_writeOps[MAX_OPS_TRANS];
    int m_countWriteOps;

    Iterator* m_iterators[MAX_IT_TRANS];
    int m_countIterators;
};

#endif
