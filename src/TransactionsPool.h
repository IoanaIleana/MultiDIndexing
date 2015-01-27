#ifndef _TransactionsPool_H
#define _TransactionsPool_H

class FixedSizePool;

class TransactionsPool
{
public:
    static FixedSizePool* getPool(int allocindice);
private:
    static void initializePools();
public:
    static FixedSizePool* m_pools[2];
};

#endif
