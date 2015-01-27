#ifndef _IndexDescription_H
#define _IndexDescription_H

#include "../include/contest_interface.h"
#include <pthread.h>

class WriteOp;
class FixedSizePool;
class Allocator;

union IntAndChar
{
    int64_t intval;
    char* charval;
};
typedef IntAndChar SerialKey;

class Payload
{
public:
    uint64_t m_size;
    char* m_data;

    //pthread_mutex_t m_mutex;
    //uint32_t m_refCount;
    //void addRef() {pthread_mutex_lock(&m_mutex); m_refCount++; pthread_mutex_unlock(&m_mutex);}
};

class QueryPayload
{
public:
    uint64_t m_size;
    char m_data[MAX_PAYLOAD_LENGTH];
};

typedef int (*CompFunction)(IntAndChar, IntAndChar);

class IndexDescription
{
public:
	IndexDescription(KeyType keyType, int attribute_count, int allocindice);

public:
    ErrorCode createOrgaRecord(SerialKey* key, Payload* payload, Record** tofill);

public:
    ErrorCode fillWriteOp(WriteOp& op, int type, const Key& key, Block* payload, Block* new_payload, uint8_t flags);
    void releaseWriteOp(WriteOp& op);

public:
    ErrorCode fillQueryKey(SerialKey* ourkey, const Key& orgakey);
    void clearQueryKey(SerialKey* ourkey);

public:
    ErrorCode createPayload(Payload*& ourpayload, Block* orgapayload);
    //void releasePayload(Payload* payload);

public:
    int compareKeysOnDim(SerialKey* key1, SerialKey* key2, int dim);
    int compareKeys(SerialKey* key1, SerialKey* key2);
    bool keysAreEqual(SerialKey* key1, SerialKey* key2);
    bool payloadsAreEqual(QueryPayload* payload1, Payload* payload2);

public:
    FixedSizePool* getIteratorsPool()
    {
        return m_iteratorsPool;
    }

    FixedSizePool* getHandlesPool()
    {
        return m_handlesPool;
    }

public:
    int m_dimCount;
    int m_allocIndice;
	uint64_t m_keySize;
    uint8_t m_types[32];
    CompFunction m_compares[32];
    Allocator* m_allocator;

    FixedSizePool* m_iteratorsPool;
    FixedSizePool* m_handlesPool;

};
#endif

