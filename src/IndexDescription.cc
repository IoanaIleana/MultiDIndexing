#include "IndexDescription.h"

#include "FixedSizePool.h"
#include "constants.h"
#include "WriteOp.h"
#include "Iterator.h"
#include "Index.h"
#include "Allocator.h"

#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>


Attribute *getCopyShortAttribute(int32_t value)
{
    Attribute *attribute = (Attribute*) malloc(sizeof(Attribute));
    attribute->type = kShort;
    attribute->short_value = value;
    return attribute;
}

Attribute *getCopyIntAttribute(int64_t value)
{
    Attribute *attribute = (Attribute*) malloc(sizeof(Attribute));
    attribute->type = kInt;
    attribute->int_value = value;
    return attribute;
}

Attribute *getCopyVarcharAttribute(char* value)
{
    Attribute *attribute = (Attribute*) malloc(sizeof(Attribute));
    attribute->type = kVarchar;
    strcpy(attribute->char_value, value);
    return attribute;
}

ErrorCode IndexDescription::fillWriteOp(WriteOp& op, int type, const Key& key, Block* payload, Block* new_payload, uint8_t flags)
{
    op.m_type = (EWriteOpType)type;
    op.m_flags = flags;
    op.m_indexPayload = 0;
    op.m_queryPayload.m_size = 0;

    ErrorCode retCode = fillQueryKey(op.m_key, key);
    if (kOk!= retCode) return retCode;

    switch (op.m_type)
    {
        case kDelete:
        {
            op.m_queryPayload.m_size = payload->size;
            memcpy(op.m_queryPayload.m_data, payload->data, payload->size);
            return kOk;
            break;
        }
        case kInsert:
        {
            retCode = createPayload(op.m_indexPayload, payload);
            return retCode;
            break;
        }

        case kUpdate:
        {
            op.m_queryPayload.m_size = payload->size;
            memcpy(op.m_queryPayload.m_data, payload->data, payload->size);

            retCode = createPayload(op.m_indexPayload, new_payload);
            return retCode;
            break;
        }
    }

    return kOk;
}


void IndexDescription::releaseWriteOp(WriteOp& op)
{
    if (op.m_type!= kInsert)
        clearQueryKey(op.m_key);

    //if (op.m_payload) releasePayload(op.m_payload);

    //if (op.m_newPayload) releasePayload(op.m_newPayload);
}

ErrorCode IndexDescription::createOrgaRecord(SerialKey* key, Payload* payload, Record** tofill)
{
    Record* newRec = (Record*) malloc(sizeof(Record));
    newRec->key.attribute_count = m_dimCount;
    newRec->key.value = (Attribute**) malloc(m_dimCount*sizeof(Attribute*));

    for (int i=0; i<m_dimCount; i++) {
        switch(m_types[i])
        {
            case 0: newRec->key.value[i] = getCopyIntAttribute(key[i].intval); break;
            case 1: newRec->key.value[i] = getCopyShortAttribute(key[i].intval); break;
            case 2: newRec->key.value[i] = getCopyVarcharAttribute(key[i].charval); break;
        }
    }

    newRec->payload.size = payload->m_size;
    newRec->payload.data = (char*) malloc (payload->m_size);
    memcpy(newRec->payload.data, payload->m_data, payload->m_size);

    *tofill = newRec;
    return kOk; //check for out of memory
}

static int compareInts(IntAndChar int1, IntAndChar int2)
{
    if (int1.intval<int2.intval) return -1;
    if (int1.intval>int2.intval) return 1;
    return 0;
}

static int compareChars(IntAndChar int1, IntAndChar int2)
{
    return strcmp(int1.charval, int2.charval);
}


IndexDescription::IndexDescription(KeyType keyType, int attribute_count, int allocindice)
    : m_dimCount(attribute_count)
    , m_allocIndice(allocindice)
    , m_keySize(m_dimCount*sizeof(IntAndChar))
{
    for (int i=0; i<m_dimCount; i++) {
        switch(keyType[i])
        {
            case kInt:m_types[i]=0;  m_compares[i] = compareInts; break;
            case kShort:  m_types[i]=1; m_compares[i] = compareInts; break;
            case kVarchar: m_types[i] = 2; m_compares[i] = compareChars; break;
        }
    }

    m_allocator = Allocator::get(m_allocIndice);

    m_iteratorsPool = new FixedSizePool();
    m_iteratorsPool->init(sizeof(Iterator), GLOBAL_IT_POOL_SIZE, m_allocIndice);


    m_handlesPool = new FixedSizePool();
    m_handlesPool->init(sizeof(Index), GLOBAL_HANDLES_POOL_SIZE, m_allocIndice);

};




ErrorCode IndexDescription::fillQueryKey(SerialKey* ourkey, const Key& orgakey)
{
    for (int i=0; i<m_dimCount; i++)
    {
        if (orgakey.value[i] == NULL)
        {
            ourkey[i].intval = 0;
            continue;
        }

        switch(m_types[i])
        {
            case 0:
            {
                if (orgakey.value[i]->type!=kInt) return kErrorIncompatibleKey;
                ourkey[i].intval = orgakey.value[i]->int_value;
                break;
            }
            case 1:
            {
                if (orgakey.value[i]->type!=kShort) return kErrorIncompatibleKey;
                ourkey[i].intval = orgakey.value[i]->short_value;
                break;
            }
            case 2:
            {
                 if (orgakey.value[i]->type!=kVarchar) return kErrorIncompatibleKey;
                 int varCharSize = strlen(orgakey.value[i]->char_value)+1;
                 ourkey[i].charval = (char*)malloc(varCharSize);
                 memcpy(ourkey[i].charval,orgakey.value[i]->char_value, varCharSize);
            }
        }
    }
    return kOk;
}

void IndexDescription::clearQueryKey(SerialKey* key)
{
    for (int i=0; i<m_dimCount; i++) if (m_types[i] == 2) //varchar
    {
        free(key[i].charval);
    }
}

ErrorCode IndexDescription::createPayload(Payload*& ourpayload, Block* orgapayload)
{
    ourpayload = (Payload*)m_allocator->getPage(sizeof(Payload));

    ourpayload->m_data = (char*)malloc(orgapayload->size);
    ourpayload->m_size = orgapayload->size;
    memcpy(ourpayload->m_data, orgapayload->data, orgapayload->size);

    //ourpayload->m_refCount = 1;
    //pthread_mutex_init(&ourpayload->m_mutex, NULL);

    return kOk;
}


/*
void IndexDescription::releasePayload(Payload* ourpayload)
{
    pthread_mutex_lock(&ourpayload->m_mutex);
    ourpayload->m_refCount --;
    if (!ourpayload->m_refCount)
    {
        FixedSizePool& chunksPool = GlobalAllocators::getByteChunkPool(ourpayload->m_size);
        chunksPool.freeChunk((uint8_t*)ourpayload->m_data);
        ourpayload->m_size = 0;

        FixedSizePool& payloadsPool = GlobalAllocators::getPayloadsPool();
        payloadsPool.freeChunk((uint8_t*)ourpayload);
    }
    pthread_mutex_unlock(&ourpayload->m_mutex);
}
*/

int IndexDescription::compareKeysOnDim(SerialKey* key1, SerialKey* key2, int dim)
{
    return m_compares[dim](key1[dim], key2[dim]);
}

int IndexDescription::compareKeys(SerialKey* key1, SerialKey* key2)
{
    int r = 0;
    for (int i = 0; i< m_dimCount; ++i) if ((r = compareKeysOnDim(key1, key2, i))) return r;
    return 0;
}

bool IndexDescription::keysAreEqual(SerialKey* key1, SerialKey* key2)
{
    return compareKeys(key1, key2) == 0;
}

bool IndexDescription::payloadsAreEqual(QueryPayload* payload1, Payload* payload2)
{
    if (payload1->m_size != payload2->m_size) return false;
    return memcmp(payload1->m_data, payload2->m_data, payload1->m_size) == 0;
}









