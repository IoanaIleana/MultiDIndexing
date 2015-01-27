#ifndef _WriteOp_H
#define _WriteOp_H

#include <string.h>
#include "IndexDescription.h"

enum EWriteOpType
{
    kInsert = 0,
    kDelete = 1,
    kUpdate = 2
};

class WriteOp
{
public:
    SerialKey m_key[32];
    QueryPayload m_queryPayload;
    Payload* m_indexPayload;
    uint8_t m_flags;
    EWriteOpType m_type;
};


#endif
