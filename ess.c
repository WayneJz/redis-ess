#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <ctype.h>

#include "redismodule.h"

// Check if two strings are case-sensitively the same 
bool IsSameCSString(const char* a, const char* b) {
    size_t la = strlen(a);
    size_t lb = strlen(b);

    if (la != lb)
        return false;
    return strncmp(a, b, la) == 0;
}

// Check if two strings are case-insensitively the same 
bool IsSameString(const char* a, const char* b) {
    size_t la = strlen(a);
    size_t lb = strlen(b);

    if (la != lb)
        return false;
    return strncasecmp(a, b, la) == 0;
}

// Incr command: extend standard 'INCR' command with expire options
// INCR key [expiration EX seconds|PX milliseconds]
int Incr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long expr = 0;

    if (argc != 2 && argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    if (argc == 4) {
        size_t ptrlen;

        if (IsSameString(RedisModule_StringPtrLen(argv[2], &ptrlen), "EX")) {
            if (RedisModule_StringToLongLong(argv[3], &expr) != REDISMODULE_OK) {
                return RedisModule_ReplyWithError(ctx, "ERR 'EX' value is not an integer or out of range");
            }
            expr *= 1000;
        } else if (IsSameString(RedisModule_StringPtrLen(argv[2], &ptrlen), "PX")) {
            if (RedisModule_StringToLongLong(argv[3], &expr) != REDISMODULE_OK) {
                return RedisModule_ReplyWithError(ctx, "ERR 'EX' value is not an integer or out of range");
            }
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR wrong argument for 'INCR' command, should be 'EX' or 'PX'");
        }
    }

    RedisModuleKey* key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);

    switch (RedisModule_KeyType(key))
    {
    case REDISMODULE_KEYTYPE_EMPTY: {
        RedisModule_StringSet(key, RedisModule_CreateStringFromLongLong(ctx, 1));
        if (expr > 0)
            RedisModule_SetExpire(key, expr);
        return RedisModule_ReplyWithLongLong(ctx, 1);
    }

    case REDISMODULE_KEYTYPE_STRING: {
        size_t ptrlen;
        char* raw = RedisModule_StringDMA(key, &ptrlen, REDISMODULE_READ);
        long long num = atoll(raw);

        if (num == 0 && !(ptrlen == 1 && raw[0] == '0'))
            return RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");

        num++;
        RedisModule_StringSet(key, RedisModule_CreateStringFromLongLong(ctx, num));
        if (expr > 0)
            RedisModule_SetExpire(key, expr);
        return RedisModule_ReplyWithLongLong(ctx, num);
    }
    
    default:
        return RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}

// Module load functor
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "redispro", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    RedisModule_Log(ctx, "warning", "module redis pro initialized");

    if (RedisModule_CreateCommand(ctx, "redispro.incr", Incr_RedisCommand, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}