#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define EXPORT __attribute__((visibility("default")))

// DEBUG
EXPORT extern int test_func(uint32_t var_in, uint64_t* var_in_out,
                            uint64_t* var_out);
EXPORT extern void Inspect(void* index_data);

EXPORT extern void Init(const char* ipc_root_path);
EXPORT extern void* ConstructIndexData(uint32_t logspace_id, uint32_t user_logspace);
EXPORT extern void DestructIndexData(void* index_data);

EXPORT extern int IndexReadLocalId(void* index_data,
                                   /*InOut*/ uint64_t* metalog_progress,
                                   uint32_t user_logspace, uint64_t localid,
                                   /*Out*/ uint64_t* seqnum);
EXPORT extern int IndexReadNext(void* index_data,
                                /*InOut*/ uint64_t* metalog_progress,
                                uint32_t user_logspace, uint64_t query_seqnum,
                                uint64_t query_tag, /*Out*/ uint64_t* seqnum);
EXPORT extern int IndexReadPrev(void* index_data,
                                /*InOut*/ uint64_t* metalog_progress,
                                uint32_t user_logspace, uint64_t query_seqnum,
                                uint64_t query_tag, /*Out*/ uint64_t* seqnum);
EXPORT extern int LogReadLocalId(void* index_data, uint64_t metalog_progress,
                                 uint32_t user_logspace, uint64_t localid,
                                 /*Out*/ void* response);
EXPORT extern int LogReadNext(void* index_data, uint64_t metalog_progress,
                              uint32_t user_logspace, uint64_t query_seqnum,
                              uint64_t query_tag, /*Out*/ void* response);
EXPORT extern int LogReadPrev(void* index_data, uint64_t metalog_progress,
                              uint32_t user_logspace, uint64_t query_seqnum,
                              uint64_t query_tag, /*Out*/ void* response);

#ifdef __cplusplus
}
#endif
