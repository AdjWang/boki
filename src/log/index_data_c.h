#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define EXPORT __attribute__((visibility("default")))

EXPORT extern int test_func(uint32_t var_in, uint64_t* var_in_out,
                            uint64_t* var_out);
EXPORT extern void* ConstructIndexData(uint32_t logspace_id);
EXPORT extern void DestructIndexData(void* index_data);

EXPORT extern int ProcessLocalIdQuery(void* index_data,
                                      /*InOut*/ uint64_t* metalog_progress,
                                      uint64_t localid,
                                      /*Out*/ uint64_t* seqnum);
EXPORT extern int ProcessReadNext(void* index_data,
                                  /*InOut*/ uint64_t* metalog_progress,
                                  uint32_t user_logspace, uint64_t query_seqnum,
                                  uint64_t query_tag, /*Out*/ uint64_t* seqnum);
EXPORT extern int ProcessReadPrev(void* index_data,
                                  /*InOut*/ uint64_t* metalog_progress,
                                  uint32_t user_logspace, uint64_t query_seqnum,
                                  uint64_t query_tag, /*Out*/ uint64_t* seqnum);

#ifdef __cplusplus
}
#endif