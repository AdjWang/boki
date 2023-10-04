#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern void test_func();
extern void* ConstructIndexData(uint32_t logspace_id);
extern void DestructIndexData(void* index_data);

extern int ProcessLocalIdQuery(void* index_data,
                               /*InOut*/ uint64_t* metalog_progress,
                               uint64_t localid, /*Out*/ uint64_t* seqnum);
extern int ProcessReadNext(void* index_data,
                           /*InOut*/ uint64_t* metalog_progress,
                           uint32_t user_logspace, uint64_t query_seqnum,
                           uint64_t query_tag, /*Out*/ uint64_t* seqnum);
extern int ProcessReadPrev(void* index_data,
                           /*InOut*/ uint64_t* metalog_progress,
                           uint32_t user_logspace, uint64_t query_seqnum,
                           uint64_t query_tag, /*Out*/ uint64_t* seqnum);

#ifdef __cplusplus
}
#endif