#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern void test_func();
extern void* ConstructIndexData(uint32_t logspace_id);
extern void DestructIndexData(void* index_data);
extern bool IndexFindNext(void* index_data, uint8_t direction,
                          uint32_t user_logspace, uint64_t query_seqnum,
                          uint64_t query_tag, uint64_t* seqnum,
                          uint16_t* engine_id);
extern bool IndexFindPrev(void* index_data, uint8_t direction,
                          uint32_t user_logspace, uint64_t query_seqnum,
                          uint64_t query_tag, uint64_t* seqnum,
                          uint16_t* engine_id);
extern bool IndexFindLocalId(void* index_data, uint64_t localid,
                             uint64_t* seqnum);

#ifdef __cplusplus
}
#endif