#pragma once

#include "log/common.h"

namespace faas {
namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t engine_id;
    uint64_t seqnum;
};

struct IndexQuery {
    // determines how to response
    // kSync: return once with the log entry
    // kAsync: return twice, first only seqnum, second the same as kSync
    enum QueryType { kSync, kAsync };

    // determines how to interpret the query_seqnum
    // kReadNext, kReadPrev, kReadNextB: query_seqnum is the seqnum of the shared log
    // kReadLocalId: query_seqnum is the local_id
    enum ReadDirection { kReadNext, kReadPrev, kReadNextB, kReadLocalId };
    QueryType type;
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool     initial;
    uint64_t client_data;
    bool     metalog_inside;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;

    IndexFoundResult prev_found_result;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;

    IndexQuery       original_query;
    IndexFoundResult found_result;
};

}
}
