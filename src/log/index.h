#pragma once

#include "log/log_space_base.h"
#include "log/cache.h"

namespace faas {
namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t engine_id;
    uint64_t seqnum;
    uint64_t localid;
    size_t log_index;
};

struct IndexQuery {
    static constexpr uint16_t kReadLocalIdFlag   = (1 << 1);     // Indicates localid/seqnum

    // determines how to response
    // kSync: return once with the log entry
    // kAsync: return twice, first only seqnum, second the same as kSync
    enum QueryType { kSync, kAsync };

    // determines how to interpret the query_seqnum
    // kReadNext, kReadPrev, kReadNextB: query_seqnum is the seqnum of the shared log
    // kReadLocalId: query_seqnum is the localid
    // kReadNextU(ntil): both, use flags to resolve
    enum ReadDirection { kReadNext, kReadPrev, kReadPrevAux, kReadNextB, kReadLocalId, kReadNextU };
    QueryType type;
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool     initial;
    uint64_t client_data;
    // For ReadWithAux, perform aux read first
    // if found aux data, then use it as the final auxdata
    // if not found aux data, fallback to normal read
    std::optional<AuxEntry> promised_auxdata;

    uint32_t user_logspace;
    uint64_t user_tag;
    union {
        uint64_t query_localid;
        uint64_t query_seqnum;
    };
    uint64_t query_start_seqnum;    // Used by kReadNextU
    uint64_t metalog_progress;
    uint16_t flags;

    IndexFoundResult prev_found_result;
    uint64_t next_result_id;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kViewContinue, kAuxContinue, kEOF };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;
    // Continuous sequence number for a group of results.
    // Used to reorder in batch return paradigm.
    uint64_t id;

    IndexQuery       original_query;
    IndexFoundResult found_result;
};

class DebugQueryResultVec final : public absl::InlinedVector<IndexQueryResult, 4> {
public:
    void push_back(const IndexQueryResult& v) {
        // DEBUG
        DCHECK(v.original_query.origin_node_id != 28524) << utils::DumpStackTrace();
        static_cast<void>(emplace_back(v));
    }

    void push_back(IndexQueryResult&& v) {
        // DEBUG
        DCHECK(v.original_query.origin_node_id != 28524) << utils::DumpStackTrace();
        static_cast<void>(emplace_back(std::move(v)));
    }
};

class Index final : public LogSpaceBase {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    Index(const View* view, uint16_t sequencer_id);
    ~Index();

    void ProvideIndexData(const IndexDataProto& index_data);

    void MakeQuery(const IndexQuery& query);

    // DEBUG
    // using QueryResultVec = DebugQueryResultVec;
    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;

    void PollQueryResults(QueryResultVec* results);

private:
    class PerSpaceIndex;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;

    static constexpr uint32_t kMaxMetalogPosition = std::numeric_limits<uint32_t>::max();

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    std::vector<std::pair</* start_timestamp */ int64_t,
                          IndexQuery>> blocking_reads_;
    QueryResultVec pending_query_results_;

    std::deque<std::pair</* metalog_seqnum */ uint32_t,
                         /* end_seqnum */ uint32_t>> cuts_;
    uint32_t indexed_metalog_position_;

    struct IndexData {
        uint64_t   localid;
        uint32_t   user_logspace;
        UserTagVec user_tags;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;
    uint32_t data_received_seqnum_position_;
    uint32_t indexed_seqnum_position_;

    std::vector<IndexQuery> pending_syncto_queries_;
    // updated when receiving an index, used to serve async log query
    struct AsyncIndexData {
        uint64_t seqnum;
        UserTagVec user_tags;
    };
    absl::flat_hash_map</* localid */ uint64_t, AsyncIndexData> log_index_map_;

    uint64_t index_metalog_progress() const {
        return bits::JoinTwo32(identifier(), indexed_metalog_position_);
    }

    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void OnFinalized(uint32_t metalog_position) override;
    void AdvanceIndexProgress();
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);

    bool ProcessLocalIdQuery(const IndexQuery& query);
    void ProcessQuery(const IndexQuery& query);
    void ProcessReadNext(const IndexQuery& query);
    void ProcessReadPrev(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    void ProcessReadNextUntil(const IndexQuery& query);
    void ProcessReadNextUntilInitial(const IndexQuery& query);
    void ProcessReadNextUntilContinue(const IndexQuery& query);

    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id, uint64_t* localid);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id, uint64_t* localid);

    IndexQuery BuildContinueQuery(const IndexQuery& query, bool found,
                                  size_t index, uint64_t seqnum, uint16_t engine_id,
                                  uint64_t localid, uint64_t next_result_id);
    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id, uint64_t localid, uint64_t result_id=0);
    IndexQueryResult BuildResolvedResult(const IndexQuery& query, uint64_t result_id=0);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query, uint64_t result_id=0);
    IndexQueryResult BuildAuxContinueResult(const IndexQuery& query, uint64_t query_seqnum);
    IndexQueryResult BuildViewContinueResult(const IndexQuery& query, bool found,
                                             uint64_t seqnum, uint16_t engine_id, uint64_t localid, uint64_t result_id=0);

    DISALLOW_COPY_AND_ASSIGN(Index);
};

}  // namespace log
}  // namespace faas

template <>
struct fmt::formatter<faas::log::IndexQuery::ReadDirection>: formatter<std::string_view> {
    auto format(faas::log::IndexQuery::ReadDirection dir, format_context& ctx) const {
        std::string_view result = "unknown";
        switch (dir) {
            case faas::log::IndexQuery::ReadDirection::kReadNext:
                result = "kReadNext";
                break;
            case faas::log::IndexQuery::ReadDirection::kReadPrev:
                result = "kReadPrev";
                break;
            case faas::log::IndexQuery::ReadDirection::kReadPrevAux:
                result = "kReadPrevAux";
                break;
            case faas::log::IndexQuery::ReadDirection::kReadNextB:
                result = "kReadNextB";
                break;
            case faas::log::IndexQuery::ReadDirection::kReadNextU:
                result = "kReadNextU";
                break;
            case faas::log::IndexQuery::ReadDirection::kReadLocalId:
                result = "kReadLocalId";
                break;
        }
        return formatter<string_view>::format(result, ctx);
    }
};
