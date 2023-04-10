#pragma once

#include "log/log_space_base.h"

namespace faas {
namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t engine_id;
    uint64_t seqnum;
};

struct IndexQuery {
    // detemines how to interpret the query_seqnum
    // kSync: seqnum of the shared log
    // kAsync: local_id, **would replace with seqnum inplace when querying**
    enum QueryType { kSync, kAsync };

    enum ReadDirection { kReadNext, kReadPrev, kReadNextB, kReadIndex };
    QueryType type;
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool     initial;
    uint64_t client_data;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;

    IndexFoundResult prev_found_result;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
    static IndexQuery SpawnAsSync(const IndexQuery& original_query, uint64_t new_seqnum);
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;

    IndexQuery       original_query;
    IndexFoundResult found_result;
    bool index_only;
};

class Index final : public LogSpaceBase {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    Index(const View* view, uint16_t sequencer_id);
    ~Index();

    void ProvideIndexData(const IndexDataProto& index_data);

    void MakeQuery(const IndexQuery& query);

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
        uint64_t   local_id;
        uint32_t   user_logspace;
        UserTagVec user_tags;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;
    uint32_t data_received_seqnum_position_;
    uint32_t indexed_seqnum_position_;

    // updated when receiving an index, used to serve async log query
    struct AsyncIndexData {
        uint64_t seqnum;
        UserTagVec user_tags;
    };
    std::map</* local_id */ uint64_t, AsyncIndexData> log_index_map_;

    uint64_t index_metalog_progress() const {
        return bits::JoinTwo32(identifier(), indexed_metalog_position_);
    }

    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void OnFinalized(uint32_t metalog_position) override;
    void AdvanceIndexProgress();
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);

    void ProcessQuery(const IndexQuery& query);
    void ProcessReadNext(const IndexQuery& query);
    void ProcessReadPrev(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);
    IndexQueryResult BuildContinueResult(const IndexQuery& query, bool found,
                                         uint64_t seqnum, uint16_t engine_id);

    DISALLOW_COPY_AND_ASSIGN(Index);
};

}  // namespace log
}  // namespace faas
