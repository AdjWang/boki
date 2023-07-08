#pragma once

#include "log/common.h"

// Forward declarations
__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
#include <tkrzw_dbm.h>
#include <tkrzw_thread_util.h>
__END_THIRD_PARTY_HEADERS

namespace tkrzw {
class CacheDBMUpdateLogger final : public DBM::UpdateLogger {
   public:
    /**
     * Enumeration for operation types.
     */
    enum OpType : int32_t {
        /** Invalid operation. */
        OP_VOID = 0,
        /** To modify or add a record. */
        OP_SET = 1,
        /** To remove a record. */
        OP_REMOVE = 2,
        /** To remove all records. */
        OP_CLEAR = 3,
    };
    static std::string OpTypeToString(OpType t) {
        switch (t) {
            case OP_VOID:
                return "OP_VOID";
            case OP_SET:
                return "OP_SET";
            case OP_REMOVE:
                return "OP_REMOVE";
            case OP_CLEAR:
                return "OP_CLEAR";
            default:
                return "OP_UNKNOWN";
        }
    }

    explicit CacheDBMUpdateLogger() {}
    virtual ~CacheDBMUpdateLogger() {}

    Status WriteSet(std::string_view key, std::string_view value) override;
    Status WriteRemove(std::string_view key) override;
    Status WriteClear() override;

    std::vector<std::pair<OpType, std::string /*key*/>> PopUpdates();

   private:
    SpinMutex mutex_;
    std::vector<std::pair<OpType, std::string /*key*/>> ops_;

    DISALLOW_COPY_AND_ASSIGN(CacheDBMUpdateLogger);
};
}  // namespace tkrzw

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    ~LRUCache();

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint32_t user_logspace, uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint32_t user_logspace, uint64_t seqnum);

    struct IndexUpdate {
        tkrzw::CacheDBMUpdateLogger::OpType op_type;
        uint32_t user_logspace;
        uint64_t seqnum;
        IndexUpdate(tkrzw::CacheDBMUpdateLogger::OpType op_type,
                    uint32_t user_logspace, uint64_t seqnum)
            : op_type(op_type), user_logspace(user_logspace), seqnum(seqnum) {}
    };
    std::optional<std::vector<IndexUpdate>> PopAuxUpdates();

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;
    tkrzw::CacheDBMUpdateLogger* ulogger_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

// DEPRECATED: make capacity unified among shareds
// class ShardedLRUCache {
// public:
//     void PutAuxData(uint64_t tag, uint64_t seqnum, std::span<const char> data);
//     std::optional<std::string> GetAuxData(uint64_t tag, uint64_t seqnum);
//     std::optional<std::pair<std::uint64_t, std::string>> GetPrevAuxData(uint64_t tag);
// 
// private:
//     // DEPRECATED
//     explicit ShardedLRUCache(int mem_cap_mb);
//     ~ShardedLRUCache();
// 
//     std::string log_header_;
//     int mem_cap_mb_;
// 
//     absl::Mutex mu_;
//     absl::flat_hash_map</* tag */ uint64_t,
//                         std::unique_ptr<tkrzw::CacheDBM>>
//         dbs_ ABSL_GUARDED_BY(mu_);
// 
//     tkrzw::CacheDBM* GetDBM(uint64_t tag);
//     tkrzw::CacheDBM* GetOrCreateDBM(uint64_t tag);
// 
//     DISALLOW_COPY_AND_ASSIGN(ShardedLRUCache);
// };

}  // namespace log
}  // namespace faas
