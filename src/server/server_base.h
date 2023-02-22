#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "server/node_watcher.h"
#include "server/timer.h"

namespace faas {
namespace server {

class ServerBase {
public:
    static constexpr size_t kDefaultIOWorkerBufferSize = 65536;

    explicit ServerBase(std::string_view node_name);
    virtual ~ServerBase();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

protected:
    enum State { kCreated, kBootstrapping, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    // zookeeper
    zk::ZKSession* zk_session() { return &zk_session_; }
    NodeWatcher* node_watcher() { return &node_watcher_; }

    // thread identify
    bool WithinMyEventLoopThread() const;

    // io worker
    void ForEachIOWorker(std::function<void(IOWorker* io_worker)> cb) const;
    // round-robin pick a worker for a specific conn type
    // similar to SomeIOWorker()
    IOWorker* PickIOWorkerForConnType(int conn_type);
    // round-robin pick a worker
    // similar to PickIOWorkerForConnType(...)
    IOWorker* SomeIOWorker() const;

    static IOWorker* CurrentIOWorker() { return IOWorker::current(); }
    static IOWorker* CurrentIOWorkerChecked() { return DCHECK_NOTNULL(IOWorker::current()); }

    // connections listened by IOWorker
    // only listen client connections(sockfd), which is generated from server sockfd by accept4()
    void RegisterConnection(IOWorker* io_worker, ConnectionBase* connection);

    // connections listened by ServerBase itself, using poll()
    // only listen server connections
    using ConnectionCallback = std::function<void(int /* client_sockfd */)>;
    void ListenForNewConnections(int server_sockfd, ConnectionCallback cb);

    // timer
    Timer* CreateTimer(int timer_type, IOWorker* io_worker, Timer::Callback cb);
    // for each IOWorker. Load balance to all io workers by set interval to
    // interval*io_workers_.size() .
    void CreatePeriodicTimer(int timer_type, absl::Duration interval, Timer::Callback cb);

    // interfaces exposed to inherited classes
    // Supposed to be implemented by sub-class
    virtual void StartInternal() = 0;
    virtual void StopInternal() = 0;
    virtual void OnConnectionClose(ConnectionBase* connection) = 0;
    virtual void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                     int sockfd) = 0;

    // utils of conn interfaces
    static int GetIngressConnTypeId(protocol::ConnType conn_type, uint16_t node_id);
    static int GetEgressHubTypeId(protocol::ConnType conn_type, uint16_t node_id);

private:
    std::string node_name_;

    int stop_eventfd_;
    int message_sockfd_;
    base::Thread event_loop_thread_;
    zk::ZKSession zk_session_;
    NodeWatcher node_watcher_;

    // used in SomeIOWorker()
    // a counter for workers, pick a worker in round-robin manner
    // similar to next_io_worker_id_
    mutable std::atomic<size_t> next_io_worker_for_pick_;

    // initialized in SetupIOWorkers()
    // fixed number of io workers, set by FLAGS_num_io_workers
    std::vector<std::unique_ptr<IOWorker>> io_workers_;

    // initialized in SetupIOWorkers()
    // each io worker binds to a socketpair:
    // - pair[0] to pipes_to_io_worker_
    // - pair[1] to io_worker_->Start(...)
    absl::flat_hash_map<IOWorker*, /* fd */ int> pipes_to_io_worker_;
    
    // registered by ListenForNewConnections(...)
    // callbacks to invoke after a new connection on that fd is accepted
    absl::flat_hash_map</* fd */ int, ConnectionCallback> connection_cbs_;

    // used in PickIOWorkerForConnType()
    // a counter for conns, inc per call on PickIOWorkerForConnType() to round
    // picking a worker in io_workers_[]
    // each conn picks all io workers monotonically
    // similar to next_io_worker_for_pick_
    absl::flat_hash_map</* conn_type */ int, size_t> next_io_worker_id_;
    std::atomic<int> next_connection_id_;
    absl::flat_hash_set<std::unique_ptr<Timer>> timers_;

    void SetupIOWorkers();
    void SetupMessageServer();
    void OnNewMessageConnection(int sockfd);

    // use poll to listen to:
    // - stop_eventfd_
    // - pipes_to_io_worker_[].second: fd
    // - connection_cbs_[].first: fd
    void EventLoopThreadMain();
    void DoStop();
    void DoReadClosedConnection(int pipefd);
    void DoAcceptConnection(int server_sockfd);

    DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

}  // namespace server
}  // namespace faas
