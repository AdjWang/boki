#pragma once

#include "base/common.h"
#include "utils/socket.h"
#include "utils/appendable_buffer.h"
#include "utils/round_robin_set.h"
#include "server/io_worker.h"
#include "common/otel_trace.h"

namespace faas {
namespace server {

namespace egress_hub_impl{

class EgressHub : public ConnectionBase {
public:
    EgressHub(int type, const struct sockaddr_in* addr, size_t num_conn);
    virtual ~EgressHub();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    using HandshakeMessageCallback = std::function<void(std::string* /* handshake */)>;
    void SetHandshakeMessageCallback(HandshakeMessageCallback cb);

    void SendMessage(std::span<const char> part1,
                     std::span<const char> part2 = EMPTY_CHAR_SPAN,
                     std::span<const char> part3 = EMPTY_CHAR_SPAN,
                     std::span<const char> part4 = EMPTY_CHAR_SPAN);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    struct sockaddr_in addr_;

    std::vector<int> sockfds_;
    utils::RoundRobinSet</* sockfd */ int> connections_for_pick_;

    HandshakeMessageCallback  handshake_message_cb_;

    std::string log_header_;
    utils::AppendableBuffer write_buffer_;
    bool send_fn_scheduled_;

    void OnSocketConnected(int sockfd, int status);
    void SocketReady(int sockfd);
    void RemoveSocket(int sockfd);
    void ScheduleSendFunction();
    void SendPendingMessages();

    static std::string GetLogHeader(int type);

    DISALLOW_COPY_AND_ASSIGN(EgressHub);
};
}  // namespace egress_hub_impl

// A wrapper to original EgressHub. Add trace context propagation support.
class EgressHub : public egress_hub_impl::EgressHub {
public:
    EgressHub(int type, const struct sockaddr_in* addr, size_t num_conn)
        : egress_hub_impl::EgressHub(type, addr, num_conn) {}

    // void SendMessage(std::span<const char> part1,
    //                  std::span<const char> part2 = EMPTY_CHAR_SPAN,
    //                  std::span<const char> part3 = EMPTY_CHAR_SPAN,
    //                  std::span<const char> part4 = EMPTY_CHAR_SPAN) {
    //     // disallow send without ctx
    //     NOT_IMPLEMENTED();
    // }

    void SendMessage(otel::context& ctx,
                     std::span<const char> part1,
                     std::span<const char> part2 = EMPTY_CHAR_SPAN,
                     std::span<const char> part3 = EMPTY_CHAR_SPAN,
                     std::span<const char> part4 = EMPTY_CHAR_SPAN);

private:
    DISALLOW_COPY_AND_ASSIGN(EgressHub);
};


}  // namespace server
}  // namespace faas
