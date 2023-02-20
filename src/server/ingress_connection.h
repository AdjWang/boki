#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

// Initialized in virtual ServerBase::OnRemoteMessageConn
// Glue class, receive stream data from fd and remove msg header, give meg body
// to user defined callback function.
class IngressConnection : public ConnectionBase {
public:
    IngressConnection(int type, int sockfd, size_t msghdr_size);
    virtual ~IngressConnection();

    static constexpr size_t kDefaultBufSize  = 65536;

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void set_buffer_group(uint16_t buf_group, size_t buf_size) {
        buf_group_ = buf_group;
        buf_size_  = buf_size;
    }

    // input: data header
    // output: full data size including header
    using MessageFullSizeCallback = std::function<size_t(std::span<const char> /* header */)>;
    void SetMessageFullSizeCallback(MessageFullSizeCallback cb);

    // input: full data (header + body)
    // output: none
    using NewMessageCallback = std::function<void(std::span<const char> /* message */)>;
    void SetNewMessageCallback(NewMessageCallback cb);

    // selectable message handler strategies as MessageFullSizeCallback

    static size_t GatewayMessageFullSizeCallback(std::span<const char> header);
    static NewMessageCallback BuildNewGatewayMessageCallback(
        std::function<void(const protocol::GatewayMessage&,
                           std::span<const char> /* payload */)> cb);

    static size_t SharedLogMessageFullSizeCallback(std::span<const char> header);
    static NewMessageCallback BuildNewSharedLogMessageCallback(
        std::function<void(const protocol::SharedLogMessage&,
                           std::span<const char> /* payload */)> cb);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    // io worker is used to do thread assertation only here
    IOWorker* io_worker_;
    // connection state
    State state_;
    // every ingress connection binds to a sockfd
    int sockfd_;
    // const size of message header, defined in common/protocol.h
    size_t msghdr_size_;

    uint16_t buf_group_;
    size_t   buf_size_;

    MessageFullSizeCallback  message_full_size_cb_;
    NewMessageCallback       new_message_cb_;

    std::string log_header_;
    utils::AppendableBuffer read_buffer_;

    // try to process messages after OnRecvData. can fails if data stream is 
    // not ending
    void ProcessMessages();
    // invoked from io_uring from socket fd
    bool OnRecvData(int status, std::span<const char> data);

    static std::string GetLogHeader(int type, int sockfd);

    DISALLOW_COPY_AND_ASSIGN(IngressConnection);
};

}  // namespace server
}  // namespace faas
