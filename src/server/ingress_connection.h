#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "common/otel_trace.h"

namespace faas {
namespace server {

namespace ingress_conn_impl {

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
    // static functions are selected by caller by conn type, set to Set...Callback(cb)

    // default gateway msg full size callback
    static size_t GatewayMessageFullSizeCallback(std::span<const char> header);
    // new meg callback wrapper, separate msg header and payload then pass to
    // the callback.
    static NewMessageCallback BuildNewGatewayMessageCallback(
        std::function<void(const protocol::GatewayMessage&,
                           std::span<const char> /* payload */)> cb);

    // default shared log msg full size callback
    static size_t SharedLogMessageFullSizeCallback(std::span<const char> header);
    // new meg callback wrapper, separate msg header and payload then pass to
    // the callback.
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

}   // namespace ingress_conn_impl

// A wrapper to original IngressConnection. Add trace context propagation support.
class IngressConnection : public ingress_conn_impl::IngressConnection {
public:
    // since all message are wrapped by ctx, just discard original msghdr_size
    IngressConnection(int type, int sockfd, size_t msghdr_size)
        : ingress_conn_impl::IngressConnection(type, sockfd, sizeof(protocol::TraceCtxMessage)) {}

    // default msg full size callback
    static size_t GatewayMessageFullSizeCallback(std::span<const char> header) {
        return MessageFullSizeCallback(header);
    }
    static size_t SharedLogMessageFullSizeCallback(std::span<const char> header) {
        return MessageFullSizeCallback(header);
    }

    static NewMessageCallback BuildNewGatewayMessageCallback(
        std::function<void(otel::context&,
                           const protocol::GatewayMessage&,
                           std::span<const char> /* payload */)> cb,
        const std::string& conn_type_hint) {
        return BuildNewMessageCallback<protocol::GatewayMessage>(cb, conn_type_hint);
    }

    static NewMessageCallback BuildNewSharedLogMessageCallback(
        std::function<void(otel::context&,
                           const protocol::SharedLogMessage&,
                           std::span<const char> /* payload */)> cb,
        const std::string& conn_type_hint) {
        return BuildNewMessageCallback<protocol::SharedLogMessage>(cb, conn_type_hint);
    }

private:
    // typename be protocol::SharedLogMessage or protocol::GatewayMessage
    static size_t MessageFullSizeCallback(std::span<const char> header) {
        using protocol::TraceCtxMessage;
        DCHECK_EQ(header.size(), sizeof(TraceCtxMessage));
        const TraceCtxMessage* message = reinterpret_cast<const TraceCtxMessage*>(
            header.data());
        VLOG_F(1, "MessageFullSizeCallback message payload_size={}, message_size={}",
                message->payload_size, message->message_size);
        return sizeof(TraceCtxMessage) + message->payload_size + message->message_size;
    }

    // typename be protocol::SharedLogMessage or protocol::GatewayMessage
    template<typename T>
    static NewMessageCallback BuildNewMessageCallback(
        std::function<void(otel::context&,
                           const T&,
                           std::span<const char> /* payload */)> cb,
        const std::string& conn_type_hint) {
        using protocol::TraceCtxMessage;
        return [conn_type_hint, cb] /*IngressConnection::NewMessageCallback*/ (std::span<const char> data) {
            // data layout:
            // | ctx header | ctx payload | message header | message payload |
            // where:
            // ctx_header->payload_size = ctx_payload.size()
            // ctx_header->message_size = message_header.size() + message_payload.size()

            // context header
            DCHECK_GE(data.size(), sizeof(TraceCtxMessage));
            const TraceCtxMessage* ctx_header = reinterpret_cast<const TraceCtxMessage*>(data.data());
            VLOG(1) << fmt::format("NewMessageCallback conn_type={}, msg_type={}, data_size={}, message_payload_size={}, message_size={}",
                    conn_type_hint, typeid(T).name(), data.size(), ctx_header->payload_size, ctx_header->message_size);
            DCHECK_EQ(data.size(), sizeof(TraceCtxMessage)+ctx_header->payload_size+ctx_header->message_size);
            std::span<const char> ctx_payload = data.subspan(sizeof(TraceCtxMessage), ctx_header->payload_size);
            DCHECK_EQ(ctx_header->payload_size, ctx_payload.size());

            auto propagator = trace::propagation::HttpTraceContext();
            auto carrier = otel::StringTextMapCarrier::Deserialize(std::string(ctx_payload.data(), ctx_payload.size()));
            otel::context ctx(otel::get_context());
            ctx = propagator.Extract(carrier, ctx);

            std::span<const char> message_data = data.subspan(sizeof(TraceCtxMessage)+ctx_payload.size());

            // shared log message
            DCHECK_GE(message_data.size(), sizeof(T));
            const T* message_header = reinterpret_cast<const T*>(message_data.data());
            std::span<const char> payload = message_data.subspan(sizeof(T));
            DCHECK_EQ(message_header->payload_size, payload.size());

            // assert protocol metadata
            DCHECK_EQ(data.size(), sizeof(TraceCtxMessage)+ctx_header->payload_size+sizeof(T)+message_header->payload_size);
            DCHECK_EQ(sizeof(T)+message_header->payload_size, ctx_header->message_size);

            // DEBUG print
            otel::PrintSpanContextFromContext(ctx);

            cb(ctx, *message_header, payload);
        };
    }

    DISALLOW_COPY_AND_ASSIGN(IngressConnection);
};

}  // namespace server
}  // namespace faas
