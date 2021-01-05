#include "server/ingress_connection.h"

namespace faas {
namespace server {

IngressConnection::IngressConnection(int type, int sockfd, size_t msghdr_size)
    : ConnectionBase(type),
      io_worker_(nullptr),
      state_(kCreated),
      sockfd_(sockfd),
      msghdr_size_(msghdr_size),
      buf_group_(kDefaultBufGroup),
      buf_size_(kDefaultBufSize),
      log_header_(fmt::format("IngressConn[{}-{}]: ", type, sockfd)) {}

IngressConnection::~IngressConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void IngressConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    current_io_uring()->PrepareBuffers(buf_group_, buf_size_);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        sockfd_, buf_group_,
        absl::bind_front(&IngressConnection::OnRecvData, this)));
    state_ = kRunning;
}

void IngressConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }));
    state_ = kClosing;
}

void IngressConnection::SetMessageFullSizeCallback(MessageFullSizeCallback cb) {
    message_full_size_cb_ = cb;
}

void IngressConnection::SetNewMessageCallback(NewMessageCallback cb) {
    new_message_cb_ = cb;
}

void IngressConnection::ProcessMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    while (read_buffer_.length() >= msghdr_size_) {
        std::span<const char> header(read_buffer_.data(), msghdr_size_);
        size_t full_size = message_full_size_cb_(header);
        DCHECK_GE(full_size, msghdr_size_);
        if (read_buffer_.length() >= full_size) {
            new_message_cb_(std::span<const char>(read_buffer_.data(), full_size));
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

bool IngressConnection::OnRecvData(int status, std::span<const char> data) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    } else if (data.size() == 0) {
        HLOG(INFO) << "Connection closed remotely";
        ScheduleClose();
        return false;
    } else {
        read_buffer_.AppendData(data);
        ProcessMessages();
        return true;
    }
}

size_t IngressConnection::GatewayMessageFullSizeCallback(std::span<const char> header) {
    using protocol::GatewayMessage;
    DCHECK_EQ(header.size(), sizeof(GatewayMessage));
    const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(
        header.data());
    DCHECK_GE(message->payload_size, 0);
    return sizeof(GatewayMessage) + message->payload_size;
}

IngressConnection::NewMessageCallback IngressConnection::BuildNewGatewayMessageCallback(
        std::function<void(const protocol::GatewayMessage&, std::span<const char>)> cb) {
    using protocol::GatewayMessage;
    return [cb] (std::span<const char> data) {
        DCHECK_GE(data.size(), sizeof(GatewayMessage));
        const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(data.data());
        std::span<const char> payload;
        if (data.size() > sizeof(GatewayMessage)) {
            payload = data.subspan(sizeof(GatewayMessage));
        }
        cb(*message, payload);
    };
}

}  // namespace server
}  // namespace faas