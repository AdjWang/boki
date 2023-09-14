#include "engine/message_connection.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "utils/io.h"
#include "engine/flags.h"
#include "server/constants.h"
#include "engine/engine.h"

namespace faas {
namespace engine {

using protocol::Message;
using protocol::MessageHelper;

MessageConnection::MessageConnection(Engine* engine, int sockfd)
    : server::ConnectionBase(kMessageConnectionTypeId),
      engine_(engine), io_worker_(nullptr), state_(kCreated),
      func_id_(0), client_id_(0), handshake_done_(false),
      sockfd_(sockfd), idx_next_out_fifo_(0),
      func_worker_handshake_done_(false),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void MessageConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    current_io_uring()->PrepareBuffers(kMessageConnectionBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(*sockfd_));
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        *sockfd_, kMessageConnectionBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            if (status != 0) {
                HPLOG(ERROR) << "Read error on handshake, will close this connection";
                ScheduleClose();
                return false;
            } else if (data.size() == 0) {
                HLOG(INFO) << "Connection closed remotely";
                ScheduleClose();
                return false;
            } else {
                message_buffer_.AppendData(data);
                if (message_buffer_.length() > sizeof(Message)) {
                    HLOG(ERROR) << "Invalid handshake, will close this connection";
                    ScheduleClose();
                    return false;
                } else if (message_buffer_.length() == sizeof(Message)) {
                    RecvHandshakeMessage();
                    return false;
                }
                return true;
            }
        }
    ));
    state_ = kHandshake;
}

void MessageConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    HLOG(INFO) << "Start closing";
    DCHECK(sockfd_.has_value());
    URING_DCHECK_OK(current_io_uring()->Close(*sockfd_, [this] () {
        sockfd_ = std::nullopt;
        OnFdClosed();
    }));
    if (in_fifo_fd_.has_value()) {
        URING_DCHECK_OK(current_io_uring()->Close(*in_fifo_fd_, [this] () {
            in_fifo_fd_ = std::nullopt;
            OnFdClosed();
        }));
    }
    {
        closed_count_.store(0);
        absl::MutexLock lock(&out_fifo_group_mu_);
        if (!out_fifo_fd_group_.empty()) {
            size_t n = out_fifo_fd_group_.size();
            DCHECK_EQ(n, engine_->func_worker_ipc_output_channels());
            for (int fd : out_fifo_fd_group_) {
                URING_DCHECK_OK(current_io_uring()->Close(fd,
                    [this, n]() {
                        // add_fetch
                        size_t count = closed_count_.fetch_add(1, std::memory_order_relaxed) + 1;
                        DCHECK_LE(count, n);
                        if (count == n) {
                            {
                                absl::MutexLock lock(&out_fifo_group_mu_);
                                out_fifo_fd_group_.clear();
                            }
                            OnFdClosed();
                        }
                    }));
            }
        }
    }
    state_ = kClosing;
}

void MessageConnection::SendPendingMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kHandshake) {
        return;
    }
    if (state_ != kRunning) {
        HLOG(WARNING) << "MessageConnection is closing or has closed, "
                         "will not send pending messages";
        return;
    }
    size_t write_size = 0;
    {
        absl::MutexLock lk(&write_message_mu_);
        write_size = pending_messages_.size() * sizeof(Message);
        if (write_size > 0) {
            write_message_buffer_.Reset();
            write_message_buffer_.AppendData(
                reinterpret_cast<char*>(pending_messages_.data()),
                write_size);
            pending_messages_.clear();
        }
    }
    if (write_size == 0) {
        return;
    }
    size_t n_msg = write_size / sizeof(Message);
    for (size_t i = 0; i < n_msg; i++) {
        const char* ptr = write_message_buffer_.data() + i * sizeof(Message);
        if (func_worker_handshake_done_) {
            const Message* message = reinterpret_cast<const Message*>(ptr);
            if (!WriteMessageWithFifo(*message)) {
                HLOG(FATAL) << "WriteMessageWithFifo failed";
            }
        } else {
            std::span<char> buf;
            io_worker_->NewWriteBuffer(&buf);
            CHECK_GE(buf.size(), sizeof(Message));
            memcpy(buf.data(), ptr, sizeof(Message));
            URING_DCHECK_OK(current_io_uring()->SendAll(
                *sockfd_, std::span<const char>(buf.data(), sizeof(Message)),
                [this, buf] (int status) {
                    io_worker_->ReturnWriteBuffer(buf);
                    if (status != 0) {
                        HPLOG(ERROR) << "Failed to write response, will close this connection";
                        ScheduleClose();
                    }
                }
            ));
        }
    }
}

void MessageConnection::OnFdClosed() {
    DCHECK(state_ == kClosing);
    if (    !sockfd_.has_value()
         && !in_fifo_fd_.has_value()
         && closed_count_.load() == engine_->func_worker_ipc_output_channels()) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }
}

int MessageConnection::GetPipeForWriteFd() {
    size_t n_output_ch = engine_->func_worker_ipc_output_channels();

    absl::ReaderMutexLock lock(&out_fifo_group_mu_);
    DCHECK(!out_fifo_fd_group_.empty());
    int fd = out_fifo_fd_group_[idx_next_out_fifo_];

    idx_next_out_fifo_ = (idx_next_out_fifo_ + 1) % n_output_ch;
    return fd;
}

void MessageConnection::RecvHandshakeMessage() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    Message* message = reinterpret_cast<Message*>(message_buffer_.data());
    func_id_ = message->func_id;
    if (MessageHelper::IsLauncherHandshake(*message)) {
        client_id_ = 0;
        log_header_ = fmt::format("LauncherConnection[{}]: ", func_id_);
    } else if (MessageHelper::IsFuncWorkerHandshake(*message)) {
        client_id_ = message->client_id;
        log_header_ = fmt::format("FuncWorkerConnection[{}-{}]: ", func_id_, client_id_);
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    std::span<const char> payload;
    if (!engine_->OnNewHandshake(this, *message, &handshake_response_, &payload)) {
        ScheduleClose();
        return;
    }
    if (MessageHelper::IsFuncWorkerHandshake(*message)
            && !engine_->func_worker_use_engine_socket()) {
        // init output fifo fd
        {
            absl::MutexLock lock(&out_fifo_group_mu_);
            size_t n_output_ch = engine_->func_worker_ipc_output_channels();
            // HVLOG_F(1, "[INFO] about to init input chs={} client_id={}", n_output_ch, client_id_);
            for (size_t ch = 0; ch < n_output_ch; ch++) {
                std::optional<int> out_fifo_fd = ipc::FifoOpenForWrite(ipc::GetFuncWorkerInputFifoName(client_id_, ch));
                if (!out_fifo_fd.has_value()) {
                    HLOG(ERROR) << "FifoOpenForWrite failed";
                    ScheduleClose();
                    return;
                }
                URING_DCHECK_OK(current_io_uring()->RegisterFd(*out_fifo_fd));
                io_utils::FdUnsetNonblocking(*out_fifo_fd);
                out_fifo_fd_group_.push_back(*out_fifo_fd);
            }
        }
        // init input fifo fd
        in_fifo_fd_ = ipc::FifoOpenForRead(ipc::GetFuncWorkerOutputFifoName(client_id_));
        if (!in_fifo_fd_.has_value()) {
            HLOG(ERROR) << "FifoOpenForRead failed";
            ScheduleClose();
            return;
        }
        URING_DCHECK_OK(current_io_uring()->RegisterFd(*in_fifo_fd_));
        io_utils::FdUnsetNonblocking(*in_fifo_fd_);

        func_worker_handshake_done_ = true;
    }
    char* buf = reinterpret_cast<char*>(malloc(sizeof(Message) + payload.size()));
    memcpy(buf, &handshake_response_, sizeof(Message));
    if (payload.size() > 0) {
        memcpy(buf + sizeof(Message), payload.data(), payload.size());
    }
    URING_DCHECK_OK(current_io_uring()->SendAll(
        *sockfd_, std::span<const char>(buf, sizeof(Message) + payload.size()),
        [this, buf] (int status) {
            free(buf);
            if (status != 0) {
                HPLOG(ERROR) << "Failed to write handshake response, will close this connection";
                ScheduleClose();
                return;
            }
            handshake_done_ = true;
            state_ = kRunning;
            message_buffer_.Reset();
            if (in_fifo_fd_.has_value()) {
                URING_DCHECK_OK(current_io_uring()->StartRead(
                    *in_fifo_fd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvData, this)));
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    *sockfd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvSockData, this)));
            } else {
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    *sockfd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvData, this)));
            }
            SendPendingMessages();
        }
    ));
}

void MessageConnection::WriteMessage(const Message& message) {
    if (is_func_worker_connection()
            && absl::GetFlag(FLAGS_func_worker_pipe_direct_write)
            && WriteMessageWithFifo(message)) {
        return;
    }
    {
        absl::MutexLock lk(&write_message_mu_);
        pending_messages_.push_back(message);
    }
    io_worker_->ScheduleFunction(
        this, absl::bind_front(&MessageConnection::SendPendingMessages, this));
}

bool MessageConnection::OnRecvSockData(int status, std::span<const char> data) {
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    }
    if (data.size() == 0) {
        HLOG(INFO) << "Connection closed remotely";
        ScheduleClose();
        return false;
    }
    HLOG(WARNING) << "Unexpected data from socket: size=" << data.size();
    return true;
}

bool MessageConnection::OnRecvData(int status, std::span<const char> data) {
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    }
    if (data.size() == 0) {
        if (!in_fifo_fd_.has_value()) {
            HLOG(INFO) << "Connection closed remotely";
            ScheduleClose();
            return false;
        } else {
            return true;
        }
    }
    utils::ReadMessages<Message>(
        &message_buffer_, data.data(), data.size(),
        [this] (Message* message) {
            engine_->OnRecvMessage(this, *message);
        });
    return true;
}

bool MessageConnection::WriteMessageWithFifo(const protocol::Message& message) {
    int fd = GetPipeForWriteFd();
    if (fd == -1) {
        return false;
    }
    server::IOWorker* current = server::IOWorker::current();
    if (current == nullptr) {
        return false;
    }
    std::span<char> buf;
    current->NewWriteBuffer(&buf);
    CHECK_GE(buf.size(), sizeof(Message));
    memcpy(buf.data(), &message, sizeof(Message));
    URING_DCHECK_OK(current->io_uring()->Write(
        fd, std::span<const char>(buf.data(), sizeof(Message)),
        [this, current, buf] (int status, size_t nwrite) {
            current->ReturnWriteBuffer(buf);
            if (status != 0 || nwrite == 0) {
                if (status != 0) {
                    HPLOG(ERROR) << "Failed to write message";
                } else {
                    HLOG(ERROR) << "Failed to write message: nwrite=0";
                }
                if (current == io_worker_) {
                    ScheduleClose();
                    return;
                }
            }
            CHECK_EQ(nwrite, sizeof(Message)) << "Write to FIFO is not atomic";
        }
    ));
    return true;
}

}  // namespace engine
}  // namespace faas
