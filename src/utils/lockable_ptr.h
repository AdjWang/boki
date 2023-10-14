#pragma once

#include "base/common.h"
#ifdef __FAAS_SRC
#include "base/thread.h"
#endif
#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "utils/fs.h"
#include "utils/debug.h"

namespace faas {

class MutexBase {
public:
    virtual ~MutexBase() {}

    virtual void Lock() = 0;
    virtual void Unlock() = 0;
    virtual void AssertHeld() = 0;
    virtual void AssertNotHeld() = 0;

    virtual void ReaderLock() = 0;
    virtual void ReaderUnlock() = 0;
    virtual void AssertReaderHeld() = 0;
};

class AbslMutex : public MutexBase {
public:
    void Lock() override ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_.Lock();
    }
    void Unlock() override ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_.Unlock();
    }
    void AssertHeld() override {
        absl_mu_.AssertHeld();
    }
    void AssertNotHeld() override {
        absl_mu_.AssertNotHeld();
    }

    void ReaderLock() override ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_.ReaderLock();
    }
    void ReaderUnlock() override ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_.ReaderUnlock();
    }
    void AssertReaderHeld() override {
        absl_mu_.AssertReaderHeld();
    }

private:
    absl::Mutex absl_mu_;
};

class BoostMutex : public MutexBase {
public:
    BoostMutex(std::string_view mu_name)
    : mu_name_(mu_name),
#ifdef __COMPILE_AS_SHARED
      boost_mu_(boost::interprocess::open_only, mu_name_.c_str())
#else
      boost_mu_(boost::interprocess::create_only, mu_name_.c_str())
#endif
    { }

    ~BoostMutex() override {
        // DEBUG
        UNREACHABLE();

#ifndef __COMPILE_AS_SHARED
        bool success =
            boost::interprocess::named_sharable_mutex::remove(mu_name_.c_str());
        if (!success) {
            LOG_F(FATAL, "failed to remove mu name={}", mu_name_);
        }
        // DEBUG
        else {
            LOG_F(INFO, "remove mu name={}", mu_name_);
        }
#endif
    }

    void Lock() override {
        boost_mu_.lock();
    }
    void Unlock() override {
        boost_mu_.unlock();
    }
    void AssertHeld() override {}
    void AssertNotHeld() override {}

    void ReaderLock() override {
        boost_mu_.lock_sharable();
    }
    void ReaderUnlock() override {
        boost_mu_.unlock_sharable();
    }
    void AssertReaderHeld() override {}

private:
    std::string mu_name_;
    // Shared between processes. Each mutex must have an unique name.
    // TODO: isolation between users
    // TODO: use timed lock to resist malicious user side long term locking
    boost::interprocess::named_sharable_mutex boost_mu_;
};

// TODO: use timed lock to resist malicious user side long term locking
// class PosixMutex : public MutexBase {
// public:
//     PosixMutex(std::string_view mu_name)
//      : mu_name_(mu_name),
// #ifdef __COMPILE_AS_SHARED
//        posix_mu_region_(
//            ipc::ShmOpenByPath(std::string(mu_name), /*readonly*/ false)),
// #else
//        posix_mu_region_(
//            ipc::ShmCreateByPath(std::string(mu_name), sizeof(pthread_rwlock_t))),
// #endif
//        posix_mu_(reinterpret_cast<pthread_rwlock_t*>(posix_mu_region_->base()))
//     {
// #ifdef __COMPILE_AS_SHARED
//         posix_mu_region_->DisableRemoveOnDestruction();
// #else
//         posix_mu_region_->EnableRemoveOnDestruction();

//         if ((errno = pthread_rwlockattr_init(&posix_mu_attr_)) != 0) {
//             PLOG_F(FATAL, "failed to init pthread mutex attr {}", mu_name_);
//         }
//         if ((errno = pthread_rwlockattr_setpshared(&posix_mu_attr_, PTHREAD_PROCESS_SHARED)) != 0) {
//             PLOG_F(FATAL, "failed to init pthread mutex attr shared {}", mu_name_);
//         }
//         if ((errno = pthread_rwlock_init(posix_mu_, &posix_mu_attr_)) != 0) {
//             PLOG_F(FATAL, "failed to init pthread mutex {}", mu_name_);
//         }
// #endif
//     }
//     ~PosixMutex() override {
//         // DEBUG
//         UNREACHABLE();

//         DCHECK_EQ(pthread_rwlock_trywrlock(posix_mu_), 0);
// #ifndef __COMPILE_AS_SHARED
//         if ((errno = pthread_rwlockattr_destroy(&posix_mu_attr_)) != 0) {
//             PLOG_F(FATAL, "failed to destroy pthread mutex attr {}", mu_name_);
//         }
// #endif
//         if ((errno = pthread_rwlock_destroy(posix_mu_)) != 0) {
//             PLOG_F(FATAL, "failed to destroy pthread mutex {}", mu_name_);
//         }
//     }

//     void Lock() override {
//         if ((errno = pthread_rwlock_wrlock(posix_mu_)) != 0) {
//             PLOG_F(FATAL, "failed to lock pthread mutex {}", mu_name_);
//         }
//     }
//     void Unlock() override {
//         if ((errno = pthread_rwlock_unlock(posix_mu_)) != 0) {
//             PLOG_F(FATAL, "failed to unlock pthread mutex {}", mu_name_);
//         }
//     }
//     void AssertHeld() override {
//         if ((errno = pthread_rwlock_trywrlock(posix_mu_)) != EBUSY) {
//             PLOG_F(FATAL, "AssertHeld of {} failed", mu_name_);
//         }
//     }
//     void AssertNotHeld() override {}

//     void ReaderLock() override {
//         if ((errno = pthread_rwlock_rdlock(posix_mu_)) != 0) {
//             PLOG_F(FATAL, "failed to read lock pthread mutex {}", mu_name_);
//         }
//     }
//     void ReaderUnlock() override {
//         if ((errno = pthread_rwlock_unlock(posix_mu_)) != 0) {
//             PLOG_F(FATAL, "failed to read unlock pthread mutex {}", mu_name_);
//         }
//     }
//     void AssertReaderHeld() override {
//         if ((errno = pthread_rwlock_tryrdlock(posix_mu_)) != EBUSY) {
//             PLOG_F(FATAL, "AssertReaderHeld of {} failed", mu_name_);
//         }
//     }

// private:
//     std::string mu_name_;
//     std::unique_ptr<ipc::ShmRegion> posix_mu_region_;

// #ifndef __COMPILE_AS_SHARED
//     pthread_rwlockattr_t posix_mu_attr_;
// #endif
//     pthread_rwlock_t* posix_mu_;
// };

class PosixMutex : public MutexBase {
public:
    PosixMutex(std::string_view mu_name)
     : mu_name_(mu_name),
#ifdef __COMPILE_AS_SHARED
       posix_mu_region_(
           ipc::ShmOpenByPath(std::string(mu_name), /*readonly*/ false)),
#else
       posix_mu_region_(
           ipc::ShmCreateByPath(std::string(mu_name), sizeof(pthread_mutex_t))),
#endif
       posix_mu_(reinterpret_cast<pthread_mutex_t*>(posix_mu_region_->base()))
    {
#ifdef __COMPILE_AS_SHARED
        posix_mu_region_->DisableRemoveOnDestruction();
#else
        posix_mu_region_->EnableRemoveOnDestruction();

        if ((errno = pthread_mutexattr_init(&posix_mu_attr_)) != 0) {
            PLOG_F(FATAL, "failed to init pthread mutex attr {}", mu_name_);
        }
        if ((errno = pthread_mutexattr_setpshared(&posix_mu_attr_, PTHREAD_PROCESS_SHARED)) != 0) {
            PLOG_F(FATAL, "failed to init pthread mutex attr shared {}", mu_name_);
        }
        if ((errno = pthread_mutex_init(posix_mu_, &posix_mu_attr_)) != 0) {
            PLOG_F(FATAL, "failed to init pthread mutex {}", mu_name_);
        }
#endif
    }
    ~PosixMutex() override {
        DCHECK_EQ(pthread_mutex_trylock(posix_mu_), 0);
#ifndef __COMPILE_AS_SHARED
        if ((errno = pthread_mutexattr_destroy(&posix_mu_attr_)) != 0) {
            PLOG_F(FATAL, "failed to destroy pthread mutex attr {}", mu_name_);
        }
#endif
        if ((errno = pthread_mutex_destroy(posix_mu_)) != 0) {
            PLOG_F(FATAL, "failed to destroy pthread mutex {}", mu_name_);
        }
    }

    void Lock() override {
        if ((errno = pthread_mutex_lock(posix_mu_)) != 0) {
            PLOG_F(FATAL, "failed to lock pthread mutex {}", mu_name_);
        }
    }
    void Unlock() override {
        if ((errno = pthread_mutex_unlock(posix_mu_)) != 0) {
            PLOG_F(FATAL, "failed to unlock pthread mutex {}", mu_name_);
        }
    }
    void AssertHeld() override {
        if ((errno = pthread_mutex_trylock(posix_mu_)) != EBUSY) {
            PLOG_F(FATAL, "AssertHeld of {} failed", mu_name_);
        }
    }
    void AssertNotHeld() override {}

    void ReaderLock() override {
        if ((errno = pthread_mutex_lock(posix_mu_)) != 0) {
            PLOG_F(FATAL, "failed to read lock pthread mutex {}", mu_name_);
        }
    }
    void ReaderUnlock() override {
        if ((errno = pthread_mutex_unlock(posix_mu_)) != 0) {
            PLOG_F(FATAL, "failed to read unlock pthread mutex {}", mu_name_);
        }
    }
    void AssertReaderHeld() override {
        if ((errno = pthread_mutex_trylock(posix_mu_)) != EBUSY) {
            PLOG_F(FATAL, "AssertReaderHeld of {} failed", mu_name_);
        }
    }

private:
    std::string mu_name_;
    std::unique_ptr<ipc::ShmRegion> posix_mu_region_;

#ifndef __COMPILE_AS_SHARED
    pthread_mutexattr_t posix_mu_attr_;
#endif
    pthread_mutex_t* posix_mu_;
};

class Mutex : public MutexBase {
public:
    Mutex(std::string_view mu_name) {
        if (mu_name == "") {
            mu_impl_.reset(new AbslMutex());
        } else {
            DCHECK_GT(mu_name.size(), 0u);
            // DEBUG
#ifdef __COMPILE_AS_SHARED
            LOG_F(INFO, "open named_sharable_mutex={}", mu_name);
#else
            LOG_F(INFO, "create named_sharable_mutex={}", mu_name);
#endif
            // DEBUG
            // mu_impl_.reset(new BoostMutex(mu_name));
            mu_impl_.reset(new PosixMutex(mu_name));
        }
    }

    void Lock() override {
        mu_impl_->Lock();
    }
    void Unlock() override {
        mu_impl_->Unlock();
    }
    void AssertHeld() override {
        mu_impl_->AssertHeld();
    }
    void AssertNotHeld() override {
        mu_impl_->AssertNotHeld();
    }

    void ReaderLock() override {
        mu_impl_->ReaderLock();
    }
    void ReaderUnlock() override {
        mu_impl_->ReaderUnlock();
    }
    void AssertReaderHeld() override {
        mu_impl_->AssertReaderHeld();
    }

private:
    std::unique_ptr<MutexBase> mu_impl_;
};

template<class T>
class LockablePtr {
public:
    LockablePtr() : inner_(nullptr) {}

    // LockablePtr takes ownership of target
    explicit LockablePtr(std::unique_ptr<T> target,
                         std::string_view mu_name = "")
        : inner_(nullptr) {
        if (target != nullptr) {
            inner_.reset(new Inner(mu_name));
            inner_->target = std::move(target);
        }
    }

    // LockablePtr is copyable, thus can be shared between threads
    LockablePtr(const LockablePtr& other) = default;
    LockablePtr(LockablePtr&& other) = default;
    LockablePtr& operator=(LockablePtr&& other) noexcept {
        this->inner_ = std::move(other.inner_);
        return *this;
    }
    LockablePtr& operator=(const LockablePtr& other) noexcept {
        this->inner_ = other.inner_;
        return *this;
    }

    // Check if holds a target object
    inline bool is_null() const noexcept { return inner_ == nullptr; }
    inline bool not_null() const noexcept { return inner_ != nullptr; }
    explicit operator bool() const noexcept { return not_null(); }

    class Guard {
    public:
        ~Guard() {
            if (mutex_ == nullptr) {
                return;
            }
#if DCHECK_IS_ON()
            mutex_->AssertHeld();
#ifdef __FAAS_SRC
            if (base::Thread::current() != thread_) {
                LOG(FATAL) << "Guard moved between threads";
            }
#endif
#endif
            mutex_->Unlock();
        }

        T& operator*() const noexcept { return *DCHECK_NOTNULL(target_); }
        T* operator->() const noexcept { return DCHECK_NOTNULL(target_); }

        // Guard is movable, but should avoid doing so explicitly
#ifdef __FAAS_SRC
        Guard(Guard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_),
              thread_(other.thread_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
            other.thread_ = nullptr;
        }
        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                thread_ = other.thread_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
                other.thread_ = nullptr;
            }
            return *this;
        }
#else
        Guard(Guard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
        }
        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
            }
            return *this;
        }
#endif

    private:
        friend class LockablePtr;
        Mutex*        mutex_;
        T*            target_;
#ifdef __FAAS_SRC
        base::Thread* thread_;

        Guard(Mutex* mutex, T* target,
              base::Thread* thread = nullptr)
            : mutex_(mutex), target_(target), thread_(thread) {}
#else
        Guard(Mutex* mutex, T* target)
            : mutex_(mutex), target_(target) {}
#endif

        DISALLOW_COPY_AND_ASSIGN(Guard);
    };

    class ReaderGuard {
    public:
        ~ReaderGuard() {
            if (mutex_ == nullptr) {
                return;
            }
#if DCHECK_IS_ON()
            mutex_->AssertReaderHeld();
#ifdef __FAAS_SRC
            if (base::Thread::current() != thread_) {
                LOG(FATAL) << "ReaderGuard moved between threads";
            }
#endif
#endif
            mutex_->ReaderUnlock();
        }

        const T& operator*() const noexcept { return *DCHECK_NOTNULL(target_); }
        const T* operator->() const noexcept { return DCHECK_NOTNULL(target_); }

        // ReaderGuard is movable, but should avoid doing so explicitly
#ifdef __FAAS_SRC
        ReaderGuard(ReaderGuard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_),
              thread_(other.thread_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
            other.thread_ = nullptr;
        }
        ReaderGuard& operator=(ReaderGuard&& other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                thread_ = other.thread_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
                other.thread_ = nullptr;
            }
            return *this;
        }
#else
        ReaderGuard(ReaderGuard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
        }
        ReaderGuard& operator=(ReaderGuard&& other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
            }
            return *this;
        }
#endif

    private:
        friend class LockablePtr;
        Mutex*        mutex_;
        const T*      target_;
#ifdef __FAAS_SRC
        base::Thread* thread_;

        ReaderGuard(Mutex* mutex, const T* target,
                    base::Thread* thread = nullptr)
            : mutex_(mutex), target_(target), thread_(thread) {}
#else
        ReaderGuard(Mutex* mutex, const T* target)
            : mutex_(mutex), target_(target) {}
#endif

        DISALLOW_COPY_AND_ASSIGN(ReaderGuard);
    };

    // Returned Guard must not live longer than parent LockablePtr
    Guard Lock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        if (__FAAS_PREDICT_FALSE(inner_ == nullptr)) {
            LOG(FATAL) << "Cannot Lock() on null pointer";
        }
#if DCHECK_IS_ON()
        inner_->mu.AssertNotHeld();
#endif
        inner_->mu.Lock();
#if DCHECK_IS_ON() && defined(__FAAS_SRC)
        return Guard(&inner_->mu, inner_->target.get(), base::Thread::current());
#else
        return Guard(&inner_->mu, inner_->target.get());
#endif
    }
    ReaderGuard ReaderLock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        if (__FAAS_PREDICT_FALSE(inner_ == nullptr)) {
            LOG(FATAL) << "Cannot ReaderLock() on null pointer";
        }
        inner_->mu.ReaderLock();
#if DCHECK_IS_ON() && defined(__FAAS_SRC)
        return ReaderGuard(&inner_->mu, inner_->target.get(), base::Thread::current());
#else
        return ReaderGuard(&inner_->mu, inner_->target.get());
#endif
    }

private:
    struct Inner {
        Mutex              mu;
        std::unique_ptr<T> target;
        Inner(std::string_view mu_name)
            : mu(mu_name) {}
    };
    std::shared_ptr<Inner> inner_;
};

template<class T>
bool operator==(const LockablePtr<T>& ptr, std::nullptr_t) noexcept {
    return ptr.is_null();
}

template<class T>
bool operator!=(const LockablePtr<T>& ptr, std::nullptr_t) noexcept {
    return ptr.not_null();
}

}  // namespace faas
