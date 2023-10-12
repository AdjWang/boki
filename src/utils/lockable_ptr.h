#pragma once

#include "base/common.h"
#ifdef __FAAS_SRC
#include "base/thread.h"
#endif

namespace faas {

class Mutex {
public:
    Mutex(const char* mu_name) {
        if (mu_name == nullptr) {
            use_boost_mu = false;
            absl_mu_.reset(new absl::Mutex());
        } else {
            use_boost_mu = true;
            // DEBUG
            LOG_F(INFO, "named_sharable_mutex={}", mu_name);
            boost_mu_.reset(new boost::interprocess::named_sharable_mutex(
                boost::interprocess::open_or_create, mu_name));
        }
    }

    void Lock() {
        if (use_boost_mu) {
            BoostLock();
        } else {
            AbslLock();
        }
    }
    void Unlock() {
        if (use_boost_mu) {
            BoostUnlock();
        } else {
            AbslUnlock();
        }
    }
#if DCHECK_IS_ON()
    void AssertHeld() {
        if (use_boost_mu) {
            BoostAssertHeld();
        } else {
            AbslAssertHeld();
        }
    }
    void AssertNotHeld() {
        if (use_boost_mu) {
            BoostAssertNotHeld();
        } else {
            AbslAssertNotHeld();
        }
    }
#endif

    void ReaderLock() {
        if (use_boost_mu) {
            BoostReaderLock();
        } else {
            AbslReaderLock();
        }
    }
    void ReaderUnlock() {
        if (use_boost_mu) {
            BoostReaderUnlock();
        } else {
            AbslReaderUnlock();
        }
    }
#if DCHECK_IS_ON()
    void AssertReaderHeld() {
        if (use_boost_mu) {
            BoostAssertReaderHeld();
        } else {
            AbslAssertReaderHeld();
        }
    }
#endif

private:
    bool use_boost_mu;
    std::unique_ptr<absl::Mutex> absl_mu_;
    // Shared between processes. Each mutex must have an unique name.
    // TODO: isolation between users
    // TODO: use timed lock to resist malicious user side long term locking
    std::unique_ptr<boost::interprocess::named_sharable_mutex> boost_mu_;

    void AbslLock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_->Lock();
    }
    void AbslUnlock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_->Unlock();
    }
#if DCHECK_IS_ON()
    void AbslAssertHeld() {
        absl_mu_->AssertHeld();
    }
    void AbslAssertNotHeld() {
        absl_mu_->AssertNotHeld();
    }
#endif

    void AbslReaderLock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_->ReaderLock();
    }
    void AbslReaderUnlock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        absl_mu_->ReaderUnlock();
    }
#if DCHECK_IS_ON()
    void AbslAssertReaderHeld() {
        absl_mu_->AssertReaderHeld();
    }
#endif

    void BoostLock() {
        boost_mu_->lock();
    }
    void BoostUnlock() {
        boost_mu_->unlock();
    }
#if DCHECK_IS_ON()
    void BoostAssertHeld() {}
    void BoostAssertNotHeld() {}
#endif

    void BoostReaderLock() {
        boost_mu_->lock_sharable();
    }
    void BoostReaderUnlock() {
        boost_mu_->unlock_sharable();
    }
#if DCHECK_IS_ON()
    void BoostAssertReaderHeld() {}
#endif
};

template<class T>
class LockablePtr {
public:
    LockablePtr() : inner_(nullptr) {}

    // LockablePtr takes ownership of target
    explicit LockablePtr(std::unique_ptr<T> target,
                         const char* mu_name = nullptr)
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
        Inner(const char* mu_name)
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
