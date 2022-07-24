package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for(Lock lock : locks) {
                if(!LockType.compatible(lockType, lock.lockType) && lock.transactionNum != except) return false;
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            for(Lock lk : locks) {
                if(lk.transactionNum.equals(lock.transactionNum)) {
                    // 到这儿表明锁需要更新
                    lk.lockType = lock.lockType;
                    // 将该 lock 对应的 transaction 上的锁更新过来
                    List<Lock> transactionLks = transactionLocks.get(lk.transactionNum);
                    for(Lock transactionLk : transactionLks) {
                        if(transactionLk.name.equals(lk.name)) {
                            transactionLk.lockType = lock.lockType;
                        }
                    }
                    return;
                }
            }

            // 到这儿表明锁需要添加
            locks.add(lock);
            // 在该 lock 对应的 transaction 上也相应地添加
            // 这个地方是应该创建一个新的 lock 还是只用浅拷贝？ 这里采用浅拷贝，也就是认为 transactionLocks 和 locks 中相同参数的锁是同一把锁
            addLocks(lock.transactionNum, lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.removeIf(lk->Objects.equals(lk, lock));
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if(addFront) {
                waitingQueue.addFirst(request);
            }
            else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement
            while(requests.hasNext()) {
                LockRequest lr = requests.next();
                // 这里假定同一个 transaction，对于同一个 resource 的锁不管如何，可以直接 update 上去
                if (checkCompatible(lr.lock.lockType, lr.lock.transactionNum)){
                    grantOrUpdateLock(lr.lock);
                    // 这样子操作会不会出问题？
                    waitingQueue.removeFirst();
                    lr.transaction.unblock();
                } else {
                    break;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for(Lock lk : locks) {
                if(lk.transactionNum == transaction) return lk.lockType;
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.     // 这个在这里要如何实现呢？ 加入请求队列即可
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.  // 这么处理的目的是什么呢？
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transaction.getTransNum()) == lockType) {
                throw new DuplicateLockRequestException("This transaction has already acquire a same type lock on the resource!");
            }

            List<Lock> releasedLocks = new ArrayList<>();
            for(ResourceName releaseName : releaseNames) {
                LockType type = getResourceEntry(releaseName).getTransactionLockType(transaction.getTransNum());
                if(type == LockType.NL) {
                    throw new NoLockHeldException("This transaction doesn't hold a lock on one or more of the resource!");
                }
                // 这里的 releasedLocks 并没有直接引用现有的锁，而是重新创建了
                releasedLocks.add(new Lock(releaseName, type, transaction.getTransNum()));
            }


            if(!entry.checkCompatible(lockType, transaction.getTransNum())) {
                // 为什么这里要把请求放在队列的前面？ 应该是这个函数的特质（支持后续的实现），在 project guide 中有比较好的说明
                // 那么为什么要有这样的特质呢？
                // transaction blocked
                shouldBlock = true;
                Lock requestLock = new Lock(name, lockType, transaction.getTransNum());
                LockRequest request = new LockRequest(transaction, requestLock, releasedLocks);
                entry.addToQueue(request, true);

                // 调用以下方法的原因是为了避免 race condition：the transaction may be dequeued between the time
                // it leaves the synchronized block and the time it actually blocks.
                transaction.prepareBlock();
            } else {
                // 这里直接更新或者新增锁了（更新锁的时候，没有考虑新的锁是否能够代替原来的锁）
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
                // 释放锁
                for(ResourceName releaseName : releaseNames) {
                    // 如果释放的对象是自己的话，由于已经更新过锁了，就不需要再释放了
                    if (releaseName.equals(name)) continue;
                    release(transaction, releaseName);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> lks = getLocks(name);
            for(Lock lk : lks) {
                if (lk.transactionNum == transaction.getTransNum()) {
                    // 这个异常为什么需要抛出？不是可以直接 update 上去的吗？
                    // 应该是因为无法隐式转换的缘故，需要显示调用才行
                    throw new DuplicateLockRequestException("This transaction has already acquire a lock on the resource!");
                }
            }

            ResourceEntry entry = getResourceEntry(name);
            // 先处理等待队列里的锁
            // entry.processQueue();
            if(!entry.checkCompatible(lockType, transaction.getTransNum()) || entry.waitingQueue.size() > 0) {
                // transaction blocked
                shouldBlock = true;
                // 调用以下方法的原因是为了避免 race condition：the transaction may be dequeued between the time
                // it leaves the synchronized block and the time it actually blocks.
                transaction.prepareBlock();
                LockRequest request = new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum()));
                entry.addToQueue(request, false);
            } else {
                // 到这儿具备的条件：
                // 1. 该 resource 现在所有的上的 lock 都是可以和 LockType 兼容的（不涉及锁升级）  或者  该 resource 现在并不拥有锁
                // 2. 在 waitingQueue 中没有有其他 transaction 在等待
                // 如果等待队列里存在同一个 transaction 的两个 lock，要怎么处理呢？
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
                // entry.processQueue();
                // transaction 也获取相同的 lock
                addLocks(transaction.getTransNum(), new Lock(name, lockType, transaction.getTransNum()));
            }

        }

        // 为什么把这个放在 synchronized 外面？
        // 如果一个 transaction 的线程在 synchronized block 中阻塞，那么其它线程就无法访问 LockManger 直到其解除阻塞，但是这没有可能，
        // 因为 LockManager 是唯一可以解除阻塞的途径
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            boolean noLockHeld = true;
            for(Lock lk : entry.locks) {
                if(lk.transactionNum == transaction.getTransNum()) {
                    noLockHeld = false;
                    entry.releaseLock(lk);
                    break;
                }
            }
            if(noLockHeld) throw new NoLockHeldException("This transaction has no lock on the resource!");

            List<Lock> transactionLks = getLocks(transaction);
            for(Lock lk : transactionLks) {
                if(lk.name.equals(name)) removeLocks(transaction, lk);
            }
            // 在 release 被调用后，resource 的请求队列应该被处理
            entry.processQueue();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> resourceLks = getLocks(name);
            Lock desLock = null;
            for(Lock lk : resourceLks) {
                if (lk.transactionNum == transaction.getTransNum() && lk.lockType == newLockType) {
                    throw new DuplicateLockRequestException("This transaction has already acquire a same type lock on the resource!");
                }
                if (lk.transactionNum == transaction.getTransNum()) {
                    desLock = lk;
                }
            }
            if(desLock == null) {
                throw new NoLockHeldException("This transaction hasn't have a lock on the resource!");
            }
            if(!LockType.substitutable(newLockType, desLock.lockType) || newLockType != desLock.lockType) {
                throw new InvalidLockException("This new lock ca not substitute the old one!");
            }

            ResourceEntry entry = getResourceEntry(name);
            // 先处理等待队列里的锁
            // entry.processQueue();

            if(!entry.checkCompatible(newLockType, transaction.getTransNum())) {
                // 到这儿具备的条件：
                // 1. 该 resource 包含一个该 transaction 的 lock
                // 2. 该 resource 现在所有的上的 lock 有和 LockType 不兼容的
                shouldBlock = true;
                // 调用以下方法的原因是为了避免 race condition：the transaction may be dequeued between the time
                // it leaves the synchronized block and the time it actually blocks.
                transaction.prepareBlock();
                LockRequest request = new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum()));
                entry.addToQueue(request, true);
            } else {
                // 到这儿具备的条件：
                // 1. 该 resource 包含一个该 transaction 的 lock
                // 2. 该 resource 现在所有的上的 lock 都是可以和 LockType 兼容的
                entry.grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
                // entry.processQueue();
                // transaction 中记录的 locks 也需要进行更新
                promoteLocks(transaction, name, newLockType);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        List<Lock> lks = getLocks(name);
        for (Lock lk : lks) {
            if (lk.transactionNum == transaction.getTransNum()) return lk.lockType;
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    public synchronized void addLocks(long transactionNum, Lock lock) {
        if(transactionLocks.containsKey(transactionNum)) {
            transactionLocks.get(transactionNum).add(lock);
        } else {
            List<Lock> value = new ArrayList<>();
            value.add(lock);
            transactionLocks.put(transactionNum, value);
        }
    }

    public synchronized void removeLocks(TransactionContext transaction, Lock lock) {
        transactionLocks.get(transaction.getTransNum()).remove(lock);
    }

    public synchronized void promoteLocks(TransactionContext transaction, ResourceName name, LockType newLockType) {
        List<Lock> transactionLks = transactionLocks.get(transaction.getTransNum());
        for(Lock lk : transactionLks) {
            // transactionLks 的含义是什么？
            // 被该 transaction 掌控的 locks
            if(lk.name.equals(name)) {
                lk.lockType = newLockType;
            }
        }
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
