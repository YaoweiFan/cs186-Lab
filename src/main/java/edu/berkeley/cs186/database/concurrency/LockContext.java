package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // 检查 request 是否合法，子孙资源的锁请求是否与祖先资源以上上的锁发生冲突，以下是 project guide 中列举的两种情况
        if(LockType.canBeParentLock(parent.getEffectiveLockType(transaction), lockType)) {
            // 这种情况不可以放在 waitingQueue 中等一等吗?
            throw new InvalidLockException("The request is invalid!");
        }
        if(hasSIXAncestor(transaction) && (lockType == LockType.IS || lockType == LockType.S)) {
            throw new InvalidLockException("It is redundant for the transaction to have an IS/S lock on the resource!");
        }
        // 检查锁该transaction是否已经在该资源上上了锁
        for(Lock lk : lockman.getLocks(name)) {
            if(lk.transactionNum == transaction.getTransNum()) throw new DuplicateLockRequestException("A lock is already held by the transaction on the resource!");
        }
        // 检查该 context 是否只是只读的？
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        // lockManager 会检查是否有其他 transaction 的 lock 与本请求冲突
        lockman.acquire(transaction, name, lockType);
        // 更新 numChildLocks
        numChildLocks.put(transaction.getTransNum(), lockman.getLocks(transaction).size());
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 检查 transaction 是否持有锁
        boolean noLockHeld = true;
        for(Lock lk : lockman.getLocks(transaction)) {
            if(lk.name == name) {
                noLockHeld = false;
                break;
            }
        }
        if(noLockHeld) throw new NoLockHeldException("No lock on this resource is held by the transaction！");
        // 检查该 lock 是否可已被释放
        for(Lock lk : lockman.getLocks(transaction)) {
            for(Map.Entry<String, LockContext> entry : children.entrySet()) {
                if(entry.getValue().name == lk.name) {
                    // 如果子资源有被 transaction lock 过，需要判断 这个lock 是否限制了 本资源lock 的释放
                    if(!LockType.canBeParentLock(LockType.NL, entry.getValue().getExplicitLockType(transaction))) {
                        throw new InvalidLockException("The lock cannot be released because doing so " +
                                                            "would violate multigranularity locking constraints!");
                    }
                }
            }
        }
        // 检查该 context 是否只是只读的？
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        lockman.release(transaction, name);
        // 更新 numChildLocks
        numChildLocks.put(transaction.getTransNum(), lockman.getLocks(transaction).size());
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 检查锁该transaction是否已经在该资源上上了锁
        for(Lock lk : lockman.getLocks(name)) {
            if(lk.transactionNum == transaction.getTransNum() && lk.lockType == newLockType) {
                throw new DuplicateLockRequestException("A same type lock is already held by the transaction on the resource!");
            }
        }


        lockman.promote(transaction, name, newLockType);
        // 更新 numChildLocks
        numChildLocks.put(transaction.getTransNum(), lockman.getLocks(transaction).size());
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        boolean noLockHeld = true;
        for (Lock lk : lockman.getLocks(name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                noLockHeld = false;
            }
        }
        if(noLockHeld) throw new NoLockHeldException("This transaction has no lock at this level!");
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        int num = getNumChildren(transaction);

        // 检查子孙资源是否只上了 S/IS
        if (recursiveCheckSisDescendants(this, transaction)) {
            // 只上了 S/IS
            List<ResourceName> resourceNames = sisDescendants(transaction);
            num -= resourceNames.size();
            lockman.acquireAndRelease(transaction, name, LockType.S, resourceNames);
        } else {
            // 不只上了 S/IS
            List<ResourceName> resourceNames = recursiveGetLockDescendants(this, transaction);
            num -= resourceNames.size();
            lockman.acquireAndRelease(transaction, name, LockType.X, resourceNames);
        }
        // 更新 numChildLocks
        if (numChildLocks.containsKey(transaction.getTransNum())) {
            numChildLocks.put(transaction.getTransNum(), num);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        List<Lock> transactionLks = lockman.getLocks(transaction);
        for(Lock lk : transactionLks) {
            // 遇到这一层级资源的锁就返回该锁的类型
            if(lk.name == name) return lk.lockType;
        }
        return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        // 假设在资源层级关系满足 canBeParentLock 条件
        // 得到本资源的 LockType
        LockType selfLockType = LockType.NL;
        List<Lock> lks = lockman.getLocks(name);
        for(Lock lk : lks) {
            if(lk.transactionNum == transaction.getTransNum()) {
                selfLockType = lk.lockType;
                // 如果有显示的，就直接返回显示的 LockType
                return selfLockType;
            }
        }

        // 如果一直追溯到 database 都没有持有该 transaction 的锁，表明无锁
        if(parent == null) return LockType.NL;
        // 得到祖先资源的 LockType
        LockType parentLockType = parent.getExplicitLockType(transaction);
        // 几种 intent lock 需要处理一下
        if(parentLockType == LockType.IS || parentLockType == LockType.IX) return LockType.NL;
        if(parentLockType == LockType.SIX) return LockType.S;
        return parentLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n = names.next();
        ctx = lockman.context(n);
        if(ctx.getResourceName() == name) return false;
        while (names.hasNext()) {
            n = names.next();
            // 有没有直接通过 name(String) 获取 resource 的方法？
            // ("database", "someTable", 10)
            // 从形式上看应该没有
            ctx = ctx.childContext(n);
            if(ctx.getResourceName() == name) break;
            if(lockman.getLockType(transaction, ctx.getResourceName()) == LockType.SIX) return true;
        }
        return false;
    }

    /**
     * 递归检查子资源是否上有除了 S/IS 之外的锁
     * @param lockContext the given lockContext
     * @param transaction the given transaction
     * @return true 没有除了 S/IS 之外的锁
     */
    private boolean recursiveCheckSisDescendants(LockContext lockContext, TransactionContext transaction) {
        for (Lock lk : lockman.getLocks(lockContext.name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                if(lk.lockType != LockType.S && lk.lockType != LockType.IS) {
                    return false;
                }
            }
        }
        for(Map.Entry<String, LockContext> entry : lockContext.children.entrySet()) {
            if(!recursiveCheckSisDescendants(entry.getValue(), transaction)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> res = new ArrayList<>();
        for(Map.Entry<String, LockContext> entry : children.entrySet()) {
            res.addAll(recursiveGetSisDescendants(entry.getValue(), transaction));
        }
        return res;
    }

    /**
     * 浅拷贝方式递归获取上了 S 或 IS 锁的子孙资源的 ResourceName
     * @param lockContext the given lockContext
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> recursiveGetSisDescendants(LockContext lockContext, TransactionContext transaction) {
        List<ResourceName> res = new ArrayList<>();
        for (Lock lk : lockman.getLocks(lockContext.name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                if(lk.lockType == LockType.S || lk.lockType == LockType.IS) {
                    res.add(lockContext.name);
                    break;
                }
            }
        }
        for(Map.Entry<String, LockContext> entry : lockContext.children.entrySet()) {
            res.addAll(recursiveGetSisDescendants(entry.getValue(), transaction));
        }
        return res;
    }

    /**
     *
     * @param lockContext the given lockContext
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a lock
     */
    private List<ResourceName> recursiveGetLockDescendants(LockContext lockContext, TransactionContext transaction) {
        List<ResourceName> res = new ArrayList<>();
        for (Lock lk : lockman.getLocks(lockContext.name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                res.add(lockContext.name);
                break;
            }
        }
        for(Map.Entry<String, LockContext> entry : lockContext.children.entrySet()) {
            res.addAll(recursiveGetSisDescendants(entry.getValue(), transaction));
        }
        return res;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

