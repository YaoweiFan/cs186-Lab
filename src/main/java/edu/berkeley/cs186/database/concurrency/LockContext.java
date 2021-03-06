package edu.berkeley.cs186.database.concurrency;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import edu.berkeley.cs186.database.Transaction;
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
        // ????????? context ????????????????????????
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        // ?????? request ??????????????????????????????????????????????????????????????????????????????????????????????????? project guide ????????????????????????
        if(parent!=null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), lockType)) {
            // ??????????????????????????? waitingQueue ????????????????
            throw new InvalidLockException("The request is invalid!");
        }
        if(hasSIXAncestor(transaction) && (lockType == LockType.IS || lockType == LockType.S)) {
            throw new InvalidLockException("It is redundant for the transaction to have an IS/S lock on the resource!");
        }
        // ????????????transaction????????????????????????????????????
        // ????????? LockManager ??????????????????????????????????????????
        //for(Lock lk : lockman.getLocks(name)) {
        //    if(lk.transactionNum == transaction.getTransNum()) throw new DuplicateLockRequestException("A lock is already held by the transaction on the resource!");
        //}

        // lockManager ???????????????????????? transaction ??? lock ??????????????????
        lockman.acquire(transaction, name, lockType);
        // ????????????????????????????????? numChildLocks
        List<ResourceName> updateResourceNameList = new ArrayList<>();
        updateResourceNameList.add(name);
        updateNumChildLocks(transaction, updateResourceNameList, 1);
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
        // ????????? context ????????????????????????
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        // ?????? transaction ???????????????
        boolean noLockHeld = true;
        for(Lock lk : lockman.getLocks(transaction)) {
            if(lk.name.equals(name)) {
                noLockHeld = false;
                break;
            }
        }
        if(noLockHeld) throw new NoLockHeldException("No lock on this resource is held by the transaction???");
        // ????????? lock ?????????????????????
        for(Lock lk : lockman.getLocks(transaction)) {
            for(Map.Entry<String, LockContext> entry : children.entrySet()) {
                if(entry.getValue().name.equals(lk.name)) {
                    // ????????????????????? transaction lock ?????????????????? ??????lock ??????????????? ?????????lock ?????????
                    if(!LockType.canBeParentLock(LockType.NL, entry.getValue().getExplicitLockType(transaction))) {
                        throw new InvalidLockException("The lock cannot be released because doing so " +
                                                            "would violate multigranularity locking constraints!");
                    }
                }
            }
        }

        lockman.release(transaction, name);
        // ?????? numChildLocks
        List<ResourceName> updateResourceNameList = new ArrayList<>();
        updateResourceNameList.add(name);
        updateNumChildLocks(transaction, updateResourceNameList, -1);
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
        // ????????? context ????????????????????????
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        // ????????????transaction????????????????????????????????????
        // ?????? transaction ??????????????? resource ??? lock
        boolean hasLock = false;
        LockType desLockType = LockType.NL;
        for(Lock lk : lockman.getLocks(name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                hasLock = true;
                desLockType = lk.lockType;
                if(lk.lockType == newLockType) {
                    throw new DuplicateLockRequestException("A same type lock is already held by the transaction on the resource!");
                }
            }
        }
        if(!hasLock) throw new NoLockHeldException("The transaction has no lock on the resource!");
        // ?????????????????????promote???promoting ??????????????? lock manager ???????????? invalid ??????
        // ????????????????????????????????? newLockType != desLockType
        if(!LockType.substitutable(newLockType, desLockType)) {
            throw new InvalidLockException("The lock can't be promoted!");
        }

        if(parent != null && !LockType.canBeParentLock(lockman.getLockType(transaction, parent.name), newLockType)){
            throw new InvalidLockException("The lock on parent resource for this transaction can't be parent of this newLockType!");
        }

        if(newLockType == LockType.SIX) {
            if(hasSIXAncestor(transaction)) throw new InvalidLockException("Disallow having IS/S locks on descendants when a SIX lock is held!");
            // ???????????????????????? SIX ????????????????????????????????? lock ????????????????????????
            // promote and release
            // In the special case of promotion to SIX (from IS/IX/S),
            // you should simultaneously release all descendant locks of type S/IS,
            // since we disallow having IS/S locks on descendants when a SIX lock is held.
            List<ResourceName> releaseNames = sisDescendants(transaction);
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.SIX, releaseNames);
            releaseNames.remove(name);
            // ?????? numChildLocks
            updateNumChildLocks(transaction, releaseNames, -1);

        } else {
            lockman.promote(transaction, name, newLockType);
        }
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
        if(readonly) throw new UnsupportedOperationException("The context is readonly!");

        boolean noLockHeld = true;
        LockType lockType = null;
        for (Lock lk : lockman.getLocks(name)) {
            if(lk.transactionNum == transaction.getTransNum()) {
                noLockHeld = false;
                lockType = lk.lockType;
            }
        }
        if(noLockHeld) throw new NoLockHeldException("This transaction has no lock at this level!");

        // ????????????????????????????????? intent lock???????????????????????????
//        if (recursiveCheckIntentDescendants(this, transaction)) {
//            List<ResourceName> releaseNames = intentDescendants(transaction);
//            releaseNames.add(name);
//            lockman.acquireAndRelease(transaction, name, LockType.NL, releaseNames);
//            updateNumChildLocks(transaction, releaseNames, -1);
//            return;
//        }

        // ?????????????????????????????????????????? intent lock??????????????????
        if(getNumChildren(transaction) == 0 && (lockType == LockType.S || lockType == LockType.X)) return;

        // ?????????????????????????????????????????? S/IS
        if (recursiveCheckSisDescendants(this, transaction)) {
            // ????????? S/IS
            List<ResourceName> releaseNames = sisDescendants(transaction);
            // ??????????????????????????????????????????????????? log?????????????????? TestLockUtil.testIStoS ????????????
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.S, releaseNames);
            releaseNames.remove(name);
            // ?????????????????? release ?????????????????????????????? level ????????????????????????????????? acquire ?????????
            updateNumChildLocks(transaction, releaseNames, -1);
        } else {
            // ???????????? S/IS
            List<ResourceName> releaseNames = recursiveGetLockDescendants(this, transaction);
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.X, releaseNames);
            releaseNames.remove(name);
            // ?????????????????? release ?????????????????????????????? level ????????????????????????????????? acquire ?????????
            updateNumChildLocks(transaction, releaseNames, -1);
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
            // ??????????????????????????????????????????????????????
            if(lk.name.equals(name)) return lk.lockType;
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
        // ????????????????????????????????? canBeParentLock ??????
        // ?????????????????? LockType
        LockType selfLockType = LockType.NL;
        List<Lock> lks = lockman.getLocks(name);
        for(Lock lk : lks) {
            if(lk.transactionNum == transaction.getTransNum()) {
                selfLockType = lk.lockType;
                // ????????????????????????????????????????????? LockType
                return selfLockType;
            }
        }

        // ????????????????????? database ?????????????????? transaction ?????????????????????
        if(parent == null) return LockType.NL;
        // ????????????????????? LockType
        LockType parentLockType = parent.getEffectiveLockType(transaction);
        // ?????? intent lock ??????????????????
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
        if(ctx.getResourceName().equals(name)) return false;
        while (names.hasNext()) {
            n = names.next();
            // ????????????????????? name(String) ?????? resource ????????????
            // ("database", "someTable", 10)
            // ???????????????????????????
            ctx = ctx.childContext(n);
            if(ctx.getResourceName().equals(name)) break;
            if(lockman.getLockType(transaction, ctx.getResourceName()) == LockType.SIX) return true;
        }
        return false;
    }

    /**
     * ??????????????????????????????????????? S/IS ????????????
     * @param lockContext the given lockContext
     * @param transaction the given transaction
     * @return true ???????????? S/IS ????????????
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

//    private boolean recursiveCheckIntentDescendants(LockContext lockContext, TransactionContext transaction) {
//        for (Lock lk : lockman.getLocks(lockContext.name)) {
//            if(lk.transactionNum == transaction.getTransNum()) {
//                if(lk.lockType != LockType.IS && lk.lockType != LockType.IX) {
//                    return false;
//                }
//            }
//        }
//        for(Map.Entry<String, LockContext> entry : lockContext.children.entrySet()) {
//            if(!recursiveCheckIntentDescendants(entry.getValue(), transaction)) {
//                return false;
//            }
//        }
//        return true;
//    }

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
     * ????????????????????????????????? S ??? IS ????????????????????? ResourceName
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



//    private List<ResourceName> intentDescendants(TransactionContext transaction) {
//        // TODO(proj4_part2): implement
//        List<ResourceName> res = new ArrayList<>();
//        for(Map.Entry<String, LockContext> entry : children.entrySet()) {
//            res.addAll(recursiveGetIntentDescendants(entry.getValue(), transaction));
//        }
//        return res;
//    }
//
//    private List<ResourceName> recursiveGetIntentDescendants(LockContext lockContext, TransactionContext transaction) {
//        List<ResourceName> res = new ArrayList<>();
//        for (Lock lk : lockman.getLocks(lockContext.name)) {
//            if(lk.transactionNum == transaction.getTransNum()) {
//                if(lk.lockType == LockType.IS || lk.lockType == LockType.IX) {
//                    res.add(lockContext.name);
//                    break;
//                }
//            }
//        }
//        for(Map.Entry<String, LockContext> entry : lockContext.children.entrySet()) {
//            res.addAll(recursiveGetSisDescendants(entry.getValue(), transaction));
//        }
//        return res;
//    }

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

    /**
     * ?????? numChildLocks
     * ??????????????? getNumChildren ?????????????????????????????????
     */
    private void updateNumChildLocks(TransactionContext transaction, List<ResourceName> updateResourceNameList, int change) {
        // ???????????? resource ??? parent lockContext ????????????
        for(ResourceName currName : updateResourceNameList) {
            LockContext parentContext = fromResourceName(lockman, currName).parentContext();
            if(parentContext == null) continue;
            Map<Long, Integer> map = parentContext.numChildLocks;
            map.putIfAbsent(transaction.getTransNum(), 0);
            assert(map.get(transaction.getTransNum())+change >= 0);
            map.put(transaction.getTransNum(), map.get(transaction.getTransNum())+change);
        }
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

