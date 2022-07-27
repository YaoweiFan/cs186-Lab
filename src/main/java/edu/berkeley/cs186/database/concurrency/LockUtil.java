package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // Phase1: Ensuring that we have the appropriate locks on ancestors
        // Phase2: Acquiring the lock on the resource
        if(requestType == LockType.NL) return;

        // effectiveLockType 不会是 intend lock
        // 这种情况下应该不需要做什么吧，连本资源的锁也不上
        if(LockType.substitutable(effectiveLockType, requestType)) return;
        // 执行到这儿说明需要进一步争取资源了
        if(explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        if(explicitLockType == LockType.IS || explicitLockType == LockType.IX || explicitLockType == LockType.SIX) {
            // 这种情况下当前锁是 intent lock，就意味着子资源可能还上有锁
            lockContext.escalate(transaction);
            // 这时候当前的锁只有可能是 S 或 X
            if(LockType.substitutable(lockContext.getExplicitLockType(transaction), requestType)) return;
        }
        // 执行到这里，explicitLockType 只可能是 S 或 NL，而且 requestType 只可能是 X 或 S、X
        // 先申请 intent lock
        if(requestType == LockType.S) upToUpdateLocks(parentContext, transaction, LockType.IS);
        if(requestType == LockType.X) upToUpdateLocks(parentContext, transaction, LockType.IX);
        // 再在当前资源上锁
        if(lockContext.getExplicitLockType(transaction) == LockType.S && requestType == LockType.X) {
            lockContext.promote(transaction, requestType);
            return;
        }
        if(lockContext.getExplicitLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, requestType);
            return;
        }

    }


    // TODO(proj4_part2) add any helper methods you want
    private static void upToUpdateLocks(LockContext currContext, TransactionContext transaction, LockType requestType) {
        assert(requestType == LockType.IS || requestType == LockType.IX);
        if(currContext == null) return;
        upToUpdateLocks(currContext.parentContext(), transaction, requestType);
        LockType currLockType = currContext.getExplicitLockType(transaction);
        if(!LockType.substitutable(currLockType, requestType)) {
            if(currLockType == LockType.NL) {
                currContext.acquire(transaction, requestType);
            } else {
                currContext.promote(transaction, requestType);
            }
        }
    }
}
