package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        //If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
        if (transaction == null){
            return;
        }

        // if current lock is sufficient
        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)){
            return;
        }
        // CASE 1
        // if current lock is NL we just acquire a new lock and update parents as necessary
        else if (lockContext.getEffectiveLockType(transaction) == LockType.NL){

            // if new lock is type S
            if (lockType == LockType.S){
                // if there's already intent lock in parent, just acquire the lock
                if (LockType.canBeParentLock(lockContext.parentContext().getExplicitLockType(transaction), lockType)){
                    lockContext.acquire(transaction, lockType);
                }
                // if no parent intent locks, set intent locks on all parents as necessary
                else{
                    // create list of parents
                    List<LockContext> parentContexts = new ArrayList<>();

                    // get list of parents from top to bottom
                    LockContext currParent = lockContext.parentContext();
                    while (currParent != null){
                        parentContexts.add(0, currParent);
                        currParent = currParent.parentContext();
                    }

                    // if parent not a sufficient intent lock, update to IS
                    for (LockContext parent:parentContexts){
                        //if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)){
                        if(parent.getExplicitLockType(transaction) != LockType.IX && parent.getExplicitLockType(transaction) != LockType.IS){
                            parent.acquire(transaction, LockType.IS);
                        }
                    }

                    //acquire the original lock you were going for
                    lockContext.acquire(transaction, lockType);
                }

            }
            // if new lock type is X
            else if (lockType == LockType.X){
                // if there's already intent lock in parent, just promote the lock
                if (lockContext.parentContext().getExplicitLockType(transaction) == LockType.IX){
                    lockContext.acquire(transaction, lockType);
                }
                // if no parent intent locks, set intent locks on all parents as necessary
                else{
                    // create list of parents
                    List<LockContext> parentContexts = new ArrayList<>();

                    // get list of parents from top to bottom
                    LockContext currParent = lockContext.parentContext();
                    while (currParent != null){
                        parentContexts.add(0, currParent);
                        currParent = currParent.parentContext();
                    }

                    // if not a sufficient intent lock, update to IX
                    for (LockContext parent:parentContexts){
                        //if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)){
                        if(parent.getExplicitLockType(transaction) != LockType.IX){
                            parent.acquire(transaction, LockType.IX);
                        }
                    }

                    //acquire the original lock you were going for
                    lockContext.acquire(transaction, lockType);
                }
            }
        }

        // CASE 2

        return;
    }

    // TODO(hw4_part2): add helper methods as you see fit
}
