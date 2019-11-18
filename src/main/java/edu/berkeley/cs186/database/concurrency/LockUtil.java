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


        // AUTO-ESCALATE FUNCTIONALITY

        // if parent exists, check if escalatable and whether you should escalate (only tables are escalatable)
        if(lockContext.parentContext() != null) {
            boolean escAble = lockContext.parentContext().getAutoEscalatable();

            // if the parent is escalatable (guaranteed to be a table), check if you should escalate
            if(escAble){

                // only escalate if the table has at least 10 pages
                if (lockContext.parentContext().capacity() >= 10) {

                    // if at least 20% of pages held, escalate the parent
                    if (lockContext.parentContext().saturation(transaction) >= 0.20) {

                        lockContext.parentContext().escalate(transaction);

                    }
                }
            }
        }

        // END OF AUTO-ESCALATE FUNCTIONALITY

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
                // if there's already intent lock in parent or no parent, just acquire the lock
                if (lockContext.parentContext() == null || LockType.canBeParentLock(lockContext.parentContext().getExplicitLockType(transaction), lockType)){
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
                // if there's already intent lock in parent or no parent, just acquire the lock
                if (lockContext.parentContext() == null || lockContext.parentContext().getExplicitLockType(transaction) == LockType.IX){
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
                        if(parent.getExplicitLockType(transaction) == LockType.IS){
                            parent.promote(transaction, LockType.IX);
                        }
                        else if(parent.getExplicitLockType(transaction) != LockType.IX){
                            parent.acquire(transaction, LockType.IX);
                        }
                    }

                    //acquire the original lock you were going for
                    lockContext.acquire(transaction, lockType);
                }
            }
        }

        // CASE 2 - current lock IS
        else if(lockContext.getEffectiveLockType(transaction) == LockType.IS){
            // if requesting S lock, just escalate
            if (lockType == LockType.S){
                lockContext.escalate(transaction);
            }
            // if requesting X lock, escalate, update parents, then promote to X
            else{
                lockContext.escalate(transaction);

                // create list of parents
                List<LockContext> parentContexts = new ArrayList<>();

                // get list of parents from top to bottom
                LockContext currParent = lockContext.parentContext();
                while (currParent != null){
                    parentContexts.add(0, currParent);
                    currParent = currParent.parentContext();
                }

                // if parent not a sufficient intent lock, update to IX
                for (LockContext parent:parentContexts){
                    //if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)){
                    if(parent.getExplicitLockType(transaction) != LockType.IX && parent.getExplicitLockType(transaction) != LockType.SIX){
                        parent.promote(transaction, LockType.IX);
                    }
                }

                lockContext.promote(transaction, LockType.X);
            }

        }

        // CASE 2 - current lock IX
        else if(lockContext.getEffectiveLockType(transaction) == LockType.IX){
            // if requesting X lock, just escalate
            if (lockType == LockType.X){
                lockContext.escalate(transaction);
            }
            // if requesting S lock, then promote to SIX
            else{
                lockContext.promote(transaction, LockType.SIX);
            }

        }

        // CASE 3 - current lock S (then upgrade to X)
        else if(lockContext.getEffectiveLockType(transaction) == LockType.S){
            // if explicit lock is S update parents then promote to X
            if(lockContext.getExplicitLockType(transaction) == LockType.S){

                // create list of parents
                List<LockContext> parentContexts = new ArrayList<>();

                // get list of parents from top to bottom
                LockContext currParent = lockContext.parentContext();
                while (currParent != null){
                    parentContexts.add(0, currParent);
                    currParent = currParent.parentContext();
                }

                // if parent not a sufficient intent lock, update to IX
                for (LockContext parent:parentContexts){
                    //if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)){
                    if(parent.getExplicitLockType(transaction) != LockType.IX && parent.getExplicitLockType(transaction) != LockType.SIX){
                        parent.promote(transaction, LockType.IX);
                    }
                }

                lockContext.promote(transaction, LockType.X);
            }
            // if explicit not S then is NL so set all parents to IX and set lock to S
            else{
                // create list of parents
                List<LockContext> parentContexts = new ArrayList<>();

                // get list of parents from top to bottom
                LockContext currParent = lockContext.parentContext();
                while (currParent != null){
                    parentContexts.add(0, currParent);
                    currParent = currParent.parentContext();
                }

                // if parent not a sufficient intent lock, update to IX
                for (LockContext parent:parentContexts){
                    //if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)){
                    if(parent.getExplicitLockType(transaction) != LockType.IX && parent.getExplicitLockType(transaction) != LockType.SIX){
                        // promote to SIX if parent is S
                        if (parent.getExplicitLockType(transaction) == LockType.S){
                            parent.promote(transaction, LockType.SIX);
                        }
                        // else promote to IX
                        else{
                            parent.promote(transaction, LockType.IX);
                        }
                    }
                }

                //acquire the original lock you were going for
                lockContext.acquire(transaction, lockType.X);
            }
        }

        // CASE 3 - current lock is SIX (then upgrade to X)
        else if(lockContext.getEffectiveLockType(transaction) == LockType.SIX){

            lockContext.escalate(transaction);
        }

        return;
    }

    // TODO(hw4_part2): add helper methods as you see fit
}
