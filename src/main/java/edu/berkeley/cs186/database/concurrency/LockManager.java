package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
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

        // TODO(hw4_part1): You may add helper methods here if you wish

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(hw4_part1): You may add helper methods here if you wish
    private List<Lock> getTransactionLocks(TransactionContext transaction) {
        transactionLocks.putIfAbsent(transaction.getTransNum(), new ArrayList<>());
        return transactionLocks.get(transaction.getTransNum());
    }

    public boolean getNextLockInQueue(TransactionContext transaction, ResourceName name,
                        LockType lockType, Lock newLock, LockRequest nextLockReq) throws DuplicateLockRequestException {
        // Get the ResourceEntry of the resource
        ResourceEntry resourceEntry = getResourceEntry(name);

        // List of currently granted locks on the resource.
        //List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        //Deque<LockRequest> waitingQueue = new ArrayDeque<>();
        // transactionLocks is a mapping from transaction number to a list of lock
        // objects held by that transaction.
        //private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

        // Get the locks of the transaction
        List<Lock> transLocks = getTransactionLocks(transaction);

        // Get the locks and waiting queue of the resource
        List<Lock> resourceLocks = resourceEntry.locks;
        Deque<LockRequest> resourceQueue = resourceEntry.waitingQueue;

        /*
        // if a lock on resource from same transaction exists, throw error
        for(Lock lock:resourceLocks){
            if(lock.transactionNum == transaction.getTransNum()){
                throw new DuplicateLockRequestException("a lock on NAME is already held by TRANSACTION");
            }
        }
        */

        // do acquire and release if necessary
        if(!nextLockReq.releasedLocks.isEmpty()){
            // get list of compatibilities with current locks
            List<Boolean> compList = new ArrayList<>();
            for(Lock lock:resourceLocks){
                // check compatibility with all locks besides the one to be upgraded
                if (lock.transactionNum != transaction.getTransNum()){
                    compList.add(LockType.compatible(lockType, lock.lockType));
                }
            }

            // if new lock not compatible with a current lock, block and add to front of resource queue
            if(compList.contains(false)){
                return false;
            }
            // otherwise get the lock, release releaseLocks, and allow transaction to continue
            else{

                // update lock value if lock already exists
                boolean oldLockExists = false;
                Lock oldLock = transLocks.get(0);
                for(Lock lock:resourceLocks){
                    if(lock.transactionNum == transaction.getTransNum()){
                        oldLock = lock;
                        oldLockExists = true;
                    }
                }

                if (oldLockExists){
                    int resLockIndex = resourceLocks.indexOf(oldLock);
                    resourceLocks.set(resLockIndex, newLock);
                    int transLockIndex = transLocks.indexOf(oldLock);
                    transLocks.set(transLockIndex, newLock);
                }
                else{
                    resourceLocks.add(newLock);
                    transLocks.add(newLock);
                }


                // remove the necessary locks
                for (Lock toBeReleased:nextLockReq.releasedLocks){
                    ResourceName nameToRelease = toBeReleased.name;
                    release(transaction, nameToRelease);
                    // remove locks from queue of resources
                    //getResourceEntry(toBeReleased.name).waitingQueue.remove(toBeReleased);
                }

                // check next queue

                return true;
            }

        }

        boolean isPromote = false;
        for(Lock lock:resourceLocks){
            if(lock.transactionNum == transaction.getTransNum()){
                isPromote = true;
            }
        }

        if(isPromote){
            // get the lock to be promoted
            Lock oldLock = transLocks.get(0);
            for(Lock lock:resourceLocks){
                if(lock.transactionNum == transaction.getTransNum()){
                    oldLock = lock;
                }
            }

            // get list of compatibilities with current locks
            List<Boolean> compList = new ArrayList<>();
            for(Lock lock:resourceLocks){
                // check compatibility with all locks besides the one to be upgraded
                if (lock.transactionNum != transaction.getTransNum()){
                    compList.add(LockType.compatible(lockType, lock.lockType));
                }
            }

            // if new lock not compatible with a current lock, do nothing
            if(compList.contains(false)){
                return false;
            }
            // otherwise upgrade the lock and allow transaction to continue
            else{
                int resLockIndex = resourceLocks.indexOf(oldLock);
                resourceLocks.set(resLockIndex, newLock);
                int transLockIndex = transLocks.indexOf(oldLock);
                transLocks.set(transLockIndex, newLock);
                transaction.unblock();
                return true;
            }
        }


        // get list of compatibilities with current locks
        List<Boolean> compList = new ArrayList<>();
        for(Lock lock:resourceLocks){
            compList.add(LockType.compatible(lock.lockType, lockType));
        }


        // if new lock not compatible with a current lock, keep waiting
        if(compList.contains(false)){
            return false;
        }
        // otherwise get the lock and allow transaction to continue
        else{
            // add locks to resource and remove from queue
            resourceLocks.add(newLock);
            transLocks.add(newLock);
            resourceQueue.removeFirst();
            // unblock transaction
            transaction.unblock();
            return true;
        }

    }


    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        //case where releaseLocks is empty
        if (releaseLocks.isEmpty()){
            acquire(transaction, name, lockType);
            return;
        }


        synchronized (this) {
            // Get the ResourceEntry of the resource
            ResourceEntry resourceEntry = getResourceEntry(name);

            // Get the locks of the transaction
            List<Lock> transLocks = getTransactionLocks(transaction);

            // Get the locks and waiting queue of the resource
            List<Lock> resourceLocks = resourceEntry.locks;
            Deque<LockRequest> resourceQueue = resourceEntry.waitingQueue;


            // if a lock on resource from same transaction exists and isn't being release, throw error
            for(Lock lock:resourceLocks){
                if(lock.transactionNum == transaction.getTransNum() && !releaseLocks.contains(lock.name)){
                    throw new DuplicateLockRequestException("a lock on NAME is already held by TRANSACTION");
                }
            }


            // get list of compatibilities with current locks
            List<Boolean> compList = new ArrayList<>();
            for(Lock lock:resourceLocks){
                // check compatibility with all locks besides the one to be upgraded
                if (lock.transactionNum != transaction.getTransNum()){
                    compList.add(LockType.compatible(lockType, lock.lockType));
                }
            }

            // convert list of resources to list of locks matching the transaction
            List<Lock> releaseList = new ArrayList<>();
            for(ResourceName rname:releaseLocks){
                List<Lock> resLocks = getResourceEntry(rname).locks;
                for(Lock rlock:resLocks){
                    if (rlock.transactionNum == transaction.getTransNum()){
                        releaseList.add(rlock);
                    }
                }
            }

            // throw error if no lock on a name in RELEASELOCKS is held by TRANSACTION
            if(releaseList.isEmpty()){
                throw new NoLockHeldException("no lock on a name in RELEASELOCKS is held by TRANSACTION");
            }

            // create LockRequest object for the lock request
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, newLock, releaseList);

            // if new lock not compatible with a current lock, block and add to front of resource queue
            if(compList.contains(false)){
                resourceQueue.addFirst(request);
            }
            // otherwise get the lock, release releaseLocks, and allow transaction to continue
            else{

                // update lock value if lock already exists
                boolean oldLockExists = false;
                Lock oldLock = transLocks.get(0);
                for(Lock lock:resourceLocks){
                    if(lock.transactionNum == transaction.getTransNum()){
                        oldLock = lock;
                        oldLockExists = true;
                    }
                }

                if (oldLockExists){
                    int resLockIndex = resourceLocks.indexOf(oldLock);
                    resourceLocks.set(resLockIndex, newLock);
                    int transLockIndex = transLocks.indexOf(oldLock);
                    transLocks.set(transLockIndex, newLock);
                }
                else{
                    resourceLocks.add(newLock);
                    transLocks.add(newLock);
                }


                // remove the necessary locks
                for (ResourceName toBeReleased:releaseLocks){
                    release(transaction, toBeReleased);
                    // remove locks from queue of resources
                    //getResourceEntry(toBeReleased.name).waitingQueue.remove(toBeReleased);
                }

                // check next queue

                return;
            }
            transaction.prepareBlock();
        }

        transaction.block();
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {

            // Get the ResourceEntry of the resource
            ResourceEntry resourceEntry = getResourceEntry(name);

            // List of currently granted locks on the resource.
            //List<Lock> locks = new ArrayList<>();
            // Queue for yet-to-be-satisfied lock requests on this resource.
            //Deque<LockRequest> waitingQueue = new ArrayDeque<>();
            // transactionLocks is a mapping from transaction number to a list of lock
            // objects held by that transaction.
            //private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

            // Get the locks of the transaction
            List<Lock> transLocks = getTransactionLocks(transaction);

            // Get the locks and waiting queue of the resource
            List<Lock> resourceLocks = resourceEntry.locks;
            Deque<LockRequest> resourceQueue = resourceEntry.waitingQueue;


            // if a lock on resource from same transaction exists, throw error
            for(Lock lock:resourceLocks){
                if(lock.transactionNum == transaction.getTransNum()){
                    throw new DuplicateLockRequestException("a lock on NAME is already held by TRANSACTION");
                }
            }


            // get list of compatibilities with current locks
            List<Boolean> compList = new ArrayList<>();
            for(Lock lock:resourceLocks){
                compList.add(LockType.compatible(lock.lockType, lockType));
            }

            // create LockRequest object for the lock request
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, newLock);


            // if new lock not compatible with a current lock, block and add to resource queue
            if(compList.contains(false)){
                resourceQueue.addLast(request);
            }
            // or there is queue, block transaction and place request at back of resource queue
            else if(!resourceQueue.isEmpty()){
                resourceQueue.addLast(request);
            }
            // otherwise get the lock and allow transaction to continue
            else{
                resourceLocks.add(newLock);
                transLocks.add(newLock);
                return;
            }
            transaction.prepareBlock();

        }
        // block transaction if lock request was put in the queue
        transaction.block();
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {

            // Get the ResourceEntry of the resource
            ResourceEntry resourceEntry = getResourceEntry(name);
            // Get the locks of the transaction
            List<Lock> transLocks = getTransactionLocks(transaction);

            // Get the locks and waiting queue of the resource
            List<Lock> resourceLocks = resourceEntry.locks;
            Deque<LockRequest> resourceQueue = resourceEntry.waitingQueue;

            // the lock that corresponds to the transaction (initialize to first)
            Lock currLock = resourceLocks.get(0);


            // if no lock on resource from same transaction exists, throw error
            boolean lockExists = false;
            for(Lock lock:resourceLocks){
                if(lock.transactionNum == transaction.getTransNum()){
                    lockExists = true;
                    // set currLock to the lock matching the transaction
                    currLock = lock;
                }
            }
            if (!lockExists){
                throw new DuplicateLockRequestException("no lock on NAME is held by TRANSACTION");
            }

            //1. Remove L from the list of locks that T is holding as well as the list of locks taking effect on R.
            resourceLocks.remove(currLock);
            transLocks.remove(currLock);



            //2. Check out R's waitQueue, handle a LockRequest if possible.

            // handle next LockRequest if resourceQueue not empty
            while (!resourceQueue.isEmpty()){
                //get next lockRequest
                LockRequest nextLockReq = resourceQueue.getFirst();
                // get corresponding transaction, resource name, lock
                TransactionContext nextTrans = nextLockReq.transaction;
                Lock nextLock = nextLockReq.lock;
                ResourceName nextName = nextLock.name;
                LockType nextLockType = nextLock.lockType;

                // get the next lock in the queue
                if(!getNextLockInQueue(nextTrans, nextName, nextLockType, nextLock, nextLockReq)){
                    // if no more to dequeue, break
                    break;
                }
            }



            return;
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            // Get the ResourceEntry of the resource
            ResourceEntry resourceEntry = getResourceEntry(name);

            // Get the locks of the transaction
            List<Lock> transLocks = getTransactionLocks(transaction);

            // Get the locks and waiting queue of the resource
            List<Lock> resourceLocks = resourceEntry.locks;
            Deque<LockRequest> resourceQueue = resourceEntry.waitingQueue;

            // if TRANSACTION already has a NEWLOCKTYPE lock on NAME, throw exception
            for (Lock lock:transLocks){
                if (lock.name == name && lock.lockType == newLockType){
                    throw new DuplicateLockRequestException("TRANSACTION already has a NEWLOCKTYPE lock on NAME");
                }
            }

            //if TRANSACTION has no lock on NAME, throw exception
            boolean hasLockOnName = false;
            for (Lock lock:transLocks){
                if (lock.name == name){
                    hasLockOnName = true;
                }
            }
            if (!hasLockOnName){
                throw new NoLockHeldException("TRANSACTION has no lock on NAME");
            }

            // get the lock to be promoted
            Lock oldLock = transLocks.get(0);
            for(Lock lock:resourceLocks){
                if(lock.transactionNum == transaction.getTransNum()){
                    oldLock = lock;
                }
            }

            //if the requested lock type is not a promotion, throw exception
            if (!LockType.substitutable(newLockType, oldLock.lockType)){
                throw new InvalidLockException("the requested lock type is not a promotion");
            }


            // get list of compatibilities with current locks
            List<Boolean> compList = new ArrayList<>();
            for(Lock lock:resourceLocks){
                // check compatibility with all locks besides the one to be upgraded
                if (lock.transactionNum != transaction.getTransNum()){
                    compList.add(LockType.compatible(newLockType, lock.lockType));
                }
            }

            // create LockRequest object for the lock request
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, newLock);

            // if new lock not compatible with a current lock, block and add to resource queue
            if(compList.contains(false)){
                resourceQueue.addFirst(request);
            }
            // otherwise upgrade the lock and allow transaction to continue
            else{
                int resLockIndex = resourceLocks.indexOf(oldLock);
                resourceLocks.set(resLockIndex, newLock);
                int transLockIndex = transLocks.indexOf(oldLock);
                transLocks.set(transLockIndex, newLock);
                return;
            }
            transaction.prepareBlock();
        }
        transaction.block();
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(hw4_part1): implement

        // Get the ResourceEntry of the resource
        ResourceEntry resourceEntry = getResourceEntry(name);

        // Get the locks of the resource
        List<Lock> resourceLocks = resourceEntry.locks;

        // Initialize LockType to NL (not found)
        LockType lockType = LockType.NL;

        // find lock matching the transaction
        for( Lock lock:resourceLocks ){
            if (lock.transactionNum == transaction.getTransNum()){
                lockType = lock.lockType;
            }
        }

        return lockType;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
