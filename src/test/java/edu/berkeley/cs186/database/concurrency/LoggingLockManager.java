package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingLockManager extends LockManager {
    public List<String> log = Collections.synchronizedList(new ArrayList<>());
    private boolean logging = false;
    private boolean suppressInternal = true;
    private boolean suppressStatus = false;
    private Map<Long, LockContext> contexts = new HashMap<>();
    private Map<Long, Boolean> loggingOverride = new ConcurrentHashMap<>();

    @Override
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LoggingLockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    @Override
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks) {
        StringBuilder estr = new StringBuilder("acquire-and-release ");
        estr.append(transaction.getTransNum()).append(' ').append(name).append(' ').append(lockType);
        releaseLocks.sort(Comparator.comparing(ResourceName::toString));
        estr.append(" [");
        boolean first = true;
        for (ResourceName n : releaseLocks) {
            if (!first) {
                estr.append(", ");
            }
            estr.append(n);
            first = false;
        }
        estr.append(']');
        emit(estr.toString());

        loggingOverride.put(Thread.currentThread().getId(), !suppressInternal);
        try {
            super.acquireAndRelease(transaction, name, lockType, releaseLocks);
        } finally {
            loggingOverride.remove(Thread.currentThread().getId());
        }
    }

    @Override
    public void acquire(TransactionContext transaction, ResourceName name, LockType type) {
        emit("acquire " + transaction.getTransNum() + " " + name + " " + type);

        loggingOverride.put(Thread.currentThread().getId(), !suppressInternal);
        try {
            super.acquire(transaction, name, type);
        } finally {
            loggingOverride.remove(Thread.currentThread().getId());
        }
    }

    @Override
    public void release(TransactionContext transaction, ResourceName name) {
        emit("release " + transaction.getTransNum() + " " + name);

        loggingOverride.put(Thread.currentThread().getId(), !suppressInternal);
        try {
            super.release(transaction, name);
        } finally {
            loggingOverride.remove(Thread.currentThread().getId());
        }
    }

    @Override
    public void promote(TransactionContext transaction, ResourceName name, LockType newLockType) {
        emit("promote " + transaction.getTransNum() + " " + name + " " + newLockType);

        loggingOverride.put(Thread.currentThread().getId(), !suppressInternal);
        try {
            super.promote(transaction, name, newLockType);
        } finally {
            loggingOverride.remove(Thread.currentThread().getId());
        }
    }

    public void startLog() {
        logging = true;
    }

    public void endLog() {
        logging = false;
    }

    public void clearLog() {
        log.clear();
    }

    public boolean isLogging() {
        return logging;
    }

    void suppressInternals(boolean toggle) {
        suppressInternal = toggle;
    }

    public void suppressStatus(boolean toggle) {
        suppressStatus = toggle;
    }

    void emit(String s) {
        long tid = Thread.currentThread().getId();
        if (suppressStatus && !s.startsWith("acquire") && !s.startsWith("promote") &&
                !s.startsWith("release")) {
            return;
        }
        if ((loggingOverride.containsKey(tid) ? loggingOverride.get(tid) : logging)) {
            log.add(s);
        }
    }
}