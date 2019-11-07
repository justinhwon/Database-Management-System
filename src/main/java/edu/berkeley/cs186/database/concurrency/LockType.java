package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        int index1;
        int index2;

        // set a index
        if (a == NL){
            index1 = 0;
        }
        else if (a == IS){
            index1 = 1;
        }
        else if (a == IX){
            index1 = 2;
        }
        else if (a == S){
            index1 = 3;
        }
        else if (a == SIX){
            index1 = 4;
        }
        else{
            index1 = 5;
        }

        // set b index
        if (b == NL){
            index2 = 0;
        }
        else if (b == IS){
            index2 = 1;
        }
        else if (b == IX){
            index2 = 2;
        }
        else if (b == S){
            index2 = 3;
        }
        else if (b == SIX){
            index2 = 4;
        }
        else{
            index2 = 5;
        }

        //compatibility matrix
        boolean[][] compMatrix = {
                {true, true, true, true, true, true},
                {true, true, true, true, true, false},
                {true, true, true, false, false, false},
                {true, true, false, true, false, false},
                {true, true, false, false, false, false},
                {true, false, false, false, false, false},
        };

        // get compatibility from matrix
        boolean compatible = compMatrix[index1][index2];

        return compatible;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        int index1;
        int index2;

        // set a index
        if (parentLockType == NL){
            index1 = 0;
        }
        else if (parentLockType == IS){
            index1 = 1;
        }
        else if (parentLockType == IX){
            index1 = 2;
        }
        else if (parentLockType == S){
            index1 = 3;
        }
        else if (parentLockType == SIX){
            index1 = 4;
        }
        else{
            index1 = 5;
        }

        // set b index
        if (childLockType == NL){
            index2 = 0;
        }
        else if (childLockType == IS){
            index2 = 1;
        }
        else if (childLockType == IX){
            index2 = 2;
        }
        else if (childLockType == S){
            index2 = 3;
        }
        else if (childLockType == SIX){
            index2 = 4;
        }
        else{
            index2 = 5;
        }

        //parent matrix
        boolean[][] parentMatrix = {
                {true, false, false, false, false, false},
                {true, true, false, true, false, false},
                {true, true, true, true, true, true},
                {true, false, false, false, false, false},
                {true, false, true, false, false, true},
                {true, false, false, false, false, false},
        };

        // get compatibility from matrix
        boolean parentPoss = parentMatrix[index1][index2];

        return parentPoss;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        int index1;
        int index2;

        // set a index
        if (substitute == NL){
            index1 = 0;
        }
        else if (substitute == IS){
            index1 = 1;
        }
        else if (substitute == IX){
            index1 = 2;
        }
        else if (substitute == S){
            index1 = 3;
        }
        else if (substitute == SIX){
            index1 = 4;
        }
        else{
            index1 = 5;
        }

        // set b index
        if (required == NL){
            index2 = 0;
        }
        else if (required == IS){
            index2 = 1;
        }
        else if (required == IX){
            index2 = 2;
        }
        else if (required == S){
            index2 = 3;
        }
        else if (required == SIX){
            index2 = 4;
        }
        else{
            index2 = 5;
        }

        //substitute matrix
        boolean[][] subMatrix = {
                {true, false, false, false, false, false},
                {true, true, false, false, false, false},
                {true, true, true, false, false, false},
                {true, true, false, true, false, false},
                {true, true, true, true, true, false},
                {true, true, true, true, true, true},
        };

        // get substitutability from matrix
        boolean subPoss = subMatrix[index1][index2];

        return subPoss;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

