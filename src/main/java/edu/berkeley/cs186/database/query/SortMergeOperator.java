package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(hw3_part1): implement

            // get iterator over sorted left table
            SortOperator leftTable = new SortOperator(SortMergeOperator.this.getTransaction(), getLeftTableName(), new LeftRecordComparator());
            String sortedLeftTable = leftTable.sort();
            this.leftIterator = SortMergeOperator.this.getRecordIterator(sortedLeftTable);

            //get iterator over sorted right table
            SortOperator rightTable = new SortOperator(SortMergeOperator.this.getTransaction(), getRightTableName(), new RightRecordComparator());
            String sortedRightTable = rightTable.sort();
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sortedRightTable);

            /*
            this.rightIterator = SortMergeOperator.this.getRecordIterator(this.getRightTableName());
            this.leftIterator = SortMergeOperator.this.getRecordIterator(this.getLeftTableName());
            */

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // No mark in the beginning
            this.marked = false;


            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }



        }


        // ADDED THIS FUNCTION, NOT PART OF SKELETON
        private void fetchNextRecord() {

            // reset nextRecord
            this.nextRecord = null;

            do {

                // get the key values
                DataBox r = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox s = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                // if there is no mark on s
                if (!marked) {
                    // if r < s
                    while (r.compareTo(s) < 0) {
                        // exit if nothing left
                        if (!this.leftIterator.hasNext()) {
                            leftRecord = null;
                        }
                        else{
                            // update leftRecord and r value
                            leftRecord = leftIterator.next();
                            r = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                        }

                    }
                    // if r > s
                    while (r.compareTo(s) > 0) {

                        // if run out of s, all r are bigger so exit
                        if(!this.rightIterator.hasNext()){
                            throw new NoSuchElementException("No new record to fetch");
                        }

                        // update rightRecord and s value
                        rightRecord = rightIterator.next();
                        s = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    }

                }
                // if r == s
                if (r.compareTo(s) == 0){
                    // once r = s, mark that s value
                    if (!marked){
                        rightIterator.markPrev();
                        this.marked = true;
                    }

                    // combine the 2 records
                    List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);

                    // update the return value
                    this.nextRecord = new Record(leftValues);

                    //check if s left
                    if (!leftIterator.hasNext() && !rightIterator.hasNext()){
                        throw new NoSuchElementException("No new record to fetch");
                    }
                    else if(!rightIterator.hasNext()){
                        rightIterator.reset();
                        rightRecord = rightIterator.next();
                        leftRecord = leftIterator.next();
                    }
                    else{
                        // advance s
                        rightRecord = rightIterator.next();
                        s = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    }


                }
                // if r!=s
                else{
                    // reset s to mark
                    rightIterator.reset();
                    rightRecord = rightIterator.next();
                    s = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    // advance r
                    leftRecord = leftIterator.next();
                    r = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    this.marked = false;
                }
            } while (this.nextRecord == null);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(hw3_part1): implement

            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(hw3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
