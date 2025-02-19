package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(hw3_part1): implement

            // if no more blocks, set iterators to null
            if (!leftIterator.hasNext()){
                leftRecordIterator = null;
                leftRecord = null;
            } else{
                // set leftRecordIterator to the next block
                this.leftRecordIterator = getBlockIterator(this.getLeftTableName(),
                        this.leftIterator, BNLJOperator.this.numBuffers - 2);
                //mark beginning of block
                leftRecordIterator.markNext();


                leftRecord = leftRecordIterator.next();


            }

        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(hw3_part1): implement

            // if no more pages in rightIterator, set rightRecordIterator to null
            if (!rightIterator.hasNext()){
                rightRecordIterator = null;
            } else{
                // set rightRecordIterator to the next page
                this.rightRecordIterator = getBlockIterator(this.getRightTableName(),
                        this.rightIterator, 1);
                //mark beginning of page
                this.rightRecordIterator.markNext();
            }

        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(hw3_part1): implement

            //reset nextRecord to null
            //this.nextRecord = null;

            /**
            //instantiate first left record if needed, set as original marker
            if(leftRecord == null){
                leftRecord = leftRecordIterator.next();
                leftRecordIterator.markPrev();
            }
             */

            // get a new nextRecord
            this.nextRecord = null;

            //run until exit condition
            while(true){

                //if no more leftRecord then finished
                if(leftRecord == null){
                    throw new NoSuchElementException("No new record to fetch");
                }

                //if finished with everything
                if (!rightRecordIterator.hasNext() && !rightIterator.hasNext() && !leftRecordIterator.hasNext() && !leftIterator.hasNext()){
                    throw new NoSuchElementException("No new record to fetch");
                }
                //finished block on all pages of S
                else if(!rightRecordIterator.hasNext() && !rightIterator.hasNext() && !leftRecordIterator.hasNext() && leftIterator.hasNext()){
                    // get new R block and mark the beginning
                    fetchNextLeftBlock();
                    //mark beginning of block
                    //leftRecordIterator.markNext();
                    // reset page S to the beginning
                    rightIterator.reset();
                    fetchNextRightPage();
                }

                //a block on next page of S
                else if(!rightRecordIterator.hasNext() && !leftRecordIterator.hasNext() && rightIterator.hasNext()){
                    // if finished all R records on a page S, move to next page of S and reset R
                    fetchNextRightPage();
                    //mark beginning of page
                    //rightRecordIterator.markNext();
                    leftRecordIterator.reset();
                    //update leftRecord
                    leftRecord = leftRecordIterator.next();
                }

                //a standard block on one page of S (every record in Br)
                else if(!rightRecordIterator.hasNext() && leftRecordIterator.hasNext()){
                    //if reached the end of an S page and not finished block R, iterate R
                    leftRecord = leftRecordIterator.next();
                    //reset s to same page if run out while still iterating through R records
                    rightRecordIterator.reset();
                }
                else{
                    // get the next right (s) record
                    Record rightRecord = rightRecordIterator.next();


                    //if θ(ri ,sj ):
                    //yield <ri, sj>

                    DataBox leftValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftValue.equals(rightValue)) {
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                        return;
                    }
                }

            }




        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
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
    }
}
