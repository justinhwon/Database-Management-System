package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        // TODO(hw3_part1): implement

        // create new run
        Run sortedRun = createRun();

        // convert run to list
        List<Record> recordList = new ArrayList<Record>();

        Iterator<Record> runIterator = run.iterator();

        while (runIterator.hasNext()) {
            recordList.add(runIterator.next());
        }

        // sort list
        recordList.sort(comparator);

        // add records to new run
        sortedRun.addRecords(recordList);


        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        // TODO(hw3_part1): implement

        //Pair<Record, Integer>

        // create new run
        Run mergedRun = createRun();

        // create priority queue
        PriorityQueue<Pair<Record, Integer>> recordQueue = new PriorityQueue<Pair<Record, Integer>>(new RecordPairComparator());

        // create list of iterators of each run
        List<Iterator<Record>> iteratorList = new ArrayList<Iterator<Record>>();

        // put one record from each run into Priority Queue
        int numRuns = runs.size();
        for (int i = 0; i < runs.size(); i++) {
            Run run = runs.get(i);
            Iterator<Record> runIterator = run.iterator();
            // add pair of <record, run number> to queue
            Pair<Record, Integer> newPair = new Pair(runIterator.next(), i);
            recordQueue.add(newPair);
            // add iterator to list of iterators
            iteratorList.add(runIterator);
        }

        // created mergedRun from priority queue
        while(!recordQueue.isEmpty()){
            // get next smallest element from queue
            Pair<Record, Integer> nextPair = recordQueue.poll();
            int runNum = nextPair.getSecond();
            Record record = nextPair.getFirst();

            // convert record to list
            List<Record> recordList = new ArrayList<Record>();
            recordList.add(record);

            // add into new merged run
            mergedRun.addRecords(recordList);

            // get new record from run into priority queue
            Run run = runs.get(runNum);
            Iterator<Record> runIterator = iteratorList.get(runNum);
            // add pair of <record, run number> to queue
            if (runIterator.hasNext()){
                Pair<Record, Integer> newPair = new Pair(runIterator.next(), runNum);
                recordQueue.add(newPair);
            }
        }


        return mergedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(hw3_part1): implement

        // make list of runs
        List<Run> runList = new ArrayList<Run>();

        // get number of input buffers and number of runs
        int N = runs.size();
        int numInputBufs = numBuffers - 1;

        // merge N runs at a time
        for(int i=0; i < Math.ceil((double) N / numInputBufs); i++){
            //if less than N runs left
            if ( i == N / numInputBufs){
                List<Run> nextRuns = runs.subList(i*numInputBufs, N);
                Run mergeRun = mergeSortedRuns(nextRuns);
                runList.add(mergeRun);
            }
            //otherwise merge N runs
            else{
                List<Run> nextRuns = runs.subList(i*numInputBufs, (i+1)*numInputBufs);
                Run mergeRun = mergeSortedRuns(nextRuns);
                runList.add(mergeRun);
            }
        }

        // return list of runs
        return runList;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        // TODO(hw3_part1): implement

        /**
         1. I use transaction.getPageIterator to get a page iterator.

         2. loop through the page iterator to create record iterator with B pages from transaction.getBlockIterator.

         3. create runs with createRunFromIterator

         4. use sortrun to sort the list of runs I created from last step

         5. put the list through a mergepass loop to reduce the size of the list to 1

         6. lastly, put the single item in the list to mergesortedruns to create the final run to output.

         7. get the tablename from the final run.

         */

        // get a page iterator
        BacktrackingIterator<Page> pageIterator = transaction.getPageIterator(this.tableName);

        // make list of runs
        List<Run> runList = new ArrayList<Run>();


        // make sorted runs of all pages
        while(pageIterator.hasNext()) {
            // get a block iterator using the page iterator
            BacktrackingIterator<Record> blockIterator = transaction.getBlockIterator(this.tableName, pageIterator, this.numBuffers);

            // create run from the block iterator
            Run newRun = createRunFromIterator(blockIterator);

            // sort the run
            Run sortedRun = sortRun(newRun);

            //insert run into list
            runList.add(sortedRun);
        }

        // merge the sorted runs
        List<Run> sortedFile = mergePass(runList);

        // do as many passes as necessary until only one run
        while (sortedFile.size() > 1){
            sortedFile = mergePass(sortedFile);
        }

        // get the run from the list of size 1
        Run sortedTable = sortedFile.get(0);

        return sortedTable.tableName(); // TODO(hw3_part1): replace this!
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

