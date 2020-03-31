package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private int called;

    private TransactionId t;

    private  OpIterator child;

    private int tableId;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        called = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeAr = new Type[1];
        String[] fieldAr = new String[1];
        typeAr[0] = Type.INT_TYPE;
        fieldAr[0] = "aaa";
        TupleDesc td = new TupleDesc(typeAr, fieldAr);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int num = 0;
        if(called == 1){
            return null;
        }
        called = 1;
        while(child.hasNext()){
            num++;
            try {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Type[] typeAr = new Type[1];
        String[] fieldAr = new String[1];
        typeAr[0] = Type.INT_TYPE;
        fieldAr[0] = "";
        TupleDesc td = new TupleDesc(typeAr,fieldAr);
        Tuple tp = new Tuple(td);
        tp.setField(0, new IntField(num));
        return tp;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] children = new OpIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        children[0] = child;
    }
}
