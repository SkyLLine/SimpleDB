package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    File f;
    TupleDesc td;

    public class HeapIterator implements DbFileIterator{
        private TransactionId tid;
        private int pgNo;
//        private HeapFile f;
        private Iterator<Tuple> iterator;

        public HeapIterator(TransactionId tid){
            this.tid = tid;
//            this.f = f;

        }
        public boolean hasNext() throws TransactionAbortedException, DbException {
            if(iterator == null){
                return false;
            }
            if(iterator.hasNext()){
                return true;
            }
            if(pgNo < numPages() - 1){
                pgNo = pgNo + 1;
                HeapPageId heapPageId = new HeapPageId(getId(), pgNo);
                Page page = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                HeapPage heapPage = (HeapPage)page;
                iterator = heapPage.iterator();
                return iterator.hasNext();
            }else{
                return false;
            }

        }
        public void open() throws TransactionAbortedException, DbException {
//            pgNo = pgNo + 1;
            pgNo = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), pgNo);
            Page page = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            HeapPage heapPage = (HeapPage)page;
            iterator = heapPage.iterator();
//            iterator = iterator1;

        }

        public void close(){
            iterator = null;

        }

        public Tuple next() throws TransactionAbortedException, DbException {
//            if(iterator == null){
//                throw new NoSuchElementException();
//            }
//            if(iterator.hasNext()){
//                return iterator.next();
//            }
//            else if(!iterator.hasNext() && pgNo < f.numPages() - 1){
//                pgNo = pgNo + 1;
//                HeapPageId heapPageId = new HeapPageId(f.getId(), pgNo);
//                Page page = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
//                HeapPage heapPage = (HeapPage)page;
//                Iterator<Tuple> iterator1 = heapPage.iterator();
//                iterator = iterator1;
//                return iterator.next();
//            }
//            return null;
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        public void rewind() throws TransactionAbortedException, DbException {
            close();
            open();

        }
    }

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return (f.toString()+td.toString()).hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile RAF = new RandomAccessFile(this.f, "r");
            HeapPageId heapPageId = (HeapPageId)pid;
            byte[] bytes = new byte[BufferPool.getPageSize()];
            RAF.seek(pid.getPageNumber() * BufferPool.getPageSize());
            RAF.read(bytes, 0, BufferPool.getPageSize());
            HeapPage heapPage = new HeapPage(heapPageId, bytes);
            return heapPage;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapIterator(tid);
    }

}

