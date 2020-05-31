package simpledb;

public class Lock {
    private boolean isread;//1为shared锁，2为exclusive锁
    private TransactionId tid;
    private Permissions perm;

    public Lock(TransactionId tid, boolean isread, Permissions perm)
    {
        this.tid = tid;
        this.isread = isread;
        this.perm = perm;
    }

    public boolean is_shared()
    {
        return this.isread;
    }

    public Permissions getPerm()
    {
        return this.perm;
    }

    public TransactionId getId()
    {
        return this.tid;
    }

    public void setLockType(Permissions perm){
        this.perm = perm;
    }

    public void modify(boolean modi)
    {
        this.isread = modi;
    }

    public boolean equals(Object o)
    {
        if(o==null || getClass() != o.getClass())
        {
            return false;
        }
        Lock lock = (Lock)o;
        return tid.equals(lock.tid) && perm.equals(lock.perm) && (perm ==lock.perm);
    }



}
