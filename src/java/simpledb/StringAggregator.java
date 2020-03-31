package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private int afield;

    private Type gbfieldtype;

    private Op what;

    private HashMap<Field, Integer> groupnum;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupnum = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupbyfield = null;
        if(gbfield == Aggregator.NO_GROUPING){
            groupbyfield = new IntField(Aggregator.NO_GROUPING);
        }else{
            groupbyfield = tup.getField(gbfield);
        }
        int num = 0;
        if(groupnum.containsKey(groupbyfield)){
            num = groupnum.get(groupbyfield);
        }else{
            groupnum.put(groupbyfield, 0);
        }
        num = groupnum.get(groupbyfield);
        if(what == Op.COUNT){
            num++;
            groupnum.put(groupbyfield, num);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        for(Field groupbyfield : groupnum.keySet()){
            if(gbfield == Aggregator.NO_GROUPING){
                Type[] typeAr = new Type[1];
                String[] fieldAr = new String[1];
                typeAr[0] = Type.INT_TYPE;
                fieldAr[0] ="";
                td = new TupleDesc(typeAr, fieldAr);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,new IntField(groupnum.get(groupbyfield)));
                tuples.add(tuple);
            }else{
                Type[] typeAr = new Type[2];
                String[] fieldAr = new String[2];
                typeAr[0] = gbfieldtype;
                typeAr[1] = Type.INT_TYPE;
                fieldAr[0] = "";
                fieldAr[1] = "";
                td = new TupleDesc(typeAr, fieldAr);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,groupbyfield);
                tuple.setField(1,new IntField(groupnum.get(groupbyfield)));
                tuples.add(tuple);
            }
        }
        return new TupleArrayIterator(tuples);
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
