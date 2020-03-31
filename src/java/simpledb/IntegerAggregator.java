package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private HashMap<Field, Integer> group;

    private HashMap<Field, Integer> groupNum;

    private HashMap<Field, Integer> groupAvg;

    private Op what;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        group = new HashMap<>();
        groupNum = new HashMap<>();
        groupAvg = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField ;
        int aggregateFieldValue = 0;
        int nownum = 0;
        if(gbfield == Aggregator.NO_GROUPING){
            groupByField = new IntField(Aggregator.NO_GROUPING);
            aggregateFieldValue =((IntField)tup.getField(afield)).getValue();
        }else{
            groupByField = tup.getField(gbfield);
            aggregateFieldValue =((IntField)tup.getField(afield)).getValue();
        }
        int nowvalue = 0;
        if(groupNum.containsKey(groupByField)){
            nownum = groupNum.get(groupByField);
        }
        if(group.containsKey(groupByField)){
            nowvalue = group.get(groupByField);
        }else{
            dealWithGroupByField(nowvalue, nownum, what, groupByField);
        }
        nowvalue = group.get(groupByField);
        nownum = groupNum.get(groupByField);
        if(what == Op.SUM){
            nowvalue += aggregateFieldValue;
            group.put(groupByField, nowvalue);
        }
        if(what == Op.COUNT){
            nowvalue++;
            group.put(groupByField,nowvalue);
        }
        if(what == Op.AVG){
            nowvalue += aggregateFieldValue;
            nownum += 1;
            groupNum.put(groupByField, nownum);
            group.put(groupByField, nowvalue);
            groupAvg.put(groupByField, nowvalue/nownum);
        }
        if(what == Op.MIN){
            if(aggregateFieldValue < nowvalue){
                nowvalue = aggregateFieldValue;
                group.put(groupByField, nowvalue);
            }
        }
        if(what == Op.MAX){
            if(aggregateFieldValue > nowvalue){
                nowvalue = aggregateFieldValue;
                group.put(groupByField, nowvalue);
            }
        }


    }

    public void dealWithGroupByField(int nowvalue, int nownum, Op what, Field groupByField){
        if(what == Op.SUM){
            groupNum.put(groupByField, nownum);
            group.put(groupByField, nowvalue);
        }
        if(what == Op.COUNT){
            groupNum.put(groupByField, nownum);
            group.put(groupByField, nowvalue);
        }
        if(what == Op.AVG){
            groupNum.put(groupByField, nownum);
            group.put(groupByField, nowvalue);
        }
        if(what == Op.MIN){
            groupNum.put(groupByField, nownum);
            group.put(groupByField, 2147483647);
        }
        if(what == Op.MAX){
            groupNum.put(groupByField, 0);
            group.put(groupByField, -2147483648);
        }


    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for(Field groupField : group.keySet()){
            if(gbfield == Aggregator.NO_GROUPING){
                Type[] typeAr = new Type[1];
                String[] fieldAr = new String[1];
                typeAr[0] = Type.INT_TYPE;
                fieldAr[0] = "";
                TupleDesc td = new TupleDesc(typeAr,fieldAr);
                Tuple t = new Tuple(td);
                if(what == Op.AVG){
                    t.setField(0, new IntField(groupAvg.get(groupField)));
                    tuples.add(t);
                }else{
                    t.setField(0,new IntField(group.get(groupField)));
                    tuples.add(t);
                }
            }else{
                Type[] typeAr = new Type[2];
                String[] fieldAr = new String[2];
                typeAr[0] = groupField.getType();
                typeAr[1] = Type.INT_TYPE;
                fieldAr[0] = "";
                fieldAr[1] = "";
                TupleDesc td = new TupleDesc(typeAr, fieldAr);
                Tuple t = new Tuple(td);
                if(what == Op.AVG){
                    t.setField(0,groupField);
                    t.setField(1, new IntField(groupAvg.get(groupField)));
                    tuples.add(t);
                }else{
                    t.setField(0,groupField);
                    t.setField(1, new IntField(group.get(groupField)));
                    tuples.add(t);
                }
            }
        }
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        return new TupleArrayIterator(tuples);
    }

}
