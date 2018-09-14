package com.example.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinKeyGroupComparator extends WritableComparator {

    public JoinKeyGroupComparator(){
        super(JoinKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinKey first = (JoinKey)a;
        JoinKey second = (JoinKey) b;
        long firstAcctId = first.getAcctId();
        long sendAcctId = second.getAcctId();
        if(firstAcctId==sendAcctId){
            return 0;
        }else{
            return firstAcctId > sendAcctId ? 1 : -1;
        }

    }
}
