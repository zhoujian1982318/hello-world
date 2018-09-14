package com.example.hadoop.mr;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class JoinKey implements WritableComparable<JoinKey> {
    //acct id 相同， 按 type排序。acct 表必须排在 ap前面
    public static final int TYPE_ACCT = 0;
    public static final int TYPE_AP = 1;

    private long acctId;

    private int type;

    private Text acctName = new Text("");

    public JoinKey(){

    }
    public JoinKey(long acctId, int type, Text acctName){
        this.acctId = acctId;
        this.type = type;
        this.acctName = acctName;
    }

    public JoinKey(long acctId, int type){
        this.acctId = acctId;
        this.type = type;

    }

    @Override
    public int compareTo(JoinKey second) {
        JoinKey first = this;
        if (first.acctId == second.acctId) {
            return first.type - second.type ;
        } else {
            return first.acctId > second.acctId ? 1 : -1;
        }
    }

    public long getAcctId() {
        return acctId;
    }

    public void setAcctId(long acctId) {
        this.acctId = acctId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Text getAcctName() {
        return acctName;
    }

    public void setAcctName(Text acctName) {
        this.acctName = acctName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinKey joinKey = (JoinKey) o;
        return acctId == joinKey.acctId && type == joinKey.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acctId, type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(acctId);
        out.writeInt(type);
        acctName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.acctId = in.readLong();
        this.type = in.readInt();
        this.acctName.readFields(in);

    }
}
