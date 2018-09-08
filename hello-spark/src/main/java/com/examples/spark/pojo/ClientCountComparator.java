package com.examples.spark.pojo;

import java.io.Serializable;
import java.util.Comparator;

public class ClientCountComparator implements Serializable,  Comparator<ClientCntCount> {

    @Override
    public int compare(ClientCntCount o1, ClientCntCount o2) {
        return Integer.compare(o1.getCount(), o2.getCount());
    }
}
