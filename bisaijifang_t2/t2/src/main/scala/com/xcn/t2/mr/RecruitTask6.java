package com.xcn.t2.mr;

import org.apache.hadoop.hive.ql.exec.UDF;

public class RecruitTask6 extends UDF {

    public String evaluate(String value){
        String[] fields = value.split("-");
        int max = Integer.parseInt(fields[0]);
        int min = Integer.parseInt(fields[1]);
        return String.valueOf((max + min) / 2);
    }
}
