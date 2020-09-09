package com.xcn.mr2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    //省份、直销拒单率
    //SEQ,酒店,国家,省份,城市,商圈,星级,业务部门,房间数,图片数,评分,评论数,城市平均实住间夜,酒店总订单,酒店总间夜,酒店实住订单,酒店实住间夜,酒店直销订单,酒店直销间夜,酒店直销实住订单,酒店直销实住间夜,酒店直销拒单,酒店直销拒单率,城市直销订单,城市直销拒单率,拒单率是否小于等于直销城市均值
    //anqing_2730,潜山龙山快捷宾馆南岳路店,中国,安徽,安庆,NULL,二星及其他,低星,6,NULL,4.621049881,6,18.00,6,7,5,5,6,7,5,5,1,16.67%,6207,6.11%,0
    private float norate = 0;

    public FlowBean(){

    }

    @Override
    public int compareTo(FlowBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

}
