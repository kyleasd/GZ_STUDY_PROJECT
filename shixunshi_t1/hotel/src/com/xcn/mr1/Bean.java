package com.xcn.mr1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    //private String provence = "";
    //private String city = "";
    private long hotel_num = 0;
    private long room_num = 0;


    //省份、城市、酒店数量、房间数量。
    //SEQ,酒店,国家,省份,城市,商圈,星级,业务部门,房间数,图片数,评分,评论数,城市平均实住间夜,酒店总订单,酒店总间夜,酒店实住订单,酒店实住间夜,酒店直销订单,酒店直销间夜,酒店直销实住订单,酒店直销实住间夜,酒店直销拒单,酒店直销拒单率,城市直销订单,城市直销拒单率,拒单率是否小于等于直销城市均值
    //aba_2066,马尔康嘉绒大酒店,中国,四川,阿坝,NULL,四星级/高档,OTA,85,NULL,4.143799782,108,34.06,45,75,22,44,NULL,NULL,NULL,NULL,NULL,NULL,34147,7.90%,0

    public Bean(){

    }


    public long getHotel_num() {
        return hotel_num;
    }

    public void setHotel_num(long hotel_num) {
        this.hotel_num = hotel_num;
    }

    public long getRoom_num() {
        return room_num;
    }

    public void setRoom_num(long room_num) {
        this.room_num = room_num;
    }

    @Override
    public int compareTo(Bean o) {
        return this.room_num > o.room_num ? -1 : 1;
    }

    @Override
    public String toString() {
        return hotel_num + "\t" + room_num;
    }

    @Override
    public void write(DataOutput Out) throws IOException {
        //Out.writeChars(provence);
        //Out.writeChars(city);
        Out.writeLong(hotel_num);
        Out.writeLong(room_num);
    }

    @Override
    public void readFields(DataInput Input) throws IOException {
        //this.provence = Input.readLine();
        //this.city = Input.readLine();
        this.hotel_num = Input.readLong();
        this.room_num = Input.readLong();
    }
    public void set( long hotel_num,long room_num ){
        //this.provence = provence;
        //this.city = city;
        this.hotel_num = hotel_num;
        this.room_num = room_num;
    }
}
