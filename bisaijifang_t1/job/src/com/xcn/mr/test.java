package com.xcn.mr;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;


import java.io.*;

public class test {
//    public static String readJsonFile(String fileName) {
//        String jsonStr;
//        JSONObject jobj;
//        try {
//            File jsonFile = new File(fileName);
//            FileReader fileReader = new FileReader(jsonFile);
//            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
//            BufferedReader br = new BufferedReader(reader);
//            int ch = 0;
//            String tmp;
//            StringBuffer sb = new StringBuffer();
//            while ( (tmp = br.readLine()) != null){
//            //for( int i=0; i < 300; i++ ){
//                ch++;
//                if( tmp.contains("}},") ){
//                    sb.append(tmp.replace("}},","}}"));
//                } else {
//                    if( !tmp.substring(tmp.length()-2,tmp.length()).equals("}}") ){
//                        sb.append(tmp);
//                        System.out.println(tmp+"\n"+ch);
//                    }else{
//                        sb.append(tmp);
//                    }
//                }
//            }
//            fileReader.close();
//            reader.close();
//
//            return tmp;
//
//
//        }catch (IOException e){
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    public static void main(String[] args) {
//        String path = "C:\\Users\\Wen\\Desktop\\IDEAProject\\bigdata_git\\code\\bisaijifang_t1\\company.json";
//        String s = readJsonFile(path);
//
//        JSONObject jo = JSON.parseObject(s);
//        System.out.println();
//
//    }
}
