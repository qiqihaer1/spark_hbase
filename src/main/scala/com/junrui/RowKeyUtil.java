package com.junrui;

import java.text.DecimalFormat;
import java.util.Random;

/**
 * rowkey的hash设计，获得前缀
 */
public class RowKeyUtil {
    private static String REGION_NUMBER_2="00";
    //message为唯一值
    //maxSize一般与region数量相等,一般为不大于100，对rowkey进行hash设计,返回2位数String前缀
    public static String getRegNo2(String message,int maxSize){
        //hashCode()返回一个Int
        //测试发现出现了负数，取绝对值Math.abs()
        int regNo=Math.abs(message.hashCode())%maxSize;
        //0不是等长，处理成行长
        //使用applyPattern()方法对数字进行格式化
        DecimalFormat df=new DecimalFormat();
        df.applyPattern(REGION_NUMBER_2);
        //格式化处理,都是2位数？
        return df.format(regNo);
    }


    public static void main(String[] args) {
        Random random = new Random();
        for (int i=0;i<10;i++){
            int ii = random.nextInt(10000);
            String regNo2 = getRegNo2("name"+ii, 2);
            System.out.println(regNo2);
        }
    }
}
