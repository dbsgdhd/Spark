package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPer5 {
    public static void main(String[] args) {

        SparkConf mapPer2 = new SparkConf().setMaster("local[*]").setAppName("MapPer2");
        JavaSparkContext sc = new JavaSparkContext(mapPer2);
        JavaRDD<String> in = sc.textFile("in");
        JavaRDD<List<String>> mapPartitions= in.glom();
        mapPartitions.foreach(new VoidFunction<List<String>>() {
            public void call(List<String> strings) throws Exception {
                System.out.println(strings.toString());
            }
        });
    }
}
