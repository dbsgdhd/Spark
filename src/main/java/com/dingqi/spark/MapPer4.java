package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPer4 {
    public static void main(String[] args) {

        SparkConf mapPer2 = new SparkConf().setMaster("local[*]").setAppName("MapPer2");
        JavaSparkContext sc = new JavaSparkContext(mapPer2);
        JavaRDD<String> in = sc.textFile("in");
        JavaRDD<String> mapPartitions = in.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();

            }
        });
        mapPartitions.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
