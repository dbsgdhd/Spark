package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TestRdd {
    public static void main(String[] args) {
        SparkConf rdd = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(rdd);
//        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
//        JavaRDD<Integer> parallelize = sc.parallelize(list,2);
        JavaRDD<String> parallelize = sc.textFile("in",2);
        parallelize.saveAsTextFile("output");
    }


}
