package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

public class MapPer6 {
    public static void main(String[] args) {

        SparkConf mapPer2 = new SparkConf().setMaster("local[*]").setAppName("MapPer2");
        JavaSparkContext sc = new JavaSparkContext(mapPer2);
        JavaRDD<String> in = sc.textFile("in");

        JavaPairRDD<Integer, Iterable<String>> mapPartitions = in.groupBy(new Function<String, Integer>() {
            public Integer call(String v1) throws Exception {
                return Integer.valueOf(v1) % 2;

            }
        });
        mapPartitions.foreach(new VoidFunction<Tuple2<Integer, Iterable<String>>>() {
            public void call(Tuple2<Integer, Iterable<String>> integerIterableTuple2) throws Exception {
                System.out.println(integerIterableTuple2.toString());
            }
        });
    }
}
