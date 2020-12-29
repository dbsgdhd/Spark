package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPer3 {
    public static void main(String[] args) {

        SparkConf mapPer2 = new SparkConf().setMaster("local[*]").setAppName("MapPer2");
        JavaSparkContext sc = new JavaSparkContext(mapPer2);
        JavaRDD<String> in = sc.textFile("in");
        JavaRDD<String> mapPartitions = in.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                List<String> s = new ArrayList<String>();
                while (v2.hasNext()) {
                    String next = v2.next();
                    s.add(v1 + "___" + next);
                }
                Iterator<String> iterator = s.iterator();
                return iterator;

            }
        }, false);
        List<String> collect = mapPartitions.collect();
        for (String s : collect) {
            System.out.println(s);
        }


    }
}
