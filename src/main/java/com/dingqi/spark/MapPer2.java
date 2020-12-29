package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPer2 {
    public static void main(String[] args) {

        SparkConf mapPer2 = new SparkConf().setMaster("local[*]").setAppName("MapPer2");
        JavaSparkContext sc = new JavaSparkContext(mapPer2);
        JavaRDD<String> in = sc.textFile("in");
        JavaRDD<String> mapPartitions = in.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                List<String> s = new ArrayList<String>();
                while (stringIterator.hasNext()){
                    String next = stringIterator.next();
                    s.add(next+"_ss");
                }
                Iterator<String> iterator = s.iterator();
                return  iterator;
            }
        });
        List<String> collect = mapPartitions.collect();
        for (String s : collect) {
            System.out.println(s);
        }


    }
}
