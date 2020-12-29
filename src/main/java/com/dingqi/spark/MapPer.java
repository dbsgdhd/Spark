package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class MapPer {
    public static void main(String[] args) {
        SparkConf mapPer = new SparkConf().setMaster("local[*]").setAppName("MapPer");
        JavaSparkContext sc = new JavaSparkContext(mapPer);
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10));
        JavaRDD<Object> map = listRDD.map(new Function<Integer, Object>() {
            public Object call(Integer v1) throws Exception {

                return v1 + "s";
            }
        });
        List<Object> collect = map.collect();
        for (Object o : collect) {
            System.out.println(o);
        }


    }

}
