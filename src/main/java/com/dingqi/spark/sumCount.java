package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class sumCount {
    public static void main(String[] args) {
        SparkConf cumcount = new SparkConf().setMaster("local[*]").setAppName("cumcount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(cumcount);
        JavaRDD<String> in = javaSparkContext.textFile("in");
        JavaRDD<String> newKey = in.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String[] s = v1.split(" ");
                String s1 = s[1] + "_" + s[4];
                return s1;
            }
        });
        JavaPairRDD<String, Integer> sr1 = newKey.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s, 1);

            }

        });
        JavaPairRDD<String, Integer> sr2 = sr1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        JavaPairRDD<String, Map<String, Integer>> sr3 = sr2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Map<String, Integer>>() {
            @Override
            public Tuple2<String, Map<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                Map<String, Integer> map = new HashMap<>();
                String key = stringIntegerTuple2._1.split("_")[0];
                map.put(stringIntegerTuple2._1.split("_")[1], stringIntegerTuple2._2);
                return new Tuple2<>(key, map);

            }
        });

        JavaPairRDD<String, Map<String, Integer>> sr4 = sr3.reduceByKey(new Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> call(Map<String, Integer> v1, Map<String, Integer> v2) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.putAll(v1);
                map.putAll(v2);
                return map;
            }
        });
//
        JavaPairRDD<String, Map<String, Integer>> sr5 = sr4.mapValues(new Function<Map<String, Integer>, Map<String, Integer>>() {

            @Override
            public Map<String, Integer> call(Map<String, Integer> v1) throws Exception {
                List<Map.Entry<String, Integer>> list = new ArrayList<>();
                list.addAll(v1.entrySet());
                Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                    @Override
                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                        return o2.getValue()-o1.getValue();
                    }
                });
                Map<String, Integer> hm = new HashMap<>();
                for (int i = 0; i <= 2; i++) {

                    hm.put(list.get(i).getKey(), list.get(i).getValue());
                }
                return hm;
            }

        });
        sr5.foreach(new VoidFunction<Tuple2<String, Map<String, Integer>>>() {
            @Override
            public void call(Tuple2<String, Map<String, Integer>> stringMapTuple2) throws Exception {
                System.out.println(stringMapTuple2.toString());
            }
        });


    }
}
