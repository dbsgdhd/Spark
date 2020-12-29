package com.dingqi.spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class WoedCount {

    public static void main(String[] args) {
        SparkConf wordCount = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(wordCount);
        JavaRDD<String> in = sc.textFile("in");
        JavaRDD<String> sRdd = in.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                ArrayList<String> strings = new ArrayList<String>();
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    strings.add(s2);
                }
                return strings.iterator();

            }
        });
        JavaPairRDD<String,Integer> rdd3 = sRdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        //reduce化简
        JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> collect = rdd4.collect();
        for (Tuple2<String, Integer> stringIntegerTuple2 : collect) {
            System.out.println(stringIntegerTuple2);
        }

    }

}
