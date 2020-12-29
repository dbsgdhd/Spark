package com.dingqi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by hadoop on 17-10-23.
 */
public class JavaSparkCombine {
    public static void main(String[]args){

        SparkConf conf=new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> users=new ArrayList<Tuple2<String,Integer>>();
        Tuple2<String,Integer> user1=new Tuple2<String,Integer>("1212",1);
        Tuple2<String,Integer> user2=new Tuple2<String,Integer>("1213",3);
        Tuple2<String,Integer> user3=new Tuple2<String,Integer>("1214",6);
        Tuple2<String,Integer> user4=new Tuple2<String,Integer>("1215",3);
        Tuple2<String,Integer> user5=new Tuple2<String,Integer>("1212",1);
        users.add(user1);
        users.add(user2);
        users.add(user3);
        users.add(user4);
        users.add(user5);
        JavaPairRDD<String,Integer> userrdd=sc.parallelizePairs(users,2);

        JavaRDD<Tuple2<String,Integer>> userr=sc.parallelize(users);

        JavaPairRDD<String,Tuple2<Integer,Integer>>combrdd=userrdd.combineByKey(
                    new Function<Integer,Tuple2<Integer,Integer>>(){
                    public Tuple2<Integer,Integer> call(Integer x){
                        return new Tuple2<Integer,Integer> (x,1);
                    }
                },  new Function2<Tuple2<Integer,Integer>,Integer,Tuple2<Integer,Integer>>(){
                    public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> v,Integer x){
                        return new Tuple2<Integer,Integer>(v._1()+x,v._2()+1);
                    }
                },
                    new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){
                    public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> v,Tuple2<Integer,Integer> v1){
                        return new Tuple2<Integer,Integer>(v._1()+v1._1(),v._2()+v1._2());
                    }
                });

               //groupbykey

        JavaPairRDD<String,ArrayList<Integer>>combrddgroupby=userrdd.combineByKey(
                new Function<Integer, ArrayList<Integer>>(){
                    public ArrayList call(Integer x){
                        List<Integer> result=new ArrayList();
                        result.add(x);
                        return (ArrayList) result;
                    }
                },  new Function2<ArrayList<Integer>,Integer,ArrayList<Integer>>(){
                    public ArrayList<Integer> call(ArrayList<Integer> v,Integer x){
                        v.add(x);
                        return v;
                    }
                },
                new Function2<ArrayList<Integer>,ArrayList<Integer>,ArrayList<Integer>>(){
                    public ArrayList<Integer> call(ArrayList<Integer> v1,ArrayList<Integer> v2){
//                        for(Integer value:v1){
//                           v2.add(value) ;
//                        }
                        v1.forEach(v2::add);//方法二
                        return v2;
                    }
                });


        //combrddgroupby.collect().forEach(System.out::println);


        //reducebykey

        JavaPairRDD<String,Tuple2<Integer,Integer>> reducerd=userr.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Integer> s) throws Exception {
                        return new Tuple2<String, Tuple2<Integer, Integer>>(s._1(),new Tuple2<Integer,Integer>(s._2(),1));
                    }
                }
        );

        JavaPairRDD<String,Tuple2<Integer,Integer>> reducBykey=reducerd.reduceByKey(
                new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> integerIntegerTuple22) throws Exception {
                        return new Tuple2<Integer,Integer>(integerIntegerTuple22._1()+integerIntegerTuple2._1(),integerIntegerTuple22._2()+integerIntegerTuple2._2());
                    }
                }
        );
        reducBykey.collect().forEach(System.out::println);







    }

}