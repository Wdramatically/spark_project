/**
 * FileName: SparkTest
 * Author:   86155
 * Date:     2020/5/26 19:13
 * Description:
 */

package com.ibeifeng.sparkproject.test.spark;

import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.spark.session.SessionAggrStatAccumulator;
import com.ibeifeng.sparkproject.test.accumulator.AccumulatorTest;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("sparktest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        Accumulator<String> accumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaRDD<Tuple2<String, Integer>> map = rdd.map(new Function<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Integer v1) throws Exception {
                return new Tuple2<String, Integer>("number" + v1, v1);
            }
        });

        JavaPairRDD<String, Integer> pairRDD = map.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2);
            }
        });

        filterPairRDDTest(pairRDD,accumulator);


        System.out.println(accumulator.value());


    }

    private static void filterPairRDDTest(JavaPairRDD<String, Integer> pairRDD, Accumulator<String> accumulator) {

        JavaPairRDD<String, Integer> rdd = pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                int value = v1._2;
                if (value % 2 != 0) {
                    accumulator.add(Constants.TIME_PERIOD_1s_3s);
                }
                accumulator.add(Constants.SESSION_COUNT);
                return value % 2 == 0;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        rdd.count();
    }
}
