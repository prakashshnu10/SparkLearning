package com.prakash.chapter4_createrddusingparallelizing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRDDUsingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("CreateRDDUsingParallelizeTest")
    .setMaster("local[*]");


    @Test
    @DisplayName("Create an empty RDD with no partition in Spark")
    void createAnEmptyRDDWithNoPartitionsInSpark(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var emptyRDD = sparkContext.emptyRDD();
            System.out.println(emptyRDD);
            System.out.printf("Number of partition: %d%n",emptyRDD.getNumPartitions());
        }
    }


    @Test
    @DisplayName("Create an empty RDD with default partition in Spark")
    void createAnEmptyRDDWithDefaultPartitionsInSpark(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var emptyRDD = sparkContext.parallelize(List.of());
            System.out.println(emptyRDD);
            System.out.printf("Number of partition: %d%n",emptyRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java Collection using parallelize method")
    void createSparkRDDUsingParallelizeMethod(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var data = Stream.iterate(1, n -> n + 1)
                    .limit(8L)
                    .collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data);
            System.out.println(myRdd);
            System.out.printf("Number of partition: %d%n",myRdd.getNumPartitions());
            System.out.printf("Total elements in RDD: %d%n",myRdd.count());
            System.out.println("Elements in Rdd: ");
            myRdd.collect().forEach(System.out::println);

            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);

            System.out.printf("Max value - %d, Min value - %d, sum - %d", max, min, sum);


        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java Collection using parallelize method with given partition")
    void createSparkRDDUsingParallelizeMethodWithGivenPartition(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var data = Stream.iterate(1, n -> n + 1)
                    .limit(8L)
                    .collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data,8);
            System.out.println(myRdd);
            System.out.printf("Number of partition: %d%n",myRdd.getNumPartitions());
            System.out.printf("Total elements in RDD: %d%n",myRdd.count());
            System.out.println("Elements in Rdd: ");
            myRdd.collect().forEach(System.out::println);

            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);

            System.out.printf("Max value - %d, Min value - %d, sum - %d", max, min, sum);


        }
    }
}
