package com.prakash.chapter5_externalDatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testng.annotations.Test;

import java.nio.file.Path;

public class RDDExternalDatasets {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDExternalDatasets")
            .setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\test100words.txt"
    })
    @DisplayName("Test loading local text files into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDD(final String localFilePath){
        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.textFile(localFilePath);
            System.out.printf("Total lines in the file: %d%n", myRdd.count());
            System.out.println("Printing first 10 lines ->");

            myRdd.take(10).forEach(System.out::println);
            System.out.println("-----------------\n");
        }
    }

    @Test
    @DisplayName("Test loading whole directory into Spark RDD")
    void testLoadingWholeDirectoryFilesIntoSparkRDD(){
        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var testDirPath = Path.of("src","test","resources").toString();
            final var myRdd = sparkContext.wholeTextFiles(testDirPath);
            System.out.printf("Total number of file in the directory [%s] = %d%n", testDirPath,myRdd.count());

            myRdd.collect().forEach(tuple -> {
                System.out.printf("File name: %s%n",tuple._1);
                System.out.println("------------------");
                if(tuple._1.endsWith("properties")){
                    System.out.printf("Contents of [%s]:%n",tuple._1);
                    System.out.println(tuple._2);
                }
            });
//            System.out.println("Printing first 10 lines ->");
//
//            myRdd.take(10).forEach(System.out::println);
//            System.out.println("-----------------\n");
        }
    }


}
