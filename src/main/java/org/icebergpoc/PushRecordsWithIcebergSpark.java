package org.icebergpoc;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Role: To push data to file system.
 * Iceberg is invoked from Spark session. Spark is initiated as standalone process from below code.
 * Prerequisite - CatalogPublish should be run at least once with given schema.
 */
public class PushRecordsWithIcebergSpark {
    public static void main(String[] args) throws IOException {
        //Our catalog name is "demo" in CatalogPublish class, hence catalog.demo is mentioned below.
        //warehouse path should exist in the local machine
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java API Demo")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://localhost:5432/postgres")
                .config("spark.sql.catalog.demo.jdbc.user", "postgres")
                .config("spark.sql.catalog.demo.jdbc.password", "admin")
                .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                .config("spark.sql.catalog.demo.warehouse", "D:\\Iceberg\\Warehouse")
                .config("spark.sql.defaultCatalog", "demo")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "D:\\Iceberg\\spark-events")
                .config("spark.history.fs.logDirectory", "D:\\Iceberg\\spark-events")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        //Running below queries to insert records
        String query = "INSERT INTO demo.webapp.logs "
                + "VALUES "
                + "('info', timestamp 'today', 'Just letting you know!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
                + "('warning', timestamp 'today', 'You probably should not do this!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
                + "('error', timestamp 'today', 'This was a fatal application error!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3'))";

        spark.sql(query).show();

    }
}