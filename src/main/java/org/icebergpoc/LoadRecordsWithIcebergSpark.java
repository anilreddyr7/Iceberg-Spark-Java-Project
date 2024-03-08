package org.icebergpoc;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.data.Record;
//import org.apache.iceberg.data.IcebergGenerics;

import java.util.HashMap;
import java.util.Map;

public class LoadRecordsWithIcebergSpark {
    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, "jdbc:postgresql://localhost:5432/postgres");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "postgres");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "admin");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "D:\\Iceberg\\Warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());

        JdbcCatalog catalog = new JdbcCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        //Once demo catalog is initialized, Configuring namespace - webapp, table - logs with below code
        Namespace webapp = Namespace.of("webapp");
        TableIdentifier name = TableIdentifier.of(webapp, "logs");
        Table table = catalog.loadTable(name);

        //Reading table data
       CloseableIterable<Record> result = IcebergGenerics.read(table).build();

        for (Record r: result) {
            System.out.println(r);
        }

    }
}
