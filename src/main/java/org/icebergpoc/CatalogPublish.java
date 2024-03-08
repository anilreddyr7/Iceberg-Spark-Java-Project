package org.icebergpoc;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.PartitionSpec;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Initialize catalog, namespace and iceberg table into Postgres database - One time run for use case
 */
public class CatalogPublish {
    public static void main(String[] args) {

        JdbcCatalog catalog = getJdbcCatalog();

        Namespace webapp = Namespace.of("webapp");
        Schema schema = new Schema(
                Types.NestedField.required(1, "emp_id", Types.StringType.get()),
                Types.NestedField.required(2, "emp_login", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "emp_name", Types.StringType.get()),
                Types.NestedField.required(4, "emp_dept", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("emp_login")
                .identity("emp_id")
                .build();
        TableIdentifier name = TableIdentifier.of(webapp, "employee");

        catalog.createTable(name, schema, spec);
    }

    @NotNull
    private static JdbcCatalog getJdbcCatalog() {
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
        return catalog;
    }
}
