package com.shuochengwang;


import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App {

    private static final String TABLE_NAME = "Test";
    private static final String CF_Name = "cf1";
    private static final String QUAL_NAME = "cq1";

    public static void main(String... args) throws IOException
    {
        Configuration config = HBaseConfiguration.create();

        try (Connection conn = ConnectionFactory.createConnection(config); Admin admin = conn.getAdmin())
        {

            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));

            if (!admin.tableExists(table.getName()))
            {
                System.out.print("Creating table. ");
                HTableDescriptor newTable = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                newTable.addFamily(new HColumnDescriptor(CF_Name));
                admin.createTable(newTable);
                System.out.println(" Done.");
            }

            Random random = new Random();
            for (int i = 0; i < 1000; i++)
            {
                Put put = new Put(Bytes.toBytes(i));
                put.addColumn(Bytes.toBytes(CF_Name), Bytes.toBytes(QUAL_NAME), Bytes.toBytes(random.nextInt(100)));
                table.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(CF_Name), Bytes.toBytes(QUAL_NAME));

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner)
            {
                System.out.println("Found row: " + result +
                        ", Value: " + Bytes.toInt(result.getValue(Bytes.toBytes(CF_Name), Bytes.toBytes(QUAL_NAME))));
            }
        }
    }
}
