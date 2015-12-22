package com.bbva.kbdp.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
//import org.apache.hadoop.hbase.index.IndexSpecification;
//import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;

public class HBaseIndexes {

	public static void main(String[] args) {

		Configuration conf = HBaseConfiguration.create();
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();

//			IndexedHTableDescriptor htd = new IndexedHTableDescriptor("KBDP:TEST_HINDEX".getBytes());
//
//			IndexSpecification iSpec = new IndexSpecification("MYINDEX_TEST");
//
//			HColumnDescriptor hcd = new HColumnDescriptor("cf1".getBytes());
//
//			iSpec.addIndexColumn(hcd, "qual1", ValueType.String, 10);
//
//			htd.addFamily(hcd);
//
//			htd.addIndex(iSpec);

			//admin.createTable(htd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("ok");
	}

}
