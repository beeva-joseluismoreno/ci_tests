package com.bbva.kbdp.flume.sink.hbase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class MainCoprocessor {

	public static void main(String[] args) {
		try {
			String tableName = "kbdp:dfeedback_new";
			String path = "/user/joseluismoreno/rt-kbdp-feedbackIngest-1.0.0-SNAPSHOT.jar";
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			HColumnDescriptor columnFamily1 = new HColumnDescriptor("fdb");
			columnFamily1.setMaxVersions(1);
			hTableDescriptor.addFamily(columnFamily1);
			HColumnDescriptor columnFamily2 = new HColumnDescriptor("sf");
			columnFamily2.setMaxVersions(1);
			hTableDescriptor.addFamily(columnFamily2);
			HColumnDescriptor columnFamily3 = new HColumnDescriptor("ev");
			columnFamily2.setMaxVersions(1);
			hTableDescriptor.addFamily(columnFamily3);
			hTableDescriptor.setValue("COPROCESSOR$1",
					path + "|" + RegionObserverExample.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
			admin.modifyTable(tableName, hTableDescriptor);

			admin.enableTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
