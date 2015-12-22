package com.bbva.kbdp.flume.sink.hbase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;

public class RegionObserverExample extends BaseRegionObserver {

	Table table = null;
	
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		//super.postPut(e, put, edit, durability);
		
		
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("kbdp:dfeedback_new"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		
		byte[] row = put.getRow();
		System.out.println("COPROCESSOR GETROW : " + new String(row));
		
		String[] input = new String[41];
		input[0] = "01045764D94224283J2014305";
		input[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
		input[7] = "08/09/2015 0:58:34";
		//KeyByDate kbd = me.getSendKey(input, table);
		//String rowKey1 = new String(kbd.getRowKey());
//		if (rowk)
		int random = 1 + (int) (Math.random() * ((100 - 1) + 1));
		byte[] rowKey = (random + "_" + input[0] + input[7].split(" ")[0] + input[3]).getBytes(Charsets.UTF_8);
		//byte[] rowKey = (""+random).getBytes(Charsets.UTF_8);
		Put put1 = new Put(rowKey);
		put1.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
		put1.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("SEND"), Bytes.toBytes("1"));
		table.put(put1);

	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		// TODO Auto-generated method stub
		super.start(e);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
