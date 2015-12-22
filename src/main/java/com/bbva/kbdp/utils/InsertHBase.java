package com.bbva.kbdp.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertHBase {

	private String tableName;
	
	public static void main(String[] args) {
		InsertHBase me = new InsertHBase();

		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;

		me.tableName = "TEST_MICRO3";

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(me.tableName)); 

			String columnNamesFDB = "CAMP3,TOKEN3,FLANZ3,SEND3,FSEND3,OPEN3,FOPEN3,CLCK3,FCLCK3,SEGM3,LSUCC3,FCARGA3,PROD3";
			String[] arrColumnNames = columnNamesFDB.split(",");
			String columnNamesSF = "V13,V23,PERS3,EJE13,EJE23";
			String columnNamesAPX = "CONTRATA3,FEC_CONTRATA3";
			String[] cols1 = columnNamesFDB.split(",");
			String[] cols2 = columnNamesSF.split(",");
			String[] cols3 = columnNamesAPX.split(",");
			String values = "76389EFD-C1B0-4846-96A2-9FAC72A870F7,01318819J90949511A2014305,08/09/2015 0:58:34,1,08/09/2015 0:58:34,0,,0,,segmento1,45,20/01/2015,1 - 464 - Consumo";
			Put put = null;
			String[] arrValues = values.split(",");
			put = new Put("20_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);
			put = new Put("21_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);
			put = new Put("22_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);
			put = new Put("23_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);
			put = new Put("24_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);
			put = new Put("12_01318819J90949511A201430508/09/201576389EFD-C1B0-4846-96A2-9FAC72A870F7".getBytes());
			for (int i = 0; i < arrValues.length; i++) {
				put.addColumn("fdb".getBytes(), arrColumnNames[i].getBytes(), arrValues[i].getBytes());
			}
			table.put(put);

			
			
			for (int i = 0; i < 200000; i++){
				int random = 1 + (int) (Math.random() * ((100 - 1) + 1));
				String rowKey = ""+random + "_" + i;
				put = new Put(rowKey.getBytes());
				for (int j = 0; j < cols1.length - 1; j++) {
					put.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes(cols1[j]), ("VALOR de " + cols1[j] + ""+j).getBytes());
				}
				table.put(put);
				for (int j = 0; j < cols2.length - 1; j++) {
					put.addColumn(Bytes.toBytes("sf"), Bytes.toBytes(cols2[j]), ("VALOR de " + cols2[j] + ""+j).getBytes());
				}
				table.put(put);
				for (int j = 0; j < cols3.length - 1; j++) {
					put.addColumn(Bytes.toBytes("ev"), Bytes.toBytes(cols3[j]), ("VALOR de " + cols3[j] + ""+j).getBytes());
				}
				table.put(put);
				
			}
			
			System.out.println("PROCESO TERMINADO!!!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
