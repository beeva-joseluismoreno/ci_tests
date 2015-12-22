package com.bbva.kbdp.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class MainForTests {

	private Table table1;

	public Connection getConnection() {
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table1 = connection.getTable(TableName.valueOf("kbdp:detalle_feedback"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return connection;
	}

	public static void main(String[] args) {
		MainForTests me = new MainForTests();
		me.getConnection();
		ReflectionMethod method = new ReflectionMethod();
		
		method.getCallerClassName(0);
		System.out.println("sdfdsf");
	}
}
