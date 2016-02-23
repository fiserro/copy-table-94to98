package org.apache.hadoop.hbase.mapreduce;

import java.sql.*;

import org.apache.phoenix.jdbc.PhoenixDriver;

public class Mrdka {

	public static void main(String[] args) throws SQLException {
		DriverManager.registerDriver(new PhoenixDriver());
		Connection connection = DriverManager.getConnection("jdbc:phoenix:zookeeper1,zookeeper2,zookeeper3");
		DatabaseMetaData md = connection.getMetaData();
		ResultSet table = md.getTables(null, null, "analytics_dashboard", null);
		if (table.next()) {
			System.out.println("TABLE_NAME : " + table.getObject("TABLE_NAME"));
			System.out.println("TABLE_TYPE : " + table.getObject("TABLE_TYPE"));
		}

		System.out.println();

		ResultSet columns = md.getColumns(null, null, "analytics_dashboard", "%");
		while (columns.next()) {
			System.out.print(columns.getObject("COLUMN_NAME") + ";" + columns.getObject("TYPE_NAME"));
			if (!columns.getBoolean("NULLABLE")) {
				System.out.println(";NOT_NULL");
			} else {
				System.out.println();
			}
		}
	}
}
