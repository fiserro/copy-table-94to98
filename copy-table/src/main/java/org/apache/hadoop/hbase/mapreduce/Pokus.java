package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache98.hadoop.conf.Configuration;
import org.apache98.hadoop.hbase.client.HConnectionManager;
import org.apache98.hadoop.hbase.client.HTableInterface;
import org.apache98.hadoop.hbase.client.Put;
import org.apache98.hadoop.hbase.util.Bytes;

public class Pokus {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		org.apache98.hadoop.conf.Configuration conf98 = new Configuration();
		conf98.set("hbase.zookeeper.quorum", "c-sencha-s01,c-sencha-s02,c-sencha-s03");
		HTableInterface table = HConnectionManager.createConnection(conf98).getTable("twitter_tweets");
		// HTable table = new HTable(conf98, "twitter_tweets");
		Put put = new Put(Bytes.toBytes(0));
		put.add(Bytes.toBytes("d"), Bytes.toBytes("q"), Bytes.toBytes("v"));
		table.put(put);
		table.close();
	}

}
