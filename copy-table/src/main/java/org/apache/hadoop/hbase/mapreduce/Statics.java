package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.fs.Path;

public class Statics {

	public static final String NEW_TABLE_NAME = "new.table.name";
	public static final String SALT_BYTES = "salt.bytes";
	public static final String BUCKET_SIZE = "bucket.size";
	public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_ZOOKEEPER_QUORUM2 = "hbase.zookeeper.quorum2";

	public static final String NAME = "copytable";
	public static long startTime = 0;
	public static long endTime = 0;
	public static int versions = -1;
	public static String tableName = "instagram_interactions";
	public static String startRow = null;
	public static String stopRow = null;
	// public static String newTableName = "fb_comments_old";
	public static String peerAddress = null;
	public static String zkQuorum = "c-sencha-s01,c-sencha-s02,c-sencha-s03";
	public static String families = null;
	public static boolean allCells = false;
	public static int regionSplit = -1;
	public static int saltBytes = -1;
	public static int bucketSize = 30000;

	public static final Path OUPUT_PATH = new Path("hdfs:///migrations/fb_comments/");

	public static boolean doCommandLine(final String[] args) {
		// Process command-line args. TODO: Better cmd-line processing
		// (but hopefully something not as painful as cli options).
		if (args.length < 1) {
			printUsage(null);
			return false;
		}
		try {
			for (int i = 0; i < args.length; i++) {
				String cmd = args[i];
				if (cmd.equals("-h") || cmd.startsWith("--h")) {
					printUsage(null);
					return false;
				}

				final String startRowArgKey = "--startrow=";
				if (cmd.startsWith(startRowArgKey)) {
					startRow = cmd.substring(startRowArgKey.length());
					continue;
				}

				final String stopRowArgKey = "--stoprow=";
				if (cmd.startsWith(stopRowArgKey)) {
					stopRow = cmd.substring(stopRowArgKey.length());
					continue;
				}

				final String startTimeArgKey = "--starttime=";
				if (cmd.startsWith(startTimeArgKey)) {
					startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
					continue;
				}

				final String endTimeArgKey = "--endtime=";
				if (cmd.startsWith(endTimeArgKey)) {
					endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
					continue;
				}

				final String versionsArgKey = "--versions=";
				if (cmd.startsWith(versionsArgKey)) {
					versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
					continue;
				}

				// final String newNameArgKey = "--new.name=";
				// if (cmd.startsWith(newNameArgKey)) {
				// newTableName = cmd.substring(newNameArgKey.length());
				// continue;
				// }

				final String peerAdrArgKey = "--peer.adr=";
				if (cmd.startsWith(peerAdrArgKey)) {
					peerAddress = cmd.substring(peerAdrArgKey.length());
					continue;
				}

				final String zkQuorumArgKey = "--zk.quorum=";
				if (cmd.startsWith(zkQuorumArgKey)) {
					zkQuorum = cmd.substring(zkQuorumArgKey.length());
					continue;
				}

				final String familiesArgKey = "--families=";
				if (cmd.startsWith(familiesArgKey)) {
					families = cmd.substring(familiesArgKey.length());
					continue;
				}

				final String regionSplitArgKey = "--region.split=";
				if (cmd.startsWith(regionSplitArgKey)) {
					regionSplit = Integer.parseInt(cmd.substring(regionSplitArgKey.length()));
					continue;
				}

				final String bucketSizeArgKey = "--bucket.size=";
				if (cmd.startsWith(bucketSizeArgKey)) {
					bucketSize = Integer.parseInt(cmd.substring(bucketSizeArgKey.length()));
					continue;
				}

				final String saltBytesArgKey = "--salt.bytes=";
				if (cmd.startsWith(saltBytesArgKey)) {
					saltBytes = Integer.parseInt(cmd.substring(saltBytesArgKey.length()));
					if (saltBytes < 1 || saltBytes > 4) {
						throw new IllegalArgumentException("salt bytes must be betweet 1 and 4");
					}
					continue;
				}

				if (cmd.startsWith("--all.cells")) {
					allCells = true;
					continue;
				}

				if (i == args.length - 1) {
					tableName = cmd;
					// } else {
					// printUsage("Invalid argument '" + cmd + "'");
					// return false;
				}
			}
			// if (newTableName == null && peerAddress == null) {
			// printUsage("At least a new table name or a " +
			// "peer address must be specified");
			// return false;
			// }
			if (startTime > endTime) {
				printUsage("Invalid time range filter: starttime=" + startTime + " >  endtime=" + endTime);
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			printUsage("Can't start because " + e.getMessage());
			return false;
		}
		return true;
	}

	public static org.apache98.hadoop.conf.Configuration getSenchaConf() {
		final org.apache98.hadoop.conf.Configuration conf = new org.apache98.hadoop.conf.Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.defaultFS", "hdfs://c-sencha-s01.us-w2.aws.ccl/");
		conf.set("ha.zookeeper.quorum",
				"c-sencha-s01.us-w2.aws.ccl:2181,c-sencha-s02.us-w2.aws.ccl:2181,c-sencha-s03.us-w2.aws.ccl:2181");
		conf.set("hbase.zookeeper.quorum",
				"c-sencha-s01.us-w2.aws.ccl,c-sencha-s02.us-w2.aws.ccl,c-sencha-s03.us-w2.aws.ccl");
		return conf;
	}

	/*
	 * @param errorMsg Error message. Can be null.
	 */
	public static void printUsage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		System.err.println("Usage: CopyTable2 [general options] [--starttime=X] [--endtime=Y] " +
				"[--new.name=NEW] [--peer.adr=ADR] <tablename>");
		System.err.println();
		System.err.println("Options:");
		System.err.println(" zk.quorum    hbase.zookeeper.quorum of 0.98 cluster");
		System.err.println(" region.split number of split per region. 1 means split once to 2 ranges");
		System.err.println(" rs.class     hbase.regionserver.class of the peer cluster");
		System.err.println("              specify if different from current cluster");
		System.err.println(" rs.impl      hbase.regionserver.impl of the peer cluster");
		System.err.println(" startrow     the start row");
		System.err.println(" stoprow      the stop row");
		System.err.println(" starttime    beginning of the time range (unixtime in millis)");
		System.err.println("              without endtime means from starttime to forever");
		System.err.println(" endtime      end of the time range.  Ignored if no starttime specified.");
		System.err.println(" versions     number of cell versions to copy");
		System.err.println(" new.name     new table's name");
		System.err.println(" peer.adr     Address of the peer cluster given in the format");
		System.err.println("              hbase.zookeeer.quorum:hbase.zookeeper.client.port:zookeeper.znode.parent");
		System.err.println(" families     comma-separated list of families to copy");
		System.err.println("              To copy from cf1 to cf2, give sourceCfName:destCfName. ");
		System.err.println("              To keep the same name, just give \"cfName\"");
		System.err.println(" all.cells    also copy delete markers and deleted cells");
		System.err.println();
		System.err.println("Args:");
		System.err.println(" tablename    Name of the table to copy");
		System.err.println();
		System.err.println("Examples:");
		System.err.println(" To copy 'TestTable' to a cluster that uses replication for a 1 hour window:");
		System.err.println(" $ bin/hbase " +
				"org.apache.hadoop.hbase.mapreduce.CopyTable --starttime=1265875194289 --endtime=1265878794289 " +
				"--peer.adr=server1,server2,server3:2181:/hbase --families=myOldCf:myNewCf,cf2,cf3 TestTable ");
		System.err.println("For performance consider the following general options:\n"
				+ "-Dhbase.client.scanner.caching=100\n"
				+ "-Dmapred.map.tasks.speculative.execution=false");
	}
}
