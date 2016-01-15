package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytesBinary;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.HConnection;
import org.apache98.hadoop.hbase.client.HConnectionManager;
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable2 extends Configured implements Tool {

	private static final byte[] E = toBytes("e");

	/**
	 * Sets up the actual job.
	 *
	 * @param conf
	 *            The current configuration.
	 * @param args
	 *            The command line parameters.
	 * @return The newly created job.
	 * @throws IOException
	 *             When setting up the job fails.
	 */
	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		if (!doCommandLine(args)) {
			return null;
		}

		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		if (startTime != 0) {
			scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
		}
		if (allCells) {
			scan.setRaw(true);
		}
		if (versions >= 0) {
			scan.setMaxVersions(versions);
		}

		String jobName = NAME + "_" + tableName;

		if (startRow != null) {
			scan.setStartRow(Bytes.toBytes(startRow));
			jobName += ("_" + startRow);
		} else {
			jobName += "_firstRow";
		}

		if (stopRow != null) {
			scan.setStopRow(Bytes.toBytes(stopRow));
			jobName += ("-" + stopRow);
		} else {
			jobName += "-lastRow";
		}
		scan.setCaching(400);
		scan.addFamily(E);

		Job job = new Job(conf, jobName);
		job.setJarByClass(CopyTable2.class);

		job.setSpeculativeExecution(false);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper94_98.class, null, null, job, true, RegionSplitTableInputFormat.class);

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);

		JobConf jobConf = (JobConf) job.getConfiguration();

		jobConf.set(NEW_TABLE_NAME, newTableName);
		jobConf.set(HBASE_ZOOKEEPER_QUORUM2, zkQuorum);
		jobConf.setInt(RegionSplitTableInputFormat.REGION_SPLIT, regionSplit);
		jobConf.setInt(SALT_BYTES, saltBytes);
		jobConf.setInt(BUCKET_SIZE, bucketSize);

		// System.out.println(tableName);
		// System.out.println(newTableName);
		// System.out.println(zkQuorum);

		// TableMapReduceUtil.initTableReducerJob(
		// newTableName == null ? tableName : newTableName, null, job,
		// null, peerAddress, null, null);

		return job;
	}

	/**
	 * Main entry point.
	 *
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CopyTable2(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		Job job = createSubmittableJob(getConf(), otherArgs);
		if (job == null) {
			return 1;
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static class Mapper94_98 extends TableMapper<ImmutableBytesWritable, KeyValue> {

		private static final byte[] PROFILE_ID = toBytes("profile_id");
		private static final String FB_PAGE_POST_INSIGHTS_EVOLUTION = "FB_PAGE_POST_INSIGHTS_EVOLUTION";
		private static final String FB_POST_INSIGHTS_EVOLUTION = "FB_POST_INSIGHTS_EVOLUTION";
		private static final String FB_PAGE_POST_EVOLUTION = "FB_PAGE_POST_EVOLUTION";
		private static final String FB_POST_EVOLUTION = "FB_POST_EVOLUTION";
		private static final byte[] _0 = toBytes("0");
		private static final String _ = "_";
		private static final String DOT = ".";
		private static final byte[] DOT_BYTES = toBytes(DOT);

		public static void main(String[] args) {
			System.out
					.println(rowkeyToString(toBytesBinary("00_\\x00\\x00\\x00\\x01\\x98\\xE8\\x12\\xD4_\\x7F\\xDB\\xEDS\\xDF\\x06\\xFER")));
		}

		private static String rowkeyToString(byte[] rowkey) {
			long pageId = Bytes.toLong(rowkey, 3, Bytes.SIZEOF_LONG);
			long postId = Long.MAX_VALUE - Bytes.toLong(rowkey, 4 + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
			return pageId + _ + postId;
		}

		int bucketSize = 30000;

		private Multimap<String, Put> puts = ArrayListMultimap.create();

		private Map<String, org.apache98.hadoop.hbase.client.HTable> tables = new HashMap<String, org.apache98.hadoop.hbase.client.HTable>();

		private Converter integerC = new Converter() {
			@Override
			public byte[] convert(byte[] value, Context context) {
				try {
					return PDataType.INTEGER.toBytes(Integer.valueOf(Bytes.toString(value)));
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.integer.err").increment(1);
					return null;
				}
			}
		};

		private Converter floatC = new Converter() {
			@Override
			public byte[] convert(byte[] value, Context context) {
				try {
					String string = Bytes.toString(value);
					string = string.replaceAll("\"", "");
					return PDataType.FLOAT.toBytes(Integer.valueOf(string));
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.float.err").increment(1);
					return null;
				}
			}
		};

		private Converter doubleArrayC = new Converter() {
			@Override
			public byte[] convert(byte[] value, Context context) {
				String string = Bytes.toString(value);
				try {
					List<Double> doubles = new ArrayList<Double>();
					if (string.startsWith("[")) {
						string = string.replaceFirst("\\[", "").replaceFirst("\\]", "");
						for (String dStr : string.split(",")) {
							doubles.add(Double.valueOf(dStr.trim()));
						}
					} else if (string.startsWith("{")) {
						NavigableMap<Integer, Double> sortedMap = new TreeMap<Integer, Double>();
						for (Entry<String, Object> entry : ((Map<String, Object>) mapper.readValue(string, Map.class)).entrySet()) {
							sortedMap.put(Integer.valueOf(entry.getKey()), Double.valueOf(entry.getValue().toString()));
						}
						doubles.addAll(sortedMap.values());
					}
					PhoenixArray phoenixArray = PArrayDataType.instantiatePhoenixArray(PDataType.DOUBLE, doubles.toArray());
					return PDataType.DOUBLE_ARRAY.toBytes(phoenixArray);
				} catch (Exception e) {
					System.err.println("double_array_err:" + string);
					e.printStackTrace();
					context.getCounter("err", "converter.double_array.err").increment(1);
					return null;
				}
			}
		};

		private ObjectMapper mapper = new ObjectMapper();

		private Map<byte[], Map<String, Pair<byte[], String>>> nestedQualifiers = new TreeMap<byte[], Map<String, Pair<byte[], String>>>(
				Bytes.BYTES_COMPARATOR);

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			for (Entry<String, Collection<Put>> entry : puts.asMap().entrySet()) {
				Collection<Put> putList = entry.getValue();
				if (putList instanceof List) {
					flush(context, entry.getKey(), (List<Put>) putList);
				} else {
					flush(context, entry.getKey(), new ArrayList<Put>(putList));
				}
				putList.clear();
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

			byte[] rowkey = key.get();
			String[] stringKey = rowkeyToString(rowkey).split(_);
			byte[] pageId = toBytes(stringKey[0]);
			byte[] postId = toBytes(stringKey[1]);
			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(E);
			if (familyMap == null) {
				return;
			}
			Map<Integer, Map<String, Object>> data = new HashMap<Integer, Map<String, Object>>();
			for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
				String[] split = toStringBinary(entry.getKey()).split(_, 2);
				if (split.length != 2) {
					continue;
				}
				int tms = Bytes.toInt(toBytesBinary(split[1]));
				Map<String, Object> tmsData = data.get(tms);
				if (tmsData == null) {
					tmsData = new HashMap<String, Object>();
					data.put(tms, tmsData);
				}
				String metric = split[0];
				tmsData.put(metric, entry.getValue());
			}

			for (Entry<Integer, Map<String, Object>> entry : data.entrySet()) {

			}

			byte[] postRow = preparePostRow(postId);
			byte[] pageRow = preparePageRow(pageId, postId);
			for (Entry<Integer, Map<String, Object>> entry : data.entrySet()) {
				Date date = new Date(entry.getKey());
				updateTime(date, postRow, postId.length + 1);
				updateTime(date, pageRow, pageId.length + 1);
				Put postPut = new Put(postRow);
				Put pagePut = new Put(pageRow);
				Put postInsPut = new Put(postRow);
				Put pageInsPut = new Put(pageRow);
				for (Entry<String, Object> entry2 : entry.getValue().entrySet()) {
					String metric = entry2.getKey();
					byte[] value;
					if (entry2.getValue() instanceof Integer) {
						value = PDataType.INTEGER.toBytes(entry2.getValue());
					} else if (entry2.getValue() instanceof Float) {
						value = PDataType.FLOAT.toBytes(entry2.getValue());
					} else if (entry2.getValue() instanceof List) {
						PhoenixArray array = PArrayDataType.instantiatePhoenixArray(PDataType.DOUBLE,
								((List<Object>) entry2.getValue()).toArray());
						value = PDataType.DOUBLE_ARRAY.toBytes(array);
					} else {
						continue;
					}
					if (metric.matches("shares|likes|comments")) {
						postPut.add(_0, toBytes(entry2.getKey()), value);
						pagePut.add(_0, toBytes(entry2.getKey()), value);
					} else {
						postInsPut.add(_0, toBytes(entry2.getKey()), value);
						pageInsPut.add(_0, toBytes(entry2.getKey()), value);
					}
				}
				if (!postPut.isEmpty()) {
					postPut.add(_0, PROFILE_ID, pageId);
					puts.put(FB_POST_EVOLUTION, postPut);
				}
				if (!postInsPut.isEmpty()) {
					postInsPut.add(_0, PROFILE_ID, pageId);
					puts.put(FB_POST_INSIGHTS_EVOLUTION, postInsPut);
				}
				if (!pagePut.isEmpty()) {
					puts.put(FB_PAGE_POST_EVOLUTION, pagePut);
				}
				if (!pageInsPut.isEmpty()) {
					puts.put(FB_PAGE_POST_INSIGHTS_EVOLUTION, pageInsPut);
				}
			}
			for (Entry<String, Collection<Put>> entry : puts.asMap().entrySet()) {
				Collection<Put> putList = entry.getValue();
				if (putList.size() >= bucketSize) {
					if (putList instanceof List) {
						flush(context, entry.getKey(), (List<Put>) putList);
					} else {
						flush(context, entry.getKey(), new ArrayList<Put>(putList));
					}
					putList.clear();
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			this.bucketSize = conf.getInt(BUCKET_SIZE, 4000);
			org.apache98.hadoop.conf.Configuration conf98 = HBaseConfiguration.create();
			// conf98.set("hbase.client.write.buffer", "20971520");
			conf98.set(HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM2));
			HConnection connection = HConnectionManager.createConnection(conf98);
			for (String tableName : new String[] { FB_POST_EVOLUTION, FB_PAGE_POST_EVOLUTION, FB_POST_INSIGHTS_EVOLUTION,
					FB_PAGE_POST_INSIGHTS_EVOLUTION }) {
				org.apache98.hadoop.hbase.client.HTable table = (org.apache98.hadoop.hbase.client.HTable) connection
						.getTable(conf.get(NEW_TABLE_NAME));
				table.setAutoFlushTo(false);
				tables.put(tableName, table);
			}
		}

		private void flush(Context context, String table, List<Put> puts) throws IOException {
			int putSize = puts.size();
			if (putSize > 0) {
				HTableUtil.bucketRsPut(tables.get(table), puts);
				context.getCounter("hbase98", table + "flush").increment(1);
				context.getCounter("hbase98", table + "put").increment(putSize);
				puts.clear();
			}
		}

		private byte[] getQBytes(byte[] qualifier, String sub) {
			return getQPair(qualifier, sub).getFirst();
		}

		private Pair<byte[], String> getQPair(byte[] qualifier, String sub) {
			Map<String, Pair<byte[], String>> subMap = nestedQualifiers.get(qualifier);
			if (subMap == null) {
				subMap = new HashMap<String, Pair<byte[], String>>();
				nestedQualifiers.put(qualifier, subMap);
			}
			Pair<byte[], String> pair = subMap.get(sub);
			if (pair == null) {
				pair = new Pair<byte[], String>(Bytes.add(qualifier, DOT_BYTES, toBytes(sub)), Bytes.toString(qualifier) + DOT + sub);
				subMap.put(sub, pair);
			}
			return pair;
		}

		private byte[] preparePageRow(byte[] pageId, byte[] postId) {
			byte[] rowkey2 = new byte[pageId.length + 1 + Bytes.SIZEOF_LONG + postId.length];
			ByteBuffer rkb2 = ByteBuffer.wrap(rowkey2);
			rkb2.put(pageId);
			rkb2.position(pageId.length + 1 + Bytes.SIZEOF_LONG);
			rkb2.put(postId);
			return rowkey2;
		}

		private byte[] preparePostRow(byte[] postId) {
			byte[] rowkey1 = new byte[postId.length + 1 + Bytes.SIZEOF_LONG];
			ByteBuffer rkb1 = ByteBuffer.wrap(rowkey1);
			rkb1.put(postId);
			return rowkey1;
		}

		private void updateTime(Date date, byte[] rowkey, int offset) {
			PDataType.DATE.toBytes(date, rowkey, offset);
			SortOrder.invert(rowkey, offset, Bytes.SIZEOF_LONG);
		}

		private interface Converter {
			byte[] convert(byte[] value, Context context);
		}

	}
}