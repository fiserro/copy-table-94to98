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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.socialbakers.proto.ProtobufGeneral.ArrayOfDoubles;

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
		// scan.setCaching(400);
		scan.setBatch(2000);
		scan.addFamily(E);

		Job job = new Job(conf, jobName);
		job.setJarByClass(CopyTable2.class);

		job.setSpeculativeExecution(false);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper94_98.class, null, null, job, true, RegionSplitTableInputFormat.class);

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);

		JobConf jobConf = (JobConf) job.getConfiguration();

		// jobConf.set(NEW_TABLE_NAME, newTableName);
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

		private static final byte[] VIDEO_RETENTION_GRAPH = toBytes("VIDEO_RETENTION_GRAPH");
		private static final byte[] COMMENT_COUNT = toBytes("COMMENT_COUNT");
		private static final byte[] LIKE_COUNT = toBytes("LIKE_COUNT");
		private static final byte[] SHARE_COUNT = toBytes("SHARE_COUNT");
		private static final byte[] PROFILE_ID = toBytes("profile_id");
		private static final String FB_PAGE_POST_INSIGHTS_EVOLUTION = "FB_PAGE_POST_INSIGHTS_EVOLUTION";
		private static final String FB_POST_INSIGHTS_EVOLUTION = "FB_POST_INSIGHTS_EVOLUTION";
		private static final String FB_PAGE_POST_EVOLUTION = "FB_PAGE_POST_EVOLUTION";
		private static final String FB_POST_EVOLUTION = "FB_POST_EVOLUTION";
		private static final String[] TABLE_NAMES = new String[] { FB_POST_EVOLUTION, FB_PAGE_POST_EVOLUTION, FB_POST_INSIGHTS_EVOLUTION,
				FB_PAGE_POST_INSIGHTS_EVOLUTION };
		private static final byte[] _0 = toBytes("0");
		private static final String _ = "_";

		public static void main(String[] args) throws IOException, InterruptedException {

			Mapper94_98 mapper94_98 = new Mapper94_98();
			Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
			conf.set(HBASE_ZOOKEEPER_QUORUM, "zookeeper1");
			conf.set(HBASE_ZOOKEEPER_QUORUM2, "c-sencha-s01");
			Scan scan = new Scan(toBytesBinary("00_\\x00\\x00\\x00\\x01\\x98\\xE8\\x12\\xD4_\\x7F\\xDB\\xEDU\\x823\\xA5\\xF3"));
			scan.setCaching(10);
			HTable htable = new HTable(conf, "fb_posts_metric_evolution");
			ResultScanner scanner = htable.getScanner(scan);
			int i = 0;

			FakeContext context = mapper94_98.createFakeContext(conf);
			mapper94_98.setup(context);
			for (Result result : scanner) {
				if (++i > 3) {
					break;
				}
				mapper94_98.map(new ImmutableBytesWritable(result.getRow()), result, context);
			}
			mapper94_98.cleanup(context);
			for (Counter counter : context.getCounters().values()) {
				System.out.println(counter.getDisplayName() + ":" + counter.getValue());
			}
			htable.close();
		}

		private static String rowkeyToString(byte[] rowkey) {
			long pageId = Bytes.toLong(rowkey, 3, Bytes.SIZEOF_LONG);
			long postId = Long.MAX_VALUE - Bytes.toLong(rowkey, 4 + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
			return pageId + _ + postId;
		}

		// private Map<byte[], String> metricNames = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);

		private Map<byte[], byte[]> metrics = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR) {
			{
				put("1", "STORIES");
				put("2", "STORYTELLERS");
				put("3.like", "STORIES_BY_ACTION_TYPE_LIKE");
				put("3.comment", "STORIES_BY_ACTION_TYPE_COMMENT");
				put("3.share", "STORIES_BY_ACTION_TYPE_SHARE");
				put("3.claim", "STORIES_BY_ACTION_TYPE_CLAIM");
				put("3.answer", "STORIES_BY_ACTION_TYPE_ANSWER");
				put("3.follow", "STORIES_BY_ACTION_TYPE_FOLLOW");
				put("3.rsvp", "STORIES_BY_ACTION_TYPE_RSVP");
				put("3.other", "STORIES_BY_ACTION_TYPE_OTHER");
				put("4.like", "STORYTELLERS_BY_ACTION_TYPE_LIKE");
				put("4.comment", "STORYTELLERS_BY_ACTION_TYPE_COMMENT");
				put("4.share", "STORYTELLERS_BY_ACTION_TYPE_SHARE");
				put("4.claim", "STORYTELLERS_BY_ACTION_TYPE_CLAIM");
				put("4.answer", "STORYTELLERS_BY_ACTION_TYPE_ANSWER");
				put("4.follow", "STORYTELLERS_BY_ACTION_TYPE_FOLLOW");
				put("4.rsvp", "STORYTELLERS_BY_ACTION_TYPE_RSVP");
				put("4.other", "STORYTELLERS_BY_ACTION_TYPE_OTHER");
				put("5.like", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_LIKE");
				put("5.comment", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_COMMENT");
				put("5.share", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_SHARE");
				put("5.claim", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_CLAIM");
				put("5.answer", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_ANSWER");
				put("5.follow", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_FOLLOW");
				put("5.rsvp", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_RSVP");
				put("5.other", "STORY_ADDS_BY_ACTION_TYPE_UNIQUE_OTHER");
				put("6", "STORY_ADDS_UNIQUE");
				put("7", "STORY_ADDS");
				put("8", "IMPRESSIONS");
				put("9", "IMPRESSIONS_UNIQUE");
				put("10", "IMPRESSIONS_PAID");
				put("11", "IMPRESSIONS_PAID_UNIQUE");
				put("12", "IMPRESSIONS_FAN");
				put("13", "IMPRESSIONS_FAN_UNIQUE");
				put("14", "IMPRESSIONS_FAN_PAID");
				put("15", "IMPRESSIONS_FAN_PAID_UNIQUE");
				put("16", "IMPRESSIONS_ORGANIC");
				put("17", "IMPRESSIONS_ORGANIC_UNIQUE");
				put("18", "IMPRESSIONS_VIRAL");
				put("19", "IMPRESSIONS_VIRAL_UNIQUE");
				put("20.coupon", "IMPRESSIONS_BY_STORY_TYPE_COUPON");
				put("20.fan", "IMPRESSIONS_BY_STORY_TYPE_FAN");
				put("20.mention", "IMPRESSIONS_BY_STORY_TYPE_MENTION");
				put("20.other", "IMPRESSIONS_BY_STORY_TYPE_OTHER");
				put("20.question", "IMPRESSIONS_BY_STORY_TYPE_QUESTION");
				put("20.user post", "IMPRESSIONS_BY_STORY_TYPE_USER_POST");
				put("20.page post", "IMPRESSIONS_BY_STORY_TYPE_PAGE_POST");
				put("20.page subscribe", "IMPRESSIONS_BY_STORY_TYPE_PAGE_SUBSCRIBE");
				put("20.link", "IMPRESSIONS_BY_STORY_TYPE_LINK");
				put("20.comment", "IMPRESSIONS_BY_STORY_TYPE_COMMENT");
				put("21.coupon", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_COUPON");
				put("21.fan", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_FAN");
				put("21.mention", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_MENTION");
				put("21.other", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_OTHER");
				put("21.question", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_QUESTION");
				put("21.user post", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_USER_POST");
				put("21.page post", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_PAGE_POST");
				put("21.page subscribe", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_PAGE_SUBSCRIBE");
				put("21.link", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_LINK");
				put("21.comment", "IMPRESSIONS_BY_STORY_TYPE_UNIQUE_COMMENT");
				put("22.paid", "IMPRESSIONS_BY_PAID_NON_PAID_PAID");
				put("22.unpaid", "IMPRESSIONS_BY_PAID_NON_PAID_UNPAID");
				put("22.total", "IMPRESSIONS_BY_PAID_NON_PAID_TOTAL");
				put("23.paid", "IMPRESSIONS_BY_PAID_NON_PAID_UNIQUE_PAID");
				put("23.unpaid", "IMPRESSIONS_BY_PAID_NON_PAID_UNIQUE_UNPAID");
				put("23.total", "IMPRESSIONS_BY_PAID_NON_PAID_UNIQUE_TOTAL");
				put("24", "CONSUMPTIONS");
				put("25", "CONSUMPTIONS_UNIQUE");
				put("26.other clicks", "CONSUMPTIONS_BY_TYPE_OTHER_CLICKS");
				put("26.photo view", "CONSUMPTIONS_BY_TYPE_PHOTO_VIEW");
				put("26.video play", "CONSUMPTIONS_BY_TYPE_VIDEO_PLAY");
				put("26.link clicks", "CONSUMPTIONS_BY_TYPE_LINK_CLICKS");
				put("27.other clicks", "CONSUMPTIONS_BY_TYPE_UNIQUE_OTHER_CLICKS");
				put("27.photo view", "CONSUMPTIONS_BY_TYPE_UNIQUE_PHOTO_VIEW");
				put("27.video play", "CONSUMPTIONS_BY_TYPE_UNIQUE_VIDEO_PLAY");
				put("27.link clicks", "CONSUMPTIONS_BY_TYPE_UNIQUE_LINK_CLICKS");
				put("28", "ENGAGED_USERS");
				put("29", "NEGATIVE_FEEDBACK");
				put("30", "NEGATIVE_FEEDBACK_UNIQUE");
				put("31.hide_all_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_HIDE_ALL_CLICKS");
				put("31.hide_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_HIDE_CLICKS");
				put("31.report_spam_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_REPORT_SPAM_CLICKS");
				put("31.unlike_page_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNLIKE_PAGE_CLICKS");
				put("31.xbutton_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_XBUTTON_CLICKS");
				put("32.hide_all_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNIQUE_HIDE_ALL_CLICKS");
				put("32.hide_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNIQUE_HIDE_CLICKS");
				put("32.report_spam_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNIQUE_REPORT_SPAM_CLICKS");
				put("32.unlike_page_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNIQUE_UNLIKE_PAGE_CLICKS");
				put("32.xbutton_clicks", "NEGATIVE_FEEDBACK_BY_TYPE_UNIQUE_XBUTTON_CLICKS");
				put("33", "VIDEO_AVG_TIME_WATCHED");
				put("34", "VIDEO_COMPLETE_VIEWS_ORGANIC");
				put("35", "VIDEO_COMPLETE_VIEWS_ORGANIC_UNIQUE");
				put("36", "VIDEO_COMPLETE_VIEWS_PAID");
				put("37", "VIDEO_COMPLETE_VIEWS_PAID_UNIQUE");
				put("38", "VIDEO_VIEWS_ORGANIC");
				put("39", "VIDEO_VIEWS_ORGANIC_UNIQUE");
				put("40", "VIDEO_VIEWS_PAID");
				put("41", "VIDEO_VIEWS_PAID_UNIQUE");
				put("51.like", "STORY_ADDS_BY_ACTION_TYPE_LIKE");
				put("51.comment", "STORY_ADDS_BY_ACTION_TYPE_COMMENT");
				put("51.share", "STORY_ADDS_BY_ACTION_TYPE_SHARE");
				put("51.claim", "STORY_ADDS_BY_ACTION_TYPE_CLAIM");
				put("51.answer", "STORY_ADDS_BY_ACTION_TYPE_ANSWER");
				put("51.follow", "STORY_ADDS_BY_ACTION_TYPE_FOLLOW");
				put("51.rsvp", "STORY_ADDS_BY_ACTION_TYPE_RSVP");
				put("51.other", "STORY_ADDS_BY_ACTION_TYPE_OTHER");
				put("52", "VIDEO_RETENTION_GRAPH");
				put("48", "SHARE_COUNT");
				put("49", "LIKE_COUNT");
				put("50", "COMMENT_COUNT");
			}

			void put(String key, String value) {
				byte[] bytes = toBytes(value);
				put(toBytes(key), bytes);
				// metricNames.put(bytes, value);
			}
		};
		int bucketSize = 20000;

		private Multimap<String, Put> puts = ArrayListMultimap.create();

		private Map<String, org.apache98.hadoop.hbase.client.HTable> tables = new HashMap<String, org.apache98.hadoop.hbase.client.HTable>();

		private Converter integerC = new Converter() {
			@Override
			public Object doConvert(byte[] value, Context context) {
				try {
					return Bytes.toInt(value);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.integer.err").increment(1);
					return null;
				}
			}
		};

		private Converter doubleArrayC = new Converter() {
			@Override
			public Object doConvert(byte[] value, Context context) {
				try {
					return ArrayOfDoubles.parseFrom(value).getDataList();
				} catch (Exception e) {
					System.err.println("double_array_err:" + Bytes.toStringBinary(value));
					e.printStackTrace();
					context.getCounter("err", "converter.double_array.err").increment(1);
					return null;
				}
			}
		};

		private int cellCounter = 0;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			flushAll(context, true);
			for (org.apache98.hadoop.hbase.client.HTable table : tables.values()) {
				try {
					table.close();
				} catch (Exception e) {
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

			byte[] rowkey = key.get();
			String stringRowkey = rowkeyToString(rowkey);
			byte[] pageId = toBytes(stringRowkey.split(_)[0]);
			byte[] postId = toBytes(stringRowkey);
			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(E);
			if (familyMap == null) {
				return;
			}
			Map<Integer, Map<byte[], Object>> data = new TreeMap<Integer, Map<byte[], Object>>();
			for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
				byte[] column = entry.getKey();
				if (column.length <= 5) {
					System.out.println(toStringBinary(column) + ":" + toStringBinary(entry.getValue()));
					continue;
				}
				byte[] metric = new byte[column.length - 5];
				System.arraycopy(column, 0, metric, 0, metric.length);
				int tms = Bytes.toInt(column, column.length - 4);
				Map<byte[], Object> tmsData = data.get(tms);
				if (tmsData == null) {
					tmsData = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
					data.put(tms, tmsData);
				}
				tmsData.put(metric, entry.getValue());
			}

			for (Entry<Integer, Map<byte[], Object>> entry : data.entrySet()) {
				Map<byte[], Object> row = entry.getValue();
				Set<byte[]> keySet = new HashSet<byte[]>(row.keySet());
				for (byte[] sourceMetric : keySet) {
					byte[] bytes = (byte[]) row.remove(sourceMetric);
					byte[] targetMetric = metrics.get(sourceMetric);
					if (targetMetric == null) {
						context.getCounter("missing metric", Bytes.toString(sourceMetric)).increment(1);
						// System.out.println("missing metric: " + Bytes.toString(sourceMetric));
						continue;
					}
					Converter converter = Bytes.equals(VIDEO_RETENTION_GRAPH, targetMetric) ? doubleArrayC : integerC;
					Object value = converter.convert(bytes, context);
					if (value != null) {
						row.put(targetMetric, value);
						cellCounter++;
					}
				}
			}

			// for (Entry<Integer, Map<String, Object>> entry : data.entrySet()) {
			// System.out.println(rowkeyToString(rowkey) + ":" + new Date((long) entry.getKey() * 1000) + " : " +
			// entry.getValue());
			// }
			//
			// if (true) {
			// return;
			// }

			byte[] postRow = preparePostRow(postId);
			byte[] pageRow = preparePageRow(pageId, postId);
			for (Entry<Integer, Map<byte[], Object>> entry : data.entrySet()) {
				Date date = new Date((long) entry.getKey() * 1000);
				updateTime(date, postRow, postId.length + 1);
				updateTime(date, pageRow, pageId.length + 1);
				// Put postPut = new Put(postRow);
				// postPut.setDurability(Durability.SKIP_WAL);
				Put pagePut = new Put(pageRow);
				pagePut.setDurability(Durability.SKIP_WAL);
				// Put postInsPut = new Put(postRow);
				// postInsPut.setDurability(Durability.SKIP_WAL);
				Put pageInsPut = new Put(pageRow);
				pageInsPut.setDurability(Durability.SKIP_WAL);
				for (Entry<byte[], Object> entry2 : entry.getValue().entrySet()) {
					byte[] metric = entry2.getKey();
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
					if (Bytes.equals(metric, SHARE_COUNT) || Bytes.equals(metric, LIKE_COUNT)
							|| Bytes.equals(metric, COMMENT_COUNT)) {
						// context.getCounter("mapped1", metricNames.get(metric)).increment(1);
						// postPut.add(_0, metric, value);
						pagePut.add(_0, metric, value);
					} else {
						// context.getCounter("mapped2", metricNames.get(metric)).increment(1);
						// postInsPut.add(_0, metric, value);
						pageInsPut.add(_0, metric, value);
					}
				}
				// if (!postPut.isEmpty()) {
				// postPut.add(_0, PROFILE_ID, pageId);
				// puts.put(FB_POST_EVOLUTION, postPut);
				// }
				// if (!postInsPut.isEmpty()) {
				// postInsPut.add(_0, PROFILE_ID, pageId);
				// puts.put(FB_POST_INSIGHTS_EVOLUTION, postInsPut);
				// }
				if (!pagePut.isEmpty()) {
					puts.put(FB_PAGE_POST_EVOLUTION, pagePut);
				}
				if (!pageInsPut.isEmpty()) {
					puts.put(FB_PAGE_POST_INSIGHTS_EVOLUTION, pageInsPut);
				}
			}
			flushAll(context, false);
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
			for (String tableName : TABLE_NAMES) {
				org.apache98.hadoop.hbase.client.HTable table = (org.apache98.hadoop.hbase.client.HTable) connection
						.getTable(tableName);
				table.setAutoFlushTo(false);
				tables.put(tableName, table);
			}
		}

		private FakeContext createFakeContext(Configuration conf) throws IOException, InterruptedException {
			TaskAttemptID taskid = new TaskAttemptID("", 1, true, 1, 1);
			RecordReader<ImmutableBytesWritable, Result> reader = new RecordReader<ImmutableBytesWritable, Result>() {

				@Override
				public void close() throws IOException {
					// TODO Auto-generated method stub

				}

				@Override
				public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					return null;
				}

				@Override
				public Result getCurrentValue() throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					return null;
				}

				@Override
				public float getProgress() throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					return 0;
				}

				@Override
				public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
					// TODO Auto-generated method stub

				}

				@Override
				public boolean nextKeyValue() throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					return false;
				}
			};
			RecordWriter<ImmutableBytesWritable, KeyValue> writer = new RecordWriter<ImmutableBytesWritable, KeyValue>() {

				@Override
				public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
				}

				@Override
				public void write(ImmutableBytesWritable arg0, KeyValue arg1) throws IOException, InterruptedException {
				}
			};
			OutputCommitter committer = new OutputCommitter() {

				@Override
				public void abortTask(TaskAttemptContext arg0) throws IOException {
				}

				@Override
				public void commitTask(TaskAttemptContext arg0) throws IOException {
				}

				@Override
				public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
					return false;
				}

				@Override
				public void setupJob(JobContext arg0) throws IOException {
				}

				@Override
				public void setupTask(TaskAttemptContext arg0) throws IOException {
				}
			};
			StatusReporter reporter = new StatusReporter() {

				@Override
				public Counter getCounter(Enum<?> arg0) {
					return null;
				}

				@Override
				public Counter getCounter(String arg0, String arg1) {
					return null;
				}

				@Override
				public void progress() {
				}

				@Override
				public void setStatus(String arg0) {
				}
			};
			InputSplit split = new InputSplit() {

				@Override
				public long getLength() throws IOException, InterruptedException {
					return 0;
				}

				@Override
				public String[] getLocations() throws IOException, InterruptedException {
					return null;
				}
			};
			return new FakeContext(conf, taskid, reader, writer, committer, reporter, split);
		}

		private void flush(Context context, String table, List<Put> puts) throws IOException {
			int putSize = puts.size();
			if (putSize > 0) {
				HTableUtil.bucketRsPut(tables.get(table), puts);
				context.getCounter("hbase98", table + "-flush").increment(1);
				context.getCounter("hbase98", table + "-put").increment(putSize);
			}
		}

		private void flushAll(Context context, boolean force) throws IOException {
			if (force || cellCounter >= bucketSize) {
				for (String tableName : TABLE_NAMES) {
					Collection<Put> putList = puts.get(tableName);
					if (putList instanceof List) {
						flush(context, tableName, (List<Put>) putList);
					} else {
						flush(context, tableName, new ArrayList<Put>(putList));
					}
					puts.removeAll(tableName);
				}
				cellCounter = 0;
			}
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
			SortOrder.invert(rowkey, offset, rowkey, offset, Bytes.SIZEOF_LONG);
		}

		private abstract class Converter {
			Object convert(byte[] value, Context context) {
				if (value == null || value.length == 0) {
					return null;
				}
				return doConvert(value, context);
			}

			abstract Object doConvert(byte[] value, Context context);
		}

		private class FakeContext extends Mapper94_98.Context {

			private Map<String, Counter> counters = new TreeMap<String, Counter>();

			public FakeContext(Configuration conf, TaskAttemptID taskid, RecordReader<ImmutableBytesWritable, Result> reader,
					RecordWriter<ImmutableBytesWritable, KeyValue> writer, OutputCommitter committer, StatusReporter reporter,
					InputSplit split) throws IOException, InterruptedException {
				super(conf, taskid, reader, writer, committer, reporter, split);
			}

			@Override
			public Counter getCounter(String groupName, String counterName) {
				String key = groupName + ":" + counterName;
				Counter counter = counters.get(key);
				if (counter == null) {
					counter = CounterFactory.createCounter(groupName, counterName);
					counters.put(key, counter);
				}
				return counter;
			}

			public Map<String, Counter> getCounters() {
				return counters;
			}
		}

	}
}