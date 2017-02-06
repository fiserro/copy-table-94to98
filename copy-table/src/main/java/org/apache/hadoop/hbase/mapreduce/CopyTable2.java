package org.apache.hadoop.hbase.mapreduce;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.PDataType;
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.Durability;
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.*;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable2 extends Configured implements Tool {

	private static final byte[] D = toBytes("d");
	private static final byte[] TYPE = toBytes("type");

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
			scan.setStartRow(toBytesBinary(startRow));
			jobName += ("_" + startRow);
		} else {
			jobName += "_firstRow";
		}

		if (stopRow != null) {
			scan.setStopRow(toBytesBinary(stopRow));
			jobName += ("-" + stopRow);
		} else {
			jobName += "-lastRow";
		}
		scan.setFilter(new SingleColumnValueFilter(D, TYPE, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("c")));

		scan.setCaching(1000);

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

		private static final byte[] ID_ORIG = toBytes("id_orig");
		private static final byte[] PAGE_ID = toBytes("page_id");
		private static final byte[] POST_ID = toBytes("post_id");
		private static final byte[] USER_ID = toBytes("user_id");
		private static final byte[] AUTHOR_ID = toBytes("author_id");
		private static final byte[] MESSAGE = toBytes("message");
		private static final byte[] CREATED_TIME = toBytes("created_time");


		public static void main(String[] args) throws IOException, InterruptedException {

			Mapper94_98 mapper94_98 = new Mapper94_98();

//			System.out.println(Bytes.toStringBinary(mapper94_98.toRow(12,456,8888,6554464)));
//			System.out.println(Bytes.toStringBinary(mapper94_98.toRow(12,452346,843488,655655656)));
//			System.out.println(Bytes.toStringBinary(mapper94_98.toRow(12,454546545346L,8843488,455)));
//
//			if (true)
//				return;

			Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
			conf.set(HBASE_ZOOKEEPER_QUORUM, "zkservices2");
			conf.set(HBASE_ZOOKEEPER_QUORUM2, "c-sencha-s01");
//			Scan scan = new Scan(toBytesBinary("0000004a51ae17ac48e598abb3dd7300"));
			Scan scan = new Scan();
			scan.setCaching(1000);
			scan.setFilter(new SingleColumnValueFilter(D, TYPE, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("c")));
			org.apache.hadoop.hbase.client.HTable htable = new org.apache.hadoop.hbase.client.HTable(conf, Statics.tableName);
			ResultScanner scanner = htable.getScanner(scan);
			int i = 0;

			FakeContext context = mapper94_98.createFakeContext(conf);
			mapper94_98.setup(context);
			for (Result result : scanner) {
				if (++i > 10) {
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

		private org.apache98.hadoop.hbase.client.HTable table;
		private org.apache.hadoop.hbase.client.HTable inputTable;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			flush(context);
			try {
				table.close();
			} catch (Exception e) {
			}
			try {
				inputTable.close();
			} catch (Exception e) {
			}
		}

		private List<Put> puts = new ArrayList<Put>();

//		"d:id_orig": dt.Long
//		"d:page_id": dt.Long
//		"d:post_id": dt.Long
//		"d:user_id": dt.Long
//		"d:type": dt.String
//		"d:message": dt.String
//		"d:created_time": dt.Long

		@SuppressWarnings("unchecked")
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

			try {
				byte[] idOrig = result.getValue(D, ID_ORIG);
				byte[] profileId = result.getValue(D, PAGE_ID);
				byte[] postId = result.getValue(D, POST_ID);
				byte[] authorId = result.getValue(D, USER_ID);
				byte[] message = result.getValue(D, MESSAGE);
				byte[] createdTime = result.getValue(D, CREATED_TIME);
				long invertedCreatedTime = Long.MAX_VALUE - toLong(createdTime);
				createdTime = toBytes(invertedCreatedTime);

//				System.out.println(new Row(idOrig, profileId, postId, authorId, message, createdTime));

				Put put = new Put(toRow(profileId, postId, createdTime, idOrig));
				put.setDurability(Durability.SKIP_WAL);
				put.add(D, AUTHOR_ID, toBytes("" + toLong(authorId)));
				put.add(D, MESSAGE, message);
				puts.add(put);

				if (puts.size() >= bucketSize)
					flush(context);

			} catch (Exception e) {
				context.getCounter("custom", "err_result " + e.getMessage()).increment(1);
			}
		}

		private static class Row {
			byte[] idOrig;
			byte[] profileId;
			byte[] postId;
			byte[] authorId;
			byte[] message;
			byte[] createdTime;

			public Row(byte[] idOrig, byte[] profileId, byte[] postId, byte[] authorId, byte[] message, byte[] createdTime) {
				this.idOrig = idOrig;
				this.profileId = profileId;
				this.postId = postId;
				this.authorId = authorId;
				this.message = message;
				this.createdTime = createdTime;
			}

			@Override
			public String toString() {
				return new ToStringBuilder(this)
						.append("idOrig", toLong(idOrig))
						.append("profileId", toLong(profileId))
						.append("postId", toLong(postId))
						.append("authorId", toLong(authorId))
						.append("message", toStringBinary(message))
						.append("createdTime", new Date(Long.MAX_VALUE - toLong(createdTime)))
						.toString();
			}
		}

		public byte[] toRow(long profileId, long postId, long time, long idOrig) {
			return toRow(toBytes(profileId), toBytes(postId), toBytes(time), toBytes(idOrig));
		}

		private byte[] toRow(byte[] profileId, byte[] postId, byte[] time, byte[] idOrig) {
			byte[] row = new byte[34];
			ByteBuffer bb = ByteBuffer.wrap(row);
			bb.position(2);
			bb.put(profileId);
			bb.put(postId);
			bb.put(time);
			bb.put(idOrig);
			CRC32 crc32 = new CRC32();
			crc32.update(row, 2, 8);
			short crc = (short) crc32.getValue();
			bb.putShort(0, crc);
			return row;
		}

		private void flush(Context context) throws IOException {
			int putSize = puts.size();
			if (putSize > 0) {
				HTableUtil.bucketRsPut(table, puts);
				context.getCounter("custom", "flush").increment(1);
				context.getCounter("custom", "put").increment(putSize);
				puts.clear();
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			org.apache98.hadoop.conf.Configuration conf98 = HBaseConfiguration.create();
			conf98.set(HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM2));
			table = new org.apache98.hadoop.hbase.client.HTable(conf98, "ig_comments");
			inputTable = new org.apache.hadoop.hbase.client.HTable(conf, Statics.tableName);
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