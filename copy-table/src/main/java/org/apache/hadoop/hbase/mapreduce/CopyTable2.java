package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
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
import org.apache.phoenix.schema.PDataType;
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.Durability;
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.*;
import static org.apache.phoenix.schema.PDataType.*;
import static org.apache.phoenix.schema.PDataType.DATE;
import static org.apache.phoenix.schema.PDataType.INTEGER;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable2 extends Configured implements Tool {

	private static final byte[] E = toBytes("e");
	private static final byte[] ID = toBytes("id");
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
//		scan.setCaching(1);
		scan.setBatch(bucketSize);
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

		private static final byte[] D = toBytes("d");
		private static final byte[] ER = toBytes("er");
		private static final byte[] LIKE_COUNT = toBytes("like_count");
		private static final byte[] COMMENT_COUNT = toBytes("comment_count");
		private static final byte[] SHARE_COUNT = toBytes("share_count");
		private static final byte[] INTERACTION_COUNT = toBytes("interaction_count");

		public static void main(String[] args) throws IOException, InterruptedException {

			Mapper94_98 mapper94_98 = new Mapper94_98();
			Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
			conf.set(HBASE_ZOOKEEPER_QUORUM, "zookeeper1");
			conf.set(HBASE_ZOOKEEPER_QUORUM2, "c-sencha-s01");
			Scan scan = new Scan(toBytesBinary("0000004a51ae17ac48e598abb3dd7300"));
			scan.setCaching(10);
			org.apache.hadoop.hbase.client.HTable htable = new org.apache.hadoop.hbase.client.HTable(conf, Statics.tableName);
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

		private class Records extends TreeMap<Integer, Put> {
			public Put getOrCreate(Integer key, byte[] idBytes) {
				Put put = super.get(key);
				if (put == null) {
					System.arraycopy(idBytes, 0, rowkey, 0, 8);
					DATE.toBytes(new Date(key.longValue() * 1000), rowkey, 9);
					put = new Put(rowkey);
					put.setDurability(Durability.SKIP_WAL);
					super.put(key, put);
				}
				return put;
			}
		}

		private byte[] rowkey = new byte[17];
		private List<Put> puts = new ArrayList<Put>();

		@SuppressWarnings("unchecked")
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

			try {
				byte[] idBytes = result.getValue(E, ID);
				if (idBytes == null) {
					Get get = new Get(result.getRow());
					get.addColumn(E, ID);
					context.getCounter("custom", "reload missing id").increment(1);
					idBytes = inputTable.get(get).getValue(E, ID);
					if (idBytes == null) {
						context.getCounter("custom", "missed id").increment(1);
						return;
					}
				}
				long id = Long.valueOf(Bytes.toString(idBytes));
				idBytes = toBytes(rtCircShift(id, 42));

				Records data = new Records();
				KeyValue[] keyValues = result.raw();
				for (KeyValue kv : keyValues) {
					try {
						if (kv.getQualifierLength() < 6)
							continue;
						byte[] qualifier = kv.getQualifier();
						int tms = getTms(qualifier);
						if (tms > 1446336000) {
							context.getCounter("custom", "skip time").increment(1);
							continue;
						}
						byte[] value = kv.getValue();
						switch ((char)qualifier[0]) {
							case '1':
								data.getOrCreate(tms, idBytes).add(D, COMMENT_COUNT, convertInt(value, context));
								break;
							case '2':
								data.getOrCreate(tms, idBytes).add(D, SHARE_COUNT, convertInt(value, context));
								break;
							case '3':
								data.getOrCreate(tms, idBytes).add(D, LIKE_COUNT, convertInt(value, context));
								break;
							case '4':
								data.getOrCreate(tms, idBytes).add(D, INTERACTION_COUNT, convertInt(value, context));
								continue;
							case '5':
								data.getOrCreate(tms, idBytes).add(D, ER, convertDouble(value, context));
								break;
							case 'i':
								continue;
							default:
								continue;
						}
					} catch (Exception e) {
						context.getCounter("custom", "err_kv " + e.getMessage()).increment(1);
					}
				}

				puts.addAll(data.values());
				if (puts.size() >= bucketSize)
					flush(context);

			} catch (Exception e) {
				context.getCounter("custom", "err_result " + e.getMessage()).increment(1);
			}
		}

		private byte[] convertDouble(byte[] value, Context context) {
			if (value.length != 4) {
				context.getCounter("custom", "not 4B value").increment(1);
				return new byte[0];
			}
			return DOUBLE.toBytes((double) toFloat(value));
		}

		private byte[] convertInt(byte[] value, Context context) {
			if (value.length != 4) {
				context.getCounter("custom", "not 4B value").increment(1);
				return new byte[0];
			}
			return INTEGER.toBytes(toInt(value));
		}

		private int getTms(byte[] column) {
			return toInt(column, column.length - 4);
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

		private long rtCircShift(long bits, int k) {
			return (bits >>> k) | (bits << (Long.SIZE - k));
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			org.apache98.hadoop.conf.Configuration conf98 = HBaseConfiguration.create();
			conf98.set(HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM2));
			table = new org.apache98.hadoop.hbase.client.HTable(conf98, "tw_tweets_evolution");
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