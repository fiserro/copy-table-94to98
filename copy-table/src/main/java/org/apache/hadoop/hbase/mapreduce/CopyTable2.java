package org.apache.hadoop.hbase.mapreduce;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.hadoop.hbase.mapreduce.Statics.BUCKET_SIZE;
import static org.apache.hadoop.hbase.mapreduce.Statics.HBASE_ZOOKEEPER_QUORUM;
import static org.apache.hadoop.hbase.mapreduce.Statics.HBASE_ZOOKEEPER_QUORUM2;
import static org.apache.hadoop.hbase.mapreduce.Statics.NAME;
import static org.apache.hadoop.hbase.mapreduce.Statics.NEW_TABLE_NAME;
import static org.apache.hadoop.hbase.mapreduce.Statics.SALT_BYTES;
import static org.apache.hadoop.hbase.mapreduce.Statics.allCells;
import static org.apache.hadoop.hbase.mapreduce.Statics.bucketSize;
import static org.apache.hadoop.hbase.mapreduce.Statics.doCommandLine;
import static org.apache.hadoop.hbase.mapreduce.Statics.endTime;
import static org.apache.hadoop.hbase.mapreduce.Statics.families;
import static org.apache.hadoop.hbase.mapreduce.Statics.newTableName;
import static org.apache.hadoop.hbase.mapreduce.Statics.regionSplit;
import static org.apache.hadoop.hbase.mapreduce.Statics.saltBytes;
import static org.apache.hadoop.hbase.mapreduce.Statics.startRow;
import static org.apache.hadoop.hbase.mapreduce.Statics.startTime;
import static org.apache.hadoop.hbase.mapreduce.Statics.stopRow;
import static org.apache.hadoop.hbase.mapreduce.Statics.tableName;
import static org.apache.hadoop.hbase.mapreduce.Statics.versions;
import static org.apache.hadoop.hbase.mapreduce.Statics.zkQuorum;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.PDataType;
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.Durability;
import org.apache98.hadoop.hbase.client.HConnection;
import org.apache98.hadoop.hbase.client.HConnectionManager;
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import com.socialbakers.proto.SocialContents.SocialContent.Attachments;
import com.socialbakers.proto.SocialContents.SocialContent.Attachments.Attachment;
import com.socialbakers.proto.SocialContents.SocialContent.Attachments.Attachment.Builder;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable2 extends Configured implements Tool {

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
	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {
		if (!doCommandLine(args)) {
			return null;
		}

		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		if (startTime != 0) {
			scan.setTimeRange(startTime,
					endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
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

		Job job = new Job(conf, jobName);
		job.setJarByClass(CopyTable2.class);
		if (families != null) {
			String[] fams = families.split(",");
			Map<String, String> cfRenameMap = new HashMap<String, String>();
			for (String fam : fams) {
				String sourceCf;
				if (fam.contains(":")) {
					// fam looks like "sourceCfName:destCfName"
					String[] srcAndDest = fam.split(":", 2);
					sourceCf = srcAndDest[0];
					String destCf = srcAndDest[1];
					cfRenameMap.put(sourceCf, destCf);
				} else {
					// fam is just "sourceCf"
					sourceCf = fam;
				}
				scan.addFamily(Bytes.toBytes(sourceCf));
			}
			Import.configureCfRenaming(job.getConfiguration(), cfRenameMap);
		}
		scan.setCaching(400);

		job.setSpeculativeExecution(false);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper94_98.class, null, null, job, true,
				RegionSplitTableInputFormat.class);

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
		Configuration c = new Configuration();
		// c.set("hbase.rootdir", "hdfs://hadoops-master:9000/hbase");
		int ret = ToolRunner.run(new CopyTable2(c), args);
		System.exit(ret);
	}

	public CopyTable2(Configuration conf) {
		super(conf);
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

		private static final byte[] M = toBytes("m");
		private static final byte[] D = toBytes("d");
		private static final byte[] ID = toBytes("id");
		private static final byte[] ATTACH_TYPE = toBytes("attach_type");
		private static final byte[] ATTACH_SRC = toBytes("attach_src");
		private static final byte[] ATTACH_URL = toBytes("attach_url");
		private static final byte[] ATTACH_TARGET = toBytes("attach_target");
		private static final byte[] ATTACH_H = toBytes("attach_h");
		private static final byte[] ATTACH_W = toBytes("attach_w");
		private static final byte[] ATTACHMENTS = toBytes("attachments");
		private static final byte[] CREATED_TIME = toBytes("created_time");
		private static final byte[] ID_ORIG = toBytes("id_orig");
		private static final byte[] ORIGINAL_ID = toBytes("original_id");
		private static final byte[] OBJECT_ID = toBytes("object_id");
		private static final byte[] MESSAGE = toBytes("message");
		private static final byte[] AUTHOR_ID = toBytes("author_id");
		private static final byte[] USER_ID = toBytes("user_id");
		private static final byte[] POST_ID = toBytes("post_id");
		private static final byte[] PARENT_ID = toBytes("parent_id");
		private static final byte[] LIKE_COUNT = toBytes("like_count");
		private static final byte[] LIKES = toBytes("likes");
		private static final byte[] FACEBOOK_PARENT_COMMENT_ID = toBytes("facebook.parent_comment_id");

		// public static void main(String[] args) throws ZooKeeperConnectionException, IOException, InterruptedException
		// {
		//
		// UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hbase");
		// PrivilegedExceptionAction<Put> action = new PrivilegedExceptionAction<Put>() {
		// @Override
		// public Put run() throws Exception {
		// Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum", "zookeeper1");
		// HTable table = new HTable(conf, "fb_comments");
		// Get get = new Get(
		// toBytesBinary("00_\\x00\\x00\\x00\\x01?\\x1C\\xA3<_\\x7F\\xDB\\xBC\\xA0\\x81{\\xE8m_\\x7F\\xDB\\xBC\\xA0o\\xEF\\x9B\\xAD"));
		// Result result = table.get(get);
		// byte[] id = getBytes(ID, result);
		// byte[] createdTime = getPhoenixDateBytes(CREATED_TIME, result);
		// Put put = createPut(result, id, createdTime);
		// table.close();
		//
		// return put;
		// }
		//
		// };
		// Put put = ugi.doAs(action);
		//
		// org.apache98.hadoop.conf.Configuration conf98 = HBaseConfiguration.create();
		// conf98.set("hbase.zookeeper.quorum", "c-sencha-s01");
		// HTableInterface table98 = HConnectionManager.createConnection(conf98).getTable("fb_comments");
		// table98.put(put);
		// table98.close();
		// }

		// private static final byte[] _D = Bytes.toBytes("d");
		private org.apache98.hadoop.hbase.client.HTable table;

		private List<Put> puts = new ArrayList<Put>();

		int bucketSize = 30000;

		private int saltBytes;

		int offset;

		private CRC32 crc = new CRC32();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			flush(context);
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException,
				InterruptedException {

			byte[] id = getBytes(ID, result);
			if (id == null) {
				context.getCounter("hbase98", "miss_id").increment(1);
				return;
			}

			byte[] createdTime = getPhoenixDateBytes(CREATED_TIME, result);
			if (createdTime == null) {
				context.getCounter("hbase98", "miss_created_time").increment(1);
				return;
			}

			Put put = createPut(result, id, createdTime);
			puts.add(put);
			if (puts.size() >= bucketSize) {
				flush(context);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			this.bucketSize = conf.getInt(BUCKET_SIZE, 30000);
			this.saltBytes = conf.getInt(SALT_BYTES, -1);
			this.offset = Bytes.SIZEOF_LONG - this.saltBytes;
			org.apache98.hadoop.conf.Configuration conf98 = HBaseConfiguration.create();
			// conf98.set("hbase.client.write.buffer", "20971520");
			conf98.set(HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM2));
			HConnection connection = HConnectionManager.createConnection(conf98);
			table = (org.apache98.hadoop.hbase.client.HTable) connection.getTable(conf.get(NEW_TABLE_NAME));
			table.setAutoFlushTo(false);
		}

		private Put createPut(Result result, byte[] id, byte[] createdTime) {
			Put put = new Put(getRow(id));
			put.setDurability(Durability.SKIP_WAL);

			put(result, ID_ORIG, put, ORIGINAL_ID);
			// put(result, OBJECT_ID, put, OBJECT_ID);
			put(result, MESSAGE, put, MESSAGE);
			put(put, CREATED_TIME, createdTime);
			put(result, USER_ID, put, AUTHOR_ID);
			put(result, POST_ID, put, PARENT_ID);
			put(put, LIKE_COUNT, getPhoenixIntegerBytes(LIKES, result));
			put(result, PARENT_ID, put, FACEBOOK_PARENT_COMMENT_ID);

			String attach_type = getString(ATTACH_TYPE, result);
			String attach_src = getString(ATTACH_SRC, result);
			String attach_url = getString(ATTACH_URL, result);
			String attach_target = getString(ATTACH_TARGET, result);
			Integer attach_h = getInteger(ATTACH_H, result);
			Integer attach_w = getInteger(ATTACH_W, result);
			Attachments.Builder attachmentsBuilder = Attachments.newBuilder();
			if (isNotBlank(attach_type) || isNotBlank(attach_src) || isNotBlank(attach_url) || isNotBlank(attach_target)
					|| attach_h != null || attach_w != null) {
				Builder attachmentBuilder = Attachment.newBuilder();
				if (isNotBlank(attach_type)) {
					attachmentBuilder.setObjectType(attach_type);
				}
				if (isNotBlank(attach_src)) {
					attachmentBuilder.setDisplayName(attach_src);
				}
				if (isNotBlank(attach_url)) {
					attachmentBuilder.setId(attach_url);
				}
				if (isNotBlank(attach_target)) {
					attachmentBuilder.setContent(attach_target);
				}

				Attachment.Image.Builder imageBuilder = Attachment.Image.newBuilder();
				if (isNotBlank(attach_url)) {
					imageBuilder.setUrl(attach_url);
				}
				if (isNotBlank(attach_type)) {
					imageBuilder.setType(attach_type);
				}
				if (attach_h != null) {
					imageBuilder.setHeight(attach_h);
				}
				if (attach_w != null) {
					imageBuilder.setWidth(attach_w);
				}
				if (imageBuilder.hasUrl() || imageBuilder.hasType() || imageBuilder.hasHeight() || imageBuilder.hasWidth()) {
					attachmentBuilder.setImage(imageBuilder);
				}

				attachmentsBuilder.addAttachment(attachmentBuilder);
				put(put, ATTACHMENTS, attachmentsBuilder.build().toByteArray());
			}
			return put;
		}

		private void flush(Context context) throws IOException {
			int putSize = puts.size();
			if (putSize > 0) {
				HTableUtil.bucketRsPut(table, puts);
				context.getCounter("hbase98", "put").increment(putSize);
				puts.clear();
			}
		}

		private byte[] getBytes(byte[] qualifier, Result result) {
			return result.getValue(M, qualifier);
		}

		private Integer getInteger(byte[] qualifier, Result result) {
			byte[] value = getBytes(qualifier, result);
			if (value == null) {
				return null;
			}
			return Integer.valueOf(Bytes.toString(value));
		}

		private byte[] getPhoenixDateBytes(byte[] qualifier, Result result) {
			String value = getString(qualifier, result);
			if (value == null) {
				return null;
			}
			try {
				return toPhoenixDate(value);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		private byte[] getPhoenixIntegerBytes(byte[] qualifier, Result result) {
			Integer value = getInteger(qualifier, result);
			if (value == null) {
				return null;
			}
			try {
				return toPhoenixInteger(value);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		private byte[] getRow(byte[] row) {
			if (saltBytes > 0) {
				ByteBuffer rb = ByteBuffer.allocate(row.length + saltBytes);
				crc.update(row);
				long value = crc.getValue();
				byte[] bytes = toBytes(value);
				rb.put(bytes, offset, saltBytes);
				rb.put(row);
				row = rb.array();
			}
			return row;
		}

		private String getString(byte[] qualifier, Result result) {
			byte[] value = getBytes(qualifier, result);
			if (value == null) {
				return null;
			}
			return Bytes.toString(value);
		}

		private void put(Put put, byte[] qualifier, byte[] value) {
			if (value == null) {
				return;
			}
			put.add(D, qualifier, value);
		}

		private void put(Result result, byte[] qualifierSource, Put put, byte[] qualifierTarget) {
			put(put, qualifierTarget, getBytes(qualifierSource, result));
		}

		private byte[] toPhoenixDate(String value) {
			if (value == null) {
				return null;
			}
			Date date;
			if (NumberUtils.isDigits(value)) {
				date = new Date(Long.valueOf(value) * 1000);
			} else {
				try {
					date = sdf.parse(value);
				} catch (ParseException e) {
					value = value.replaceFirst("([0-9]{2}):([0-9]{2})$", "$1$2");
					try {
						date = sdf.parse(value);
					} catch (ParseException e1) {
						System.err.println(e.getMessage());
						System.err.println(e1.getMessage());
						date = null;
					}
				}
			}
			// 2013-07-08T17:10:16+00:00
			return PDataType.DATE.toBytes(date);
		}

		private byte[] toPhoenixInteger(Integer value) {
			return PDataType.INTEGER.toBytes(value);
		}

	}
}