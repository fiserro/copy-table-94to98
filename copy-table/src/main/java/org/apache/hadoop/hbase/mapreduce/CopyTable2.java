package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.HConnection;
import org.apache98.hadoop.hbase.client.HConnectionManager;
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable2 extends Configured implements Tool {

	private static final byte[] SBKS_EA_IS_ADMIN_POST = toBytes("sbks_ea_is_admin_post");
	private static final byte[] FACEBOOK_IS_ADMIN_POST = toBytes("facebook.is_admin_post");

	private static final byte[] SBKS_EA_RESPONSE_TIME = toBytes("sbks_ea_response_time");
	private static final byte[] SBKS_RESPONDED_RESPONSE_TIME = toBytes("sbks.responded.response_time");

	private static final byte[] SBKS_EA_ADMIN_COMMENT_ID = toBytes("sbks_ea_admin_comment_id");
	private static final byte[] SBKS_RESPONDED_RESPONSE_ID = toBytes("sbks.responded.response_id");

	private static final byte[] PAGE_ID = toBytes("page_id");
	private static final byte[] PROFILE_ID = toBytes("profile_id");

	private static final byte[] USER_ID = toBytes("user_id");
	private static final byte[] AUTHOR_ID = toBytes("author_id");

	private static final byte[] ID = toBytes("id");
	private static final byte[] CREATED_TIME = toBytes("created_time");

	private static final byte[] D = toBytes("d");
	private static final byte[] M = toBytes("m");
	private static final byte[] I = toBytes("i");

	private static final Set<byte[]> SIMPLE_INTS = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	static {
		SIMPLE_INTS.add(toBytes("stories"));
		SIMPLE_INTS.add(toBytes("storytellers"));
		SIMPLE_INTS.add(toBytes("story_adds"));
		SIMPLE_INTS.add(toBytes("story_adds_unique"));
		SIMPLE_INTS.add(toBytes("impressions"));
		SIMPLE_INTS.add(toBytes("impressions_unique"));
		SIMPLE_INTS.add(toBytes("impressions_fan"));
		SIMPLE_INTS.add(toBytes("impressions_fan_unique"));
		SIMPLE_INTS.add(toBytes("impressions_fan_paid"));
		SIMPLE_INTS.add(toBytes("impressions_fan_paid_unique"));
		SIMPLE_INTS.add(toBytes("impressions_organic"));
		SIMPLE_INTS.add(toBytes("impressions_organic_unique"));
		SIMPLE_INTS.add(toBytes("impressions_paid"));
		SIMPLE_INTS.add(toBytes("impressions_paid_unique"));
		SIMPLE_INTS.add(toBytes("impressions_viral"));
		SIMPLE_INTS.add(toBytes("impressions_viral_unique"));
		SIMPLE_INTS.add(toBytes("consumptions"));
		SIMPLE_INTS.add(toBytes("consumptions_unique"));
		SIMPLE_INTS.add(toBytes("fan_reach"));
		SIMPLE_INTS.add(toBytes("engaged_fan"));
		SIMPLE_INTS.add(toBytes("engaged_users"));
		SIMPLE_INTS.add(toBytes("negative_feedback"));
		SIMPLE_INTS.add(toBytes("negative_feedback_unique"));
		SIMPLE_INTS.add(toBytes("video_length"));
		SIMPLE_INTS.add(toBytes("video_complete_views_organic"));
		SIMPLE_INTS.add(toBytes("video_complete_views_organic_unique"));
		SIMPLE_INTS.add(toBytes("video_complete_views_paid"));
		SIMPLE_INTS.add(toBytes("video_complete_views_paid_unique"));
		SIMPLE_INTS.add(toBytes("video_views"));
		SIMPLE_INTS.add(toBytes("video_views_unique"));
		SIMPLE_INTS.add(toBytes("video_views_autoplayed"));
		SIMPLE_INTS.add(toBytes("video_views_organic"));
		SIMPLE_INTS.add(toBytes("video_views_organic_unique"));
		SIMPLE_INTS.add(toBytes("video_views_paid"));
		SIMPLE_INTS.add(toBytes("video_views_paid_unique"));
		SIMPLE_INTS.add(toBytes("video_views_clicked_to_play"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s_paid"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s_organic"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s_unique"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s_autoplayed"));
		SIMPLE_INTS.add(toBytes("video_complete_views_30s_clicked_to_play"));
	}

	private static final Set<byte[]> NESTED_INTS = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	static {
		NESTED_INTS.add(toBytes("stories_by_action_type"));
		NESTED_INTS.add(toBytes("storytellers_by_action_type"));
		NESTED_INTS.add(toBytes("story_adds_by_action_type"));
		NESTED_INTS.add(toBytes("story_adds_by_action_type_unique"));
		NESTED_INTS.add(toBytes("impressions_by_paid_non_paid"));
		NESTED_INTS.add(toBytes("impressions_by_paid_non_paid_unique"));
		NESTED_INTS.add(toBytes("impressions_by_story_type"));
		NESTED_INTS.add(toBytes("impressions_by_story_type_unique"));
		NESTED_INTS.add(toBytes("consumptions_by_type"));
		NESTED_INTS.add(toBytes("consumptions_by_type_unique"));
		NESTED_INTS.add(toBytes("negative_feedback_by_type"));
		NESTED_INTS.add(toBytes("negative_feedback_by_type_unique"));
	}

	private static final Set<byte[]> SIMPLE_FLOATS = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	static {
		SIMPLE_FLOATS.add(toBytes("video_avg_time_watched"));
	}

	// DOUBLE
	private static final Set<byte[]> ARRAY_DOUBLES = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	static {
		ARRAY_DOUBLES.add(toBytes("video_retention_graph"));
		ARRAY_DOUBLES.add(toBytes("video_retention_graph_autoplayed"));
		ARRAY_DOUBLES.add(toBytes("video_retention_graph_clicked_to_play"));
	}

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
		scan.addColumn(D, ID);
		scan.addColumn(D, CREATED_TIME);
		scan.addColumn(D, USER_ID);
		scan.addColumn(D, PAGE_ID);
		scan.addColumn(D, SBKS_EA_ADMIN_COMMENT_ID);
		scan.addColumn(D, SBKS_EA_IS_ADMIN_POST);
		scan.addColumn(M, ID);
		scan.addColumn(M, CREATED_TIME);
		scan.addColumn(M, USER_ID);
		scan.addColumn(M, PAGE_ID);
		scan.addColumn(M, SBKS_EA_ADMIN_COMMENT_ID);
		scan.addColumn(M, SBKS_EA_IS_ADMIN_POST);
		scan.addFamily(I);

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

		private static final byte[] OBJECT_OBJECT = toBytes("[object Object]");
		private static final byte[] EMPTY_ARRAY = toBytes("[]");
		private static final byte TRUE_BYTE = toBytes("1")[0];
		private static final String DOT = ".";
		private static final byte[] DOT_BYTES = toBytes(DOT);

		private static final String SKIP_ATTR = "%skip_cpr%";

		private static final byte[] SKIP_ATTR_VAL = new byte[] { 1 };

		private org.apache98.hadoop.hbase.client.HTable table;

		int bucketSize = 30000;

		private List<Put> puts = new ArrayList<Put>();

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

		private Converter longC = new Converter() {
			@Override
			public byte[] convert(byte[] value, Context context) {
				try {
					return PDataType.LONG.toBytes(Integer.valueOf(Bytes.toString(value)));
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.long.err").increment(1);
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

		private Converter booleanC = new Converter() {
			@Override
			public byte[] convert(byte[] value, Context context) {
				try {
					return PDataType.BOOLEAN.toBytes(value != null && value.length > 0 && value[0] == TRUE_BYTE);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.boolean.err").increment(1);
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
		private Converter dateC = new Converter() {

			private final String[] formats = new String[] { "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss",
					"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" };

			@Override
			public byte[] convert(byte[] value, Context context) {

				try {
					String string = Bytes.toString(value);
					Date date;
					if (string.matches("[0-9]{1,11}")) {
						date = new Date(Long.valueOf(string) * 1000);
					} else if (string.matches("[0-9]{11,}")) {
						date = new Date(Long.valueOf(string));
					} else {
						date = null;
						if (string.matches(".*?[\\+\\- ]([0-9]{2}):([0-9]{2})$")) {
							string = string.replaceFirst("([0-9]{2}):([0-9]{2})$", "$1$2");
						}
						for (String format : formats) {
							SimpleDateFormat sdf = new SimpleDateFormat(format);
							sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
							try {
								date = sdf.parse(string);
								break;
							} catch (ParseException e1) {
							}
						}
					}
					if (date == null) {
						System.err.println("Can't parse date: '" + string + "'");
						context.getCounter("err", "converter.date.err").increment(1);
						return null;
					}
					return PDataType.DATE.toBytes(date);

				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter("err", "converter.date.err").increment(1);
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
			flush(context);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

			byte[] id = getValue(ID, result);
			if (id == null) {
				context.getCounter("err", "miss_id").increment(1);
				return;
			}

			byte[] createdTime = getValue(CREATED_TIME, result);
			if (createdTime == null) {
				context.getCounter("err", "miss_created_time").increment(1);
				return;
			}

			Put put = new Put(id);
			put.setAttribute(SKIP_ATTR, SKIP_ATTR_VAL);
			put(result, CREATED_TIME, put, D, CREATED_TIME, dateC, context);
			if (!put.has(D, CREATED_TIME)) {
				return;
			}
			put(result, USER_ID, put, D, AUTHOR_ID, null, context);
			put(result, PAGE_ID, put, D, PROFILE_ID, null, context);
			put(result, SBKS_EA_RESPONSE_TIME, put, D, SBKS_RESPONDED_RESPONSE_TIME, longC, context);
			put(result, SBKS_EA_ADMIN_COMMENT_ID, put, D, SBKS_RESPONDED_RESPONSE_ID, null, context);
			put(result, SBKS_EA_IS_ADMIN_POST, put, D, FACEBOOK_IS_ADMIN_POST, booleanC, context);

			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(I);
			if (familyMap != null) {
				for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
					byte[] value = entry.getValue();
					byte[] qualifier = entry.getKey();
					if (value == null || value.length == 0) {
						context.getCounter("insights", "miss_value").increment(1);
						continue;
					}
					if (Bytes.equals(value, OBJECT_OBJECT)) {
						context.getCounter("insights", "object_object").increment(1);
						continue;
					}
					if (Bytes.equals(value, EMPTY_ARRAY)) {
						context.getCounter("insights", "empty_array").increment(1);
						continue;
					}

					if (SIMPLE_FLOATS.contains(qualifier)) {
						put(put, I, qualifier, value, floatC, context);
					} else if (SIMPLE_INTS.contains(qualifier)) {
						put(put, I, qualifier, value, integerC, context);
					} else if (ARRAY_DOUBLES.contains(qualifier)) {
						put(put, I, qualifier, value, doubleArrayC, context);
					} else if (NESTED_INTS.contains(qualifier)) {
						try {
							Object object = mapper.readValue(value, Object.class);
							if (object instanceof List) {
								System.err.println(object);
								context.getCounter("insights", "list").increment(1);
								continue;
							}
							for (Entry<String, Object> entry2 : ((Map<String, Object>) object).entrySet()) {
								if (entry2.getValue() == null) {
									continue;
								}
								// String qualifierS = getQString(qualifier, entry2.getKey());
								String string = entry2.getValue().toString();
								Integer integer;
								try {
									integer = Integer.valueOf(string);
								} catch (Exception e) {
									context.getCounter("insights", "-not-a-number").increment(1);
									System.out.println(string);
									continue;
								}
								put(put, D, getQBytes(qualifier, entry2.getKey()), PDataType.INTEGER.toBytes(integer), context);
							}
						} catch (Exception e) {
							e.printStackTrace();
							context.getCounter("insights", "exception").increment(1);
						}
					}
				}
			}
			puts.add(put);
			if (puts.size() >= bucketSize) {
				flush(context);
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
			table = (org.apache98.hadoop.hbase.client.HTable) connection.getTable(conf.get(NEW_TABLE_NAME));
			table.setAutoFlushTo(false);
		}

		private void flush(Context context) throws IOException {
			int putSize = puts.size();
			if (putSize > 0) {
				HTableUtil.bucketRsPut(table, puts);
				context.getCounter("hbase98", "flush").increment(1);
				context.getCounter("hbase98", "put").increment(putSize);
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

		private String getQString(byte[] qualifier, String sub) {
			return getQPair(qualifier, sub).getSecond();
		}

		private byte[] getValue(byte[] qualifier, Result result) {
			byte[] authorId = result.getValue(D, qualifier);
			if (authorId == null) {
				authorId = result.getValue(M, qualifier);
			}
			return authorId;
		}

		private void put(Put target, byte[] family, byte[] qualifier, byte[] value, Context context) {
			if (value != null && value.length > 0) {
				target.add(family, qualifier, value);
				// context.getCounter("hbasee98", "put" + Bytes.toString(family) + ":" +
				// Bytes.toString(qualifier)).increment(1);
			}
		}

		private void put(Put target, byte[] family, byte[] qualifier, byte[] value, Converter converter, Context context) {
			if (value != null && value.length > 0) {
				if (converter != null) {
					value = converter.convert(value, context);
				}
				put(target, family, qualifier, value, context);
			}
		}

		private void put(Result source, byte[] qualifierS, Put target, byte[] familyT, byte[] qualifierT, Converter converter,
				Context context) {
			byte[] value = getValue(qualifierS, source);
			put(target, familyT, qualifierT, value, converter, context);
		}

		private interface Converter {
			byte[] convert(byte[] value, Context context);
		}

	}
}