package org.apache.hadoop.hbase.mapreduce;

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
import org.apache98.hadoop.hbase.HBaseConfiguration;
import org.apache98.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class InstagramPostsCopyTable extends Configured implements Tool {

    private static final byte[] D = toBytes("d");

    private static final byte[] CORRUPTED_FIELDS = toBytes("fields");

    private static final byte[] PROFILE_ID = toBytes("profile_id");
    private static final byte[] PROFILE_ID_PREV = toBytes("page_id");

    private static final byte[] CREATED_TIME = toBytes("created_time");
    private static final byte[] MESSAGE = toBytes("message");
    private static final byte[] COMMENT_COUNT = toBytes("comment_count");
    private static final byte[] LIKE_COUNT = toBytes("like_count");
    private static final byte[] LINK = toBytes("link");
    private static final byte[] TYPE = toBytes("sbks.type");
    private static final byte[] TYPE_PREV = toBytes("type");
    private static final byte[] PROFILES_FANS_COUNT = toBytes("sbks.profile_fans_count");
    private static final byte[] PROFILES_FANS_COUNT_PREV = toBytes("page_fans");
    private static final byte[] RATING = toBytes("sbks.er");
    private static final byte[] RATING_PREV = toBytes("sbks_ea");
    private static final byte[] SBKS_DOWNLOAD = toBytes("sbks.download_time");
    private static final byte[] SBKS_DOWNLOAD_PREV = toBytes("sbks_download_tms");
    private static final byte[] POST_ID = toBytes("instagram.post_id");
    private static final byte[] POST_ID_PREV = toBytes("post_id");
    private static final byte[] FILTER = toBytes("instagram.filter");
    private static final byte[] FILTER_PREV = toBytes("filter");
    private static final byte[] CAPTION_ID = toBytes("instagram.caption_id");
    private static final byte[] CAPTION_ID_PREV = toBytes("caption_id");
    private static final byte[] CAPTION_USER_ID = toBytes("instagram.caption_user_id");
    private static final byte[] CAPTION_USER_ID_PREV = toBytes("caption_user_id");

    private static final byte[] USERS_IN_PHOTO_PREV = toBytes("users_in_photo");
    private static final byte[] HASHTAGS_PREV = toBytes("tags");
    private static final byte[] ENTITIES = toBytes("entities");


    private static final byte[] LOCATION_PREV = toBytes("location");

    private static final byte[] LOCATION_ID = toBytes("location.id");
    private static final byte[] LOCATION_NAME = toBytes("location.name");
    private static final byte[] LOCATION_COORDINATES = toBytes("location.coordinates");

    private static final byte[] IMAGE_LOW = toBytes("content_low");
    private static final byte[] IMAGE_STANDARD = toBytes("content_standard");
    private static final byte[] IMAGE_THUMBNAIL = toBytes("content_thumbnail");

    private static final byte[] IMAGES_ATTACHMENT = toBytes("attachments");
//    private static final byte[] VIDEOS = toBytes("videos");

    /**
     * Main entry point.
     *
     * @param args
     *            The command line parameters.
     * @throws Exception
     *             When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InstagramPostsCopyTable(), args));
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
        scan.addFamily(D);

        Job job = new Job(conf, jobName);
        job.setJarByClass(InstagramPostsCopyTable.class);

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

        return job;
    }


    private static class Mapper94_98 extends TableMapper<ImmutableBytesWritable, KeyValue> {

        private List<org.apache98.hadoop.hbase.client.Put> puts = new ArrayList<org.apache98.hadoop.hbase.client.Put>();
        private List<org.apache98.hadoop.hbase.client.Put> corruptedDataPuts = new ArrayList<org.apache98.hadoop.hbase.client.Put>();
        private org.apache98.hadoop.hbase.client.HTable table;
        private org.apache98.hadoop.hbase.client.HTable corruptedDataTable;
        private final String corruptedTableName = "ig_corrupted_posts";
        private int bucketSize;

        public static void main(String[] args) throws IOException, InterruptedException {

            Mapper94_98 mapper94_98 = new Mapper94_98();
            Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
            conf.set(HBASE_ZOOKEEPER_QUORUM, "zookeeper1.us-w2.aws.ccl");
            conf.set(HBASE_ZOOKEEPER_QUORUM2, "c-sencha-s01.us-w2.aws.ccl");
            conf.set(NEW_TABLE_NAME, "ig_posts");

            Scan scan = new Scan(Bytes.toBytesBinary("\\x00\\x01-_\\x00\\x00\\x00\\x00\\x0B-\\x01\\x00_\\x7F\\xFF\\xFE\\xAF\\xE5\\xA1>w_\\x00\\x00\\x00\\x00"));
//            Scan scan = new Scan(Bytes.toBytesBinary("\\x00\\x01-_\\x00\\x00\\x00\\x00\\x0B-\\x01\\x00_\\x7F\\xFF\\xFE\\xAF\\xD0z\\x0Dg_\\x00\\x00\\x00\\x00"));
//            Scan scan = new Scan();
            scan.setCaching(10);
            HTable htable = new HTable(conf, "instagram_posts");
            ResultScanner scanner = htable.getScanner(scan);
            int i = 0;

            FakeContext context = mapper94_98.createFakeContext(conf);
            mapper94_98.setup(context);
            try {
                mapper94_98.scan();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            for (Result result : scanner) {
                if (++i > 1) {
                    break;
                }
                byte[] rowId = Converters.idConverter.convert(
                        result.getValue(D, PROFILE_ID_PREV),
                        result.getValue(D, POST_ID_PREV),
                        context
                );
                org.apache98.hadoop.hbase.client.Result r = mapper94_98.getByRowId(rowId);
//                if (r.isEmpty() && Bytes.toString(result.getValue(D, LOCATION_PREV)).length() > 5) {
//                    System.out.println(Bytes.toStringBinary(result.getRow()));
//                    System.out.println("" + Bytes.toLong(result.getValue(D, POST_ID_PREV)) + "_" + Bytes.toLong(result.getValue(D, PROFILE_ID_PREV)));
//                }
//                if (!r.isEmpty()) {
//                    mapper94_98.delete(rowId);
//                }
                System.out.println("" + Bytes.toLong(result.getValue(D, POST_ID_PREV)) + "_" + Bytes.toLong(result.getValue(D, PROFILE_ID_PREV)));
                mapper94_98.map(new ImmutableBytesWritable(result.getRow()), result, context);
            }
            mapper94_98.flush(context);
            mapper94_98.corruptedFlush(context);
            mapper94_98.cleanup(context);
            for (Counter counter : context.getCounters().values()) {
                System.out.println(counter.getDisplayName() + ":" + counter.getValue());
            }
            htable.close();
        }

        private void delete(byte[] rowId) {
            Delete d = new Delete(rowId);
            try {
                table.delete(d);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            try {
                ValuesMapper mapper = new ValuesMapper(context, result);
                Put put = mapper.mapToNewStructurePut();
                puts.add(put);
                if (puts.size() >= bucketSize) {
                    flush(context);
                }
                if (corruptedDataPuts.size() >= 1000) {
                    corruptedFlush(context);
                }
            } catch (ValueNotFound e) {
                context.getCounter("err", "miss_" + e.getField()).increment(1);
            }
        }

        private org.apache98.hadoop.conf.Configuration conf98;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            bucketSize = conf.getInt(BUCKET_SIZE, 4000);
            conf98 = HBaseConfiguration.create();
            // conf98.set("hbase.client.write.buffer", "20971520");
            conf98.set(HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM2));
            HConnection connection = HConnectionManager.createConnection(conf98);
            table = (org.apache98.hadoop.hbase.client.HTable) connection.getTable(conf.get(NEW_TABLE_NAME));
            table.setAutoFlushTo(false);
            corruptedDataTable = (org.apache98.hadoop.hbase.client.HTable) connection.getTable(corruptedTableName);
            corruptedDataTable.setAutoFlushTo(false);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            flush(context);
            corruptedFlush(context);
            context.getCounter("hbase98", "flush_cleanup").increment(1);
            super.cleanup(context);
        }


        public org.apache98.hadoop.hbase.client.Result getByRowId(byte[] rowId) {
            Get get = new Get(rowId);
            try {
                return table.get(get);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        public void scan() throws IOException, Throwable {
            org.apache98.hadoop.hbase.client.Scan scan = new org.apache98.hadoop.hbase.client.Scan();
            scan.setCaching(10);
            scan.addFamily(D);
            org.apache98.hadoop.hbase.client.ResultScanner scanner = corruptedDataTable.getScanner(scan);
//            AggregationClient ag = new AggregationClient(conf98);
//            long count = ag.rowCount(corruptedDataTable, null, scan);
            int i = 0;

            for (org.apache98.hadoop.hbase.client.Result result : scanner) {
                if (++i > 1) {
                    break;
                }
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

        public void flush(Context context) throws IOException {
            int putSize = puts.size();
            if (putSize > 0) {
//                HTableUtil.bucketRsPut(table, puts);
                context.getCounter("hbase98", "flush").increment(1);
                context.getCounter("hbase98", "put").increment(putSize);
                puts.clear();
            }
        }

        public void corruptedFlush(Context context) throws IOException {
            List<Put> puts = corruptedDataPuts;
            int putSize = puts.size();
            if (putSize > 0) {
                HTableUtil.bucketRsPut(corruptedDataTable, puts);
                context.getCounter("hbase98", "currpted_flush").increment(1);
                context.getCounter("hbase98", "currpted_put").increment(putSize);
                puts.clear();
            }
        }

        private class ValuesMapper {
            private final Context context;
            private final Result result;

            public ValuesMapper(Context context, Result result) {
                this.context = context;
                this.result = result;
            }

            public Put mapToNewStructurePut() throws ValueNotFound {
                final byte[] profileId = getValue(PROFILE_ID_PREV, true);
                final byte[] postId = getValue(POST_ID_PREV, true);
                final byte[] id = Converters.idConverter.convert(profileId, postId, context);

                if (id == null)
                    throw new ValueNotFound("row_id");

                String postIdAsString = "" + Bytes.toLong(postId) + "_" + Bytes.toLong(profileId);

                Put put = new Put(id);
                byte[] corruptedFields = new byte[0];

                putAndTrack(put, PROFILE_ID, convert(profileId, Converters.longC));
                putAndTrack(put, POST_ID, convert(postId, Converters.longC));
                putAndTrack(put, CREATED_TIME, convert(getValue(CREATED_TIME), Converters.dateC));
                putAndTrack(put, MESSAGE, getValue(MESSAGE));
                putAndTrack(put, COMMENT_COUNT, convert(getValue(COMMENT_COUNT), Converters.integerC));
                putAndTrack(put, LIKE_COUNT, convert(getValue(LIKE_COUNT), Converters.integerC));
                putAndTrack(put, LINK, getValue(LINK));
                putAndTrack(put, TYPE, getValue(TYPE_PREV));
                putAndTrack(put, PROFILES_FANS_COUNT, convert(getValue(PROFILES_FANS_COUNT_PREV), Converters.integerC));
                putAndTrack(put, SBKS_DOWNLOAD, convert(getValue(SBKS_DOWNLOAD_PREV), Converters.dateC));
                putAndTrack(put, FILTER, getValue(FILTER_PREV));
                putAndTrack(put, CAPTION_ID, convert(getValue(CAPTION_ID_PREV), Converters.longToStringC));
                putAndTrack(put, CAPTION_USER_ID, convert(getValue(CAPTION_USER_ID_PREV), Converters.longToStringC));

                try {
                    putAndTrack(put, RATING, convert(getValue(RATING_PREV), Converters.ratingConverter));
                } catch (Converters.ConverterException e) {
                    byte[] field = Bytes.add(RATING_PREV, Bytes.toBytes("."), e.fieldName);
                    corruptedFields = corruptedFields.length > 0 ? Bytes.add(corruptedFields, Bytes.toBytes(","), field) : field;
                }
                try {
                    putAndTrack(put, ENTITIES, Converters.entitiesConverter.convert(getValue(USERS_IN_PHOTO_PREV),
                            getValue(HASHTAGS_PREV), postIdAsString, context));
                } catch (Converters.ConverterException e) {
                    byte[] field = Bytes.add(ENTITIES, Bytes.toBytes("."), e.fieldName);
                    corruptedFields = corruptedFields.length > 0 ? Bytes.add(corruptedFields, Bytes.toBytes(","), field) : field;
                }

                Converters.LocationConverter.Location location = new Converters.LocationConverter.Location();
                try {
                    location = Converters.locationConverter.convert(getValue(LOCATION_PREV), postIdAsString, context);
                } catch (Converters.ConverterException e) {
                    corruptedFields = corruptedFields.length > 0 ? Bytes.add(corruptedFields, Bytes.toBytes(","), LOCATION_PREV) : LOCATION_PREV;
                }

                putAndTrack(put, LOCATION_ID, location.id);
                putAndTrack(put, LOCATION_NAME, location.name);
                putAndTrack(put, LOCATION_COORDINATES, location.coordinates);

                try {
                    putAndTrack(put, IMAGES_ATTACHMENT, Converters.attachmentsWithImageConverter.convert(getValue(IMAGE_LOW),
                            getValue(IMAGE_STANDARD), getValue(IMAGE_THUMBNAIL), context));
                } catch (Converters.ConverterException e) {
                    byte[] field = Bytes.add(IMAGES_ATTACHMENT, Bytes.toBytes("."), e.fieldName);
                    corruptedFields = corruptedFields.length > 0 ? Bytes.add(corruptedFields, Bytes.toBytes(","), field) : field;
                }

                if (corruptedFields.length > 0) {
                    Put corrupted = new Put(id);
                    corrupted.add(D, POST_ID, postId);
                    corrupted.add(D, PROFILE_ID, profileId);
                    corrupted.add(D, CORRUPTED_FIELDS, corruptedFields);
                    corrupted.add(D, CREATED_TIME, getValue(CREATED_TIME));
                    corruptedDataPuts.add(corrupted);
                }

                return put;
            }

            private void putAndTrack(Put put, byte[] qualifier, byte[] value) {
                if (value != null && value.length > 0)
                    put.add(D, qualifier, value);
                else
                    context.getCounter("err", "empty_field_" + Bytes.toString(qualifier)).increment(1);
            }

            public byte[] getValue(byte[] qualifier, boolean isRequired) throws ValueNotFound {
                byte[] data = result.getValue(D, qualifier);
                if (data == null && isRequired)
                    throw new ValueNotFound(Bytes.toString(qualifier));
                return data;
            }

            private byte[] getValue(byte[] qualifier) throws ValueNotFound {
                return getValue(qualifier, false);
            }

            private byte[] convert(byte[] value, Converters.Converter converter) {
                if (value != null && value.length > 0)
                    return converter.convert(value, context);
                return value;
            }

        }

        class ValueNotFound extends Exception {
            private String field;
            public ValueNotFound(String field) {
                this.field = field;
            }
            public String getField() {
                return field;
            }

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
