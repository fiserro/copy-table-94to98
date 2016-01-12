package org.apache.hadoop.hbase.mapreduce;

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
import org.apache98.hadoop.hbase.client.HTableUtil;
import org.apache98.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class InaragramPostsCopyTable extends Configured implements Tool {

    private static final byte[] D = toBytes("d");

    private static final byte[] ID = toBytes("page_id");

    private static final byte[] PROFILE_ID = toBytes("profile_id");
    private static final byte[] PREV_PROFILE_ID = toBytes("page_id");

    private static final byte[] CREATE_TIME = toBytes("created_time");

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

        return job;
    }

    private static class Mapper94_98 extends TableMapper<ImmutableBytesWritable, KeyValue> {

        private List<org.apache98.hadoop.hbase.client.Put> puts = new ArrayList<org.apache98.hadoop.hbase.client.Put>();
        private org.apache98.hadoop.hbase.client.HTable table;

        @SuppressWarnings("unchecked")
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            try {
                Put put = mapToNewStructurePut(result);
                puts.add(put);
                if (puts.size() >= bucketSize) {
                    flush(context);
                }
            } catch (ValueNotFound e) {
                context.getCounter("err", "miss_" + e.getField()).increment(1);
            }
        }

        private Put mapToNewStructurePut(Result result) throws ValueNotFound {
            byte[] id = getValue(ID, result);
            Put put = new Put(id);
            put(put, PROFILE_ID, getValue(PREV_PROFILE_ID, result));
            put(put, CREATE_TIME, getValue(CREATE_TIME, result));
            return put;
        }

        private void put(Put put, byte[] qualifier, byte[] value) {
            if (value != null && value.length > 0)
                put.add(D, qualifier, value);
        }

        private byte[] getValue(byte[] qualifier, Result result, boolean isRequired) throws ValueNotFound {
            byte[] data = result.getValue(D, qualifier);
            if (data == null && isRequired)
                throw new ValueNotFound(qualifier.toString());
            return data;
        }

        private byte[] getValue(byte[] qualifier, Result result) throws ValueNotFound {
            return getValue(qualifier, result, true);
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


        class ValueNotFound extends Exception {
            private String field;
            public ValueNotFound(String field) {
                this.field = field;
            }
            public String getField() {
                return field;
            }
        }
    }
}
