package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

import static org.apache.hadoop.hbase.mapreduce.Statics.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class InaragramPostsCopyTable extends Configured implements Tool {

    private static final byte[] D = toBytes("d");
    private static final byte[] M = toBytes("m");
    private static final byte[] I = toBytes("i");

    private static final byte[] ID = toBytes("id_orig");
    private static final byte[] PAGE_ID = toBytes("page_id");
    private static final byte[] CREATED_TIME = toBytes("created_time");

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
        scan.addColumn(D, ID);
        scan.addColumn(D, CREATED_TIME);
        scan.addColumn(D, PAGE_ID);
        scan.addColumn(M, ID);
        scan.addColumn(M, CREATED_TIME);
        scan.addColumn(M, PAGE_ID);
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

        return job;
    }

    private static class Mapper94_98 extends TableMapper<ImmutableBytesWritable, KeyValue> {

    }
}
