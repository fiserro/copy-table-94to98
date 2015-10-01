package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class RegionSplitTableInputFormat extends TableInputFormat {

	public static final String REGION_SPLIT = "region.split";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		int regionSplitCount = conf.getInt(SplitUtil.REGION_SPLIT, 0);
		List<InputSplit> superSplits = super.getSplits(context);
		return SplitUtil.splitTable(superSplits, regionSplitCount);
	}
}
