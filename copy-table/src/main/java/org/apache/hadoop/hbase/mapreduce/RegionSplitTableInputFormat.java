package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class RegionSplitTableInputFormat extends TableInputFormat {

	public static final String REGION_SPLIT = "region.split";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {

		Configuration conf = context.getConfiguration();
		int regionSplitCount = conf.getInt(REGION_SPLIT, 0);
		List<InputSplit> superSplits = super.getSplits(context);
		if (regionSplitCount <= 0) {
			return superSplits;
		}

		List<InputSplit> splits = new ArrayList<InputSplit>(superSplits.size() * regionSplitCount);

		for (InputSplit inputSplit : superSplits) {
			TableSplit tableSplit = (TableSplit) inputSplit;
			System.out.println("splitting by " + regionSplitCount + " " + tableSplit);
			byte[] startRow0 = tableSplit.getStartRow();
			byte[] endRow0 = tableSplit.getEndRow();
			boolean discardLastSplit = false;
			if (endRow0.length == 0) {
				endRow0 = new byte[startRow0.length];
				Arrays.fill(endRow0, (byte) 255);
				discardLastSplit = true;
			}
			byte[][] split = Bytes.split(startRow0, endRow0, regionSplitCount);
			if (discardLastSplit) {
				split[split.length - 1] = new byte[0];
			}
			for (int regionSplit = 0; regionSplit < split.length - 1; regionSplit++) {
				byte[] startRow = split[regionSplit];
				byte[] endRow = split[regionSplit + 1];
				TableSplit newSplit = new TableSplit(tableSplit.getTableName(), startRow, endRow,
						tableSplit.getLocations()[0]);
				splits.add(newSplit);
			}
		}

		return splits;
	}
}
