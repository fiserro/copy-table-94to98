package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

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

	public static void main(String[] args) {

		byte[] startRow0 = toBytes("008051edf70e8d5f45c958d5c8fbdd6b");
		byte[] endRow0 = toBytes("ff0139aa5d7f36b22dc2ec3e92c25b08");
		byte[][] split = Bytes.split(startRow0, endRow0, 10);
		for (byte[] bs : split) {
			System.out.println(toStringBinary(bs));
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {

		Configuration conf = context.getConfiguration();
		int regionSplitCount = conf.getInt(REGION_SPLIT, 0);
		List<InputSplit> superSplits = super.getSplits(context);
		if (regionSplitCount == 0) {
			return superSplits;
		} else if (regionSplitCount > 0) {
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
		} else {
			regionSplitCount = regionSplitCount * (-1); // merge count
			List<InputSplit> splits = new ArrayList<InputSplit>();
			double desireSplitCount = superSplits.size() / (double) regionSplitCount;
			desireSplitCount = Math.round(desireSplitCount);

			if (desireSplitCount <= 1) {
				TableSplit tableSplit = (TableSplit) superSplits.get(0);
				TableSplit newSplit = new TableSplit(tableSplit.getTableName(), new byte[0], new byte[0], tableSplit.getLocations()[0]);
				splits.add(newSplit);
				return splits;
			}
			double inc = superSplits.size() / desireSplitCount;

			for (double i = 0; i < superSplits.size(); i = i + inc) {
				TableSplit tableSplit0 = (TableSplit) superSplits.get((int) i);
				byte[] tableName = tableSplit0.getTableName();
				byte[] startRow = tableSplit0.getStartRow();
				String location = tableSplit0.getLocations()[0];

				byte[] endRow;
				if (((int) i + inc + (inc / 10)) >= superSplits.size()) {
					endRow = new byte[0];
					i = superSplits.size() + 1; // break;
				} else {
					endRow = ((TableSplit) superSplits.get((int) (i + inc))).getStartRow();
				}

				TableSplit newSplit = new TableSplit(tableName, startRow, endRow, location);
				splits.add(newSplit);
			}

			System.out.println("mergins splits from " + superSplits.size() + " to " + splits.size());
			for (InputSplit inputSplit : splits) {
				TableSplit tableSplit0 = (TableSplit) inputSplit;
				System.out.println(toStringBinary(tableSplit0.getStartRow()) + " ---> " + toStringBinary(tableSplit0.getEndRow()));
			}

			return splits;
		}
	}
}
