package org.apache.hadoop.hbase.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.annotations.VisibleForTesting;

public class SplitUtil {

	private static final byte _0 = (byte) 0x00;
	private static final byte _255 = (byte) 0xFF;
	private static final byte[] EMPTY_BYTES = new byte[0];

	public static final String REGION_SPLIT = "region.split";

	public static List<InputSplit> splitTable(List<InputSplit> superSplits, int splitCount) {

		if (splitCount <= 0) {
			return superSplits;
		}

		List<InputSplit> splits = new ArrayList<InputSplit>();

		for (InputSplit inputSplit : superSplits) {
			TableSplit tableSplit = (TableSplit) inputSplit;
			// System.out.println("splitting by " + splitCount + " " + tableSplit);
			byte[][] split = splitTableSplit(splitCount, tableSplit.getStartRow(), tableSplit.getEndRow());
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

	@VisibleForTesting
	static byte[][] splitTableSplit(int splitCount, byte[] startRow, byte[] endRow) {
		if (startRow == null) {
			startRow = EMPTY_BYTES;
		}
		if (endRow == null) {
			endRow = EMPTY_BYTES;
		}

		boolean discardLastSplit = false;
		boolean discardFirstSplit = false;
		if (startRow.length == 0 && endRow.length == 0) {
			startRow = new byte[] { _0 };
			endRow = new byte[] { _255 };
			discardLastSplit = true;
			discardFirstSplit = true;
		}

		if (endRow.length == 0) {
			endRow = new byte[startRow.length];
			Arrays.fill(endRow, _255);
			discardLastSplit = true;
		}
		byte[][] split = Bytes.split(startRow, endRow, splitCount);
		if (split == null) {
			startRow = addByte(startRow, _0);
			endRow = addByte(endRow, _0);
			return splitTableSplit(splitCount, startRow, endRow);
		}
		if (discardLastSplit) {
			split[split.length - 1] = EMPTY_BYTES;
		}
		if (discardFirstSplit) {
			split[0] = EMPTY_BYTES;
		}
		return split;
	}

	private static byte[] addByte(byte[] array, byte byteToAdd) {
		byte[] newArray = new byte[array.length + 1];
		System.arraycopy(array, 0, newArray, 0, array.length);
		newArray[array.length] = byteToAdd;
		return newArray;
	}

}
