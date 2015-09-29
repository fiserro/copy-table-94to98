package org.apache.hadoop.hbase.mapreduce;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class KeyBuilderTweetByIdV2 {

	private static final int BIG_SHIFT = 42;
	private static final int SMALL_SHIFT = Long.SIZE - BIG_SHIFT;
	private static final long EPOCH = 1288834974657L;

	private static final long workerIdBits = 5L;
	private static final long datacenterIdBits = 5L;
	private static final long sequenceBits = 12L;
	private static final long workerIdShift = sequenceBits;
	private static final long datacenterIdShift = sequenceBits + workerIdBits;
	private static final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;

	private Set<Long> generatedIds = Collections.synchronizedSet(new HashSet<Long>());

	public KeyBuilderTweetByIdV2() {
	}

	public long generateId(long datacenterId, long workerId, long timestamp) {
		long id = ((timestamp - EPOCH) << timestampLeftShift) |
				(datacenterId << datacenterIdShift) |
				(workerId << workerIdShift);
		if (generatedIds.contains(id)) {
			return generateId(datacenterId, workerId, timestamp + 1);
		} else {
			generatedIds.add(id);
			return id;
		}
	}

	public byte[] toBytes(Integer id) {
		return toBytes(Long.valueOf(id));
	}

	public byte[] toBytes(Long id) {
		return Bytes.toBytes(rotate(id, BIG_SHIFT));
	}

	public byte[] toBytes(Object id) {
		if (id instanceof Long) {
			return toBytes((Long) id);
		} else if (id instanceof Integer) {
			return toBytes((Integer) id);
		} else if (id instanceof String && isNotBlank((String) id)) {
			return toBytes((String) id);
		}
		throw new IllegalArgumentException("Invalid key value:" + id);
	}

	public byte[] toBytes(String id) {
		return toBytes(Long.valueOf(id));
	}

	public long toObject(byte[] rowKey) {
		return rotate(Bytes.toLong(rowKey), SMALL_SHIFT);
	}

	public long toTimeStamp(long id) {
		return (id >> SMALL_SHIFT) + EPOCH;
	}

	private boolean isNotBlank(String id) {
		if (id == null) {
			return false;
		}
		if (id.isEmpty()) {
			return false;
		}
		return true;
	}

	private long rotate(long bits, int shift) {
		return (bits >>> shift) | (bits << (Long.SIZE - shift));
	}
}