package org.apache.hadoop.hbase.mapreduce;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertArrayEquals;

public class ConvertersTest {
    private static long pageIdPart = 123456;
    private static long postIdPart = 987;
    private static final String STRING_ID = "" + postIdPart + "_" + pageIdPart;
    private static Multimap<String, Object> MAP_ID;

    private static byte[] ROWKEY;
    private static String POST_ID = "post_id";

    private static String PROFILE_ID = "profile_id";

    static {

        MAP_ID = ArrayListMultimap.create();
        MAP_ID.put(POST_ID, 123456);
        MAP_ID.put(PROFILE_ID, 987);

        CRC32 crc32 = new CRC32();
        byte[] pageIdPart = toBytes(STRING_ID.split("_")[1]);
        byte[] postIdPart = toBytes(STRING_ID.split("_")[0]);
        crc32.update(pageIdPart);
        short crc = (short) crc32.getValue();
        ByteBuffer bb = ByteBuffer.allocate(2 + STRING_ID.length());
        bb.putShort(crc);
        bb.put(pageIdPart);
        bb.put((byte) 0);
        bb.put(postIdPart);
        ROWKEY = bb.array();
    }


    @Test
    public void testRowKeygen() {
        assertArrayEquals(ROWKEY, Converters.idConverter.convert(toBytes(pageIdPart), toBytes(postIdPart), null));
    }

}