package org.apache.hadoop.hbase.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Convertors {

    private static ObjectMapper mapper = new ObjectMapper();

    public interface Converter {
        byte[] convert(byte[] value, Mapper.Context context);
    }

    public static Converter integerC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            try {
                return PDataType.INTEGER.toBytes(Integer.valueOf(Bytes.toString(value)));
            } catch (Exception e) {
                e.printStackTrace();
                context.getCounter("err", "converter.integer.err").increment(1);
                return null;
            }
        }
    };

    public static Converter longC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            try {
                return PDataType.LONG.toBytes(Integer.valueOf(Bytes.toString(value)));
            } catch (Exception e) {
                e.printStackTrace();
                context.getCounter("err", "converter.long.err").increment(1);
                return null;
            }
        }
    };

    public static Converter doubleC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            try {
                return PDataType.DOUBLE.toBytes(Double.valueOf(Bytes.toString(value)));
            } catch (Exception e) {
                e.printStackTrace();
                context.getCounter("err", "converter.double.err").increment(1);
                return null;
            }
        }
    };

    public static Converter doubleArrayC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
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
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) mapper.readValue(string, Map.class)).entrySet()) {
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

    public static Converter dateC = new Converter() {

        private final String[] formats = new String[] { "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" };

        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {

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
}
