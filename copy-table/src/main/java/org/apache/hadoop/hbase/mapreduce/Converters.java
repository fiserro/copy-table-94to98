package org.apache.hadoop.hbase.mapreduce;

import com.socialbakers.proto.SocialContents.SocialContent.Attachments.Attachment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.CRC32;

public class Converters {

    private static ObjectMapper mapper = new ObjectMapper();

    public interface Converter {
        byte[] convert(byte[] value, Mapper.Context context);
    }

    public interface AttachmentConverter {
        byte[] convert(byte[] contentLow, byte[] contentStandard, byte[] contentThumbnail, Mapper.Context context);
    }

    public interface BiConverter {
        byte[] convert(byte[] first, byte[] second, Mapper.Context context);
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

    public static AttachmentConverter imageConverter = new AttachmentConverter() {
        @Override
        public byte[] convert(byte[] contentLow, byte[] contentStandard, byte[] contentThumbnail, Mapper.Context context) {
            Attachment.Image imageLow = createImage(contentLow, "low", context);
            Attachment.Image imageStandard = createImage(contentStandard, "standard", context);
            Attachment.Image imageThumbnail = createImage(contentLow, "thumbnail", context);

            if (imageLow == null)
                return null;

            Attachment.Builder builder = Attachment.newBuilder()
                    .setImage(imageLow)
                    .addImages(imageLow);

            if (imageStandard != null)
                builder.addImages(imageStandard);

            if (imageThumbnail != null)
                builder.addImages(imageThumbnail);

            return builder.build().toByteArray();
        }

        private Attachment.Image createImage(byte[] json, String type, Mapper.Context context) {
            if (json == null || json.length == 0) {
                context.getCounter("err", "converter.image." + type + ".missing").increment(1);
                return null;
            }
            try {
                Map<String, Object> data = jsonToMap(json);
                Attachment.Image.Builder builder = Attachment.Image.newBuilder();
                builder.setType(type);

                String url = getValueByKey(data, "url", type, context);
                if (url == null) {
                    return null;
                }
                builder.setUrl(url);

                String width = getValueByKey(data, "width", type, context);
                if (width != null)
                    builder.setWidth(Integer.parseInt(width));

                String height = getValueByKey(data, "height", type, context);
                if (height != null)
                    builder.setHeight(Integer.parseInt(height));

                return builder.build();

            } catch (IOException e) {
                e.printStackTrace();
                context.getCounter("err", "converter.image." + type).increment(1);
                return null;
            }
        }

        private Map<String, Object> jsonToMap(byte[] json) throws IOException {
            return (Map<String, Object>) mapper.readValue(Bytes.toString(json), Map.class);
        }

        private String getValueByKey(Map<String, Object> map, String k, String imageName, Mapper.Context context) {
            if (!map.containsKey(k)) {
                context.getCounter("err", "converter.image." + imageName + "." + k + ".missing").increment(1);
                return null;
            }

            Object value = map.get(k);
            if (value == null) {
                context.getCounter("err", "converter.image." + imageName + "." + k + ".is_null.").increment(1);
                return null;
            }
            return value.toString();
        }
    };

    public static BiConverter idConverter = new BiConverter(){

        private static final int RK_LEN = 18;
        private static final int PROFILE_ID_OFFSET = 2;
        private static final int POST_ID_OFFSET = 10;

        private CRC32 crc32 = new CRC32();

        @Override
        public byte[] convert(byte[] profileId, byte[] postId, Mapper.Context context) {
            try {
                Long profileIdL = Bytes.toLong(profileId);
                Long postIdL = Bytes.toLong(postId);

                byte[] bytes = new byte[RK_LEN];

                ByteBuffer bb = ByteBuffer.wrap(bytes);
                bb.putLong(PROFILE_ID_OFFSET, profileIdL);
                bb.putLong(POST_ID_OFFSET, postIdL);
                bb.position(0);
                bb.get(bytes);

                synchronized (crc32) {
                    crc32.update(bytes, PROFILE_ID_OFFSET, Bytes.SIZEOF_LONG);
                    short crc = (short) crc32.getValue();
                    crc32.reset();
                    bb.position(0);
                    bb.putShort(0, crc);
                }
                bb.position(0);
                bb.get(bytes);
                return bytes;
            } catch (Exception e) {
                e.printStackTrace();
                context.getCounter("err", "converter.instagram_post.id.create").increment(1);
                return null;
            }
        }
    };
}
