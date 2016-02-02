package org.apache.hadoop.hbase.mapreduce;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.socialbakers.proto.SocialContents.SocialContent.Attachments;
import com.socialbakers.proto.SocialContents.SocialContent.Attachments.Attachment;
import com.socialbakers.proto.SocialContents.SocialContent.Entities;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

public class Converters {

    private static ObjectMapper mapper = new ObjectMapper();


    public interface Converter {
        byte[] convert(byte[] value, Mapper.Context context);

    }

    public interface AttachmentConverter {
        byte[] convert(byte[] contentLow, byte[] contentStandard, byte[] contentThumbnail, Mapper.Context context);
    }

    public interface EntitiesConverter {
        byte[] convert(byte[] usersInPhoto, byte[] tags, Mapper.Context context);
    }

    public interface LocationConverter {
        class Location {
            public byte[] coordinates;
            public byte[] name;
            public byte[] id;
        }

        Location convert(byte[] value, Mapper.Context context);
    }

    public interface BiConverter {
        byte[] convert(byte[] first, byte[] second, Mapper.Context context);
    }

    public static Converter longToStringC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            try {
                return ("" + Bytes.toLong(value)).getBytes(Charset.forName("UTF-8"));
            } catch (Exception e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.long_to_string.err").increment(1);
                return null;
            }
        }
    };

    public static Converter integerC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            try {
                return PDataType.INTEGER.toBytes(Bytes.toInt(value));
            } catch (Exception e) {
                System.out.println(Bytes.toStringBinary(value));
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
                return PDataType.LONG.toBytes(Bytes.toLong(value));
            } catch (Exception e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.long.err").increment(1);
                return null;
            }
        }
    };

    public static Converter dateC = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {

            try {
                Long millis;
                if (value.length < 8) {
                    millis = (long) Bytes.toInt(value) * 1000;
                } else {
                    millis = Bytes.toLong(value);
                }
                Date date = new Date(millis);
                return PDataType.DATE.toBytes(date);

            } catch (Exception e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.date.err").increment(1);
                return null;
            }
        }
    };

    public static AttachmentConverter attachmentsWithImageConverter = new AttachmentConverter() {
        @Override
        public byte[] convert(byte[] contentLow, byte[] contentStandard, byte[] contentThumbnail, Mapper.Context context) {
            Attachment.Image imageLow = createImage(contentLow, "low", context);
            Attachment.Image imageStandard = createImage(contentStandard, "standard", context);
            Attachment.Image imageThumbnail = createImage(contentThumbnail, "thumbnail", context);

            if (imageLow == null)
                return null;

            Attachment.Builder builder = Attachment.newBuilder()
                    .setImage(imageLow)
                    .addImages(imageLow);

            if (imageStandard != null)
                builder.addImages(imageStandard);

            if (imageThumbnail != null)
                builder.addImages(imageThumbnail);

            return Attachments.newBuilder().addAttachment(builder.build())
                    .build().toByteArray();
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
                Bytes.toStringBinary(json);
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

    public static LocationConverter locationConverter = new LocationConverter() {
        @Override
        public Location convert(byte[] value, Mapper.Context context) {
            Location location = new Location();
            if (value == null || value.length == 0)
                return location;
            try {
                JsonLocation json = mapper.readValue(value, JsonLocation.class);
                if (json == null)
                    return location;

                if (json.id != null)
                    location.id = json.id.getBytes(Charset.forName("UTF-8"));

                if (json.name != null)
                    location.name = json.name.getBytes(Charset.forName("UTF-8"));

                List<Double> coordinates = new LinkedList<Double>();

                if (json.latitude != null)
                    coordinates.add(json.latitude);
                else
                    context.getCounter("err", "converter.location.latitude.not_set").increment(1);

                if (json.longitude != null)
                    coordinates.add(json.longitude);
                else
                    context.getCounter("err", "converter.location.longitude.not_set").increment(1);

                if (coordinates.size() > 0) {
                    PhoenixArray phoenixArray = PArrayDataType.instantiatePhoenixArray(PDataType.DOUBLE, coordinates.toArray());
                    location.coordinates = PDataType.DOUBLE_ARRAY.toBytes(phoenixArray);
                }

           } catch (IOException e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.location.parse_json").increment(1);
            }
           return location;
        }
    };

    public static EntitiesConverter entitiesConverter = new EntitiesConverter() {

        private List<Entities.Tag> convertTag(byte[] value, Mapper.Context context) {
            List<Entities.Tag> tags = new LinkedList<Entities.Tag>();
            try {
                List<UsersInPhoto> usersInPhoto = mapper.readValue(value, new TypeReference<List<UsersInPhoto>>() {});
                for (UsersInPhoto userInPhoto : usersInPhoto) {
                    Entities.Tag.Builder tagBuilder =
                            Entities.Tag.newBuilder();
                    tagBuilder.setType(Entities.Tag.Type.USER_IN_PHOTO);

                    if (userInPhoto.user != null) {
                        if (userInPhoto.user.id != null) {
                            tagBuilder.setId(userInPhoto.user.id);
                        }
                        if (userInPhoto.user.username != null) {
                            tagBuilder.setName(userInPhoto.user.username);
                        }
                    }
                    if (userInPhoto.position != null) {
                        if (userInPhoto.position.x != null) {
                            tagBuilder.addPosition(userInPhoto.position.x);
                        }
                        if (userInPhoto.position.y != null) {
                            tagBuilder.addPosition(userInPhoto.position.y);
                        }
                    }
                    tags.add(tagBuilder.build());
                }
            } catch (IOException e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.user_in_photo.parse_json").increment(1);
            }
            return tags;
        }

        private List<Entities.Hashtag> convertHashTag(byte[] value, Mapper.Context context) {
            List<Entities.Hashtag> tags = new LinkedList<Entities.Hashtag>();

            try {
                List<String> hashtags = mapper.readValue(value, new TypeReference<List<String>>() {});
                for (String tag : hashtags) {
                    tags.add(Entities.Hashtag.newBuilder().setText(tag).build());
                }
            } catch (IOException e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.hashtags.parse_json").increment(1);
            }
            return tags;
        }

        @Override
        public byte[] convert(byte[] usersInPhoto, byte[] tags, Mapper.Context context) {
            Entities.Builder entities = Entities.newBuilder();

            if (usersInPhoto == null || usersInPhoto.length == 0) {
                context.getCounter("err", "converter.user_in_photo.not_set").increment(1);
            } else {
                List<Entities.Tag> photoTags = convertTag(usersInPhoto, context);
                if (photoTags.size() > 0)
                    entities.addAllTags(photoTags);
                else
                    context.getCounter("err", "converter.user_in_photo.is_empty").increment(1);
            }

            if (tags == null || tags.length == 0) {
                context.getCounter("err", "converter.hash_tags.not_set").increment(1);
            } else {
                List<Entities.Hashtag> hashtags = convertHashTag(tags, context);
                if (hashtags.size() > 0)
                    entities.addAllHashtags(hashtags);
                else
                    context.getCounter("err", "converter.hash_tags.is_empty").increment(1);
            }

            return entities.build().toByteArray();
        }
    };

    public static Converter raitingConverter = new Converter() {
        @Override
        public byte[] convert(byte[] value, Mapper.Context context) {
            byte[] raiting = null;
            try {
                Map<String, Object> map = (Map<String, Object>) mapper.readValue(value, Map.class);
                if (map != null && map.get("rating") != null) {
                    raiting = PDataType.DOUBLE.toBytes(Double.parseDouble(map.get("rating").toString()));
                } else {
                    context.getCounter("err", "converter.sbks_ea_rating.rating_not_set").increment(1);
                }

            } catch (IOException e) {
                System.out.println(Bytes.toStringBinary(value));
                e.printStackTrace();
                context.getCounter("err", "converter.sbks_ea_rating.parse_json").increment(1);
            }
            return raiting;
        }
    };

    public static class UsersInPhoto {
        public Position position;
        public User user;

        public static class Position {
            public Float x;
            public Float y;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class User {
            public String username;
            public String id;
        }
    }

    private static class JsonLocation {
        public Double latitude;
        public Double longitude;
        public String name;
        public String id;
    }
}
