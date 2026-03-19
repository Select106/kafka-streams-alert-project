package org.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

public final class JsonSerde {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSerde() {
    }

    public static <T> Serde<T> forClass(Class<T> clazz) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize " + clazz.getSimpleName(), e);
            }
        };

        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, clazz);
            } catch (Exception e) {
                throw new RuntimeException("Cannot deserialize " + clazz.getSimpleName(), e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
