package org.example;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import ksql.product;
import ksql.purchase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.example.model.AlertMessage;
import org.example.model.ProductRevenue;
import org.example.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlertApplication {
    private static final Logger log = LoggerFactory.getLogger(AlertApplication.class);

    public static final String PRODUCTS_TOPIC = "products";
    public static final String PURCHASES_TOPIC = "purchases";
    public static final String ALERTS_TOPIC = "product-alerts";
    public static final double ALERT_THRESHOLD = 3000.0;

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(buildTopology(), buildProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        log.info("Kafka Streams application started");
    }

    static Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-revenue-alert-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-alert-state");
        return props;
    }

    static Topology buildTopology() {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        SpecificAvroSerde<product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        SpecificAvroSerde<purchase> purchaseSerde = new SpecificAvroSerde<>();
        purchaseSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<Long, product> productsTable = builder.stream(
                        PRODUCTS_TOPIC,
                        Consumed.with(Serdes.Void(), productSerde)
                )
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> value.getId())
                .groupByKey(Grouped.with(Serdes.Long(), productSerde))
                .reduce((oldValue, newValue) -> newValue);

        KStream<Long, purchase> purchasesByProductId = builder.stream(
                        PURCHASES_TOPIC,
                        Consumed.with(Serdes.Long(), purchaseSerde)
                )
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> value.getProductid())
                .repartition(Repartitioned.with(Serdes.Long(), purchaseSerde));

        KStream<String, ProductRevenue> revenueStream = purchasesByProductId
                .join(
                        productsTable,
                        (purchaseValue, productValue) -> {
                            double revenue = purchaseValue.getQuantity() * productValue.getPrice();
                            return new ProductRevenue(
                                    productValue.getId(),
                                    productValue.getName(),
                                    purchaseValue.getQuantity(),
                                    productValue.getPrice(),
                                    revenue
                            );
                        }
                )
                .selectKey((key, value) -> String.valueOf(value.getProductId()));

        KTable<Windowed<String>, ProductRevenue> totals = revenueStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerde.forClass(ProductRevenue.class)))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ZERO))
                .aggregate(
                        ProductRevenue::new,
                        (productId, value, aggregate) -> {
                            aggregate.setProductId(value.getProductId());
                            aggregate.setProductName(value.getProductName());
                            aggregate.setPrice(value.getPrice());
                            aggregate.setQuantity(aggregate.getQuantity() + value.getQuantity());
                            aggregate.setRevenue(aggregate.getRevenue() + value.getRevenue());
                            return aggregate;
                        },
                        Materialized.<String, ProductRevenue, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("revenue-per-minute-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.forClass(ProductRevenue.class))
                );

        totals
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((windowedKey, aggregate) -> aggregate.getRevenue() > ALERT_THRESHOLD)
                .map((windowedKey, aggregate) -> {
                    AlertMessage alert = new AlertMessage(
                            windowedKey.key(),
                            aggregate.getProductName(),
                            windowedKey.window().start(),
                            windowedKey.window().end(),
                            aggregate.getRevenue()
                    );
                    return KeyValue.pair(windowedKey.key(), alert);
                })
                .peek((key, value) -> log.info(
                        "ALERT productId={}, productName={}, totalRevenue={}, windowStart={}, windowEnd={}",
                        value.getProductId(),
                        value.getProductName(),
                        value.getTotalRevenue(),
                        value.getWindowStart(),
                        value.getWindowEnd()
                ))
                .to(ALERTS_TOPIC, Produced.with(Serdes.String(), JsonSerde.forClass(AlertMessage.class)));

        return builder.build();
    }
}
