# Kafka Streams alert project
Приложение отправляет сообщение в topic `product-alerts`, если суммарная выручка по продукту за последнюю минуту превышает `3000`.
Формула выручки для одной покупки:
`purchase.quantity * product.price`
## Что реализовано
- чтение `products` и `purchases` из Kafka
- re-key для `products` по `product.id`
- re-key для `purchases` по `purchase.productid`
- join покупок с продуктами
- расчёт выручки по каждой покупке
- оконная агрегация в минутном окне
- отправка алерта в `product-alerts` после закрытия окна
## Стек
- Java 17
- Gradle 7.6+
- Kafka Streams 2.8.0
- Confluent Avro Serde 6.2.0
- Kafka Connect Datagen
- Schema Registry
## Структура проекта
- `src/main/java/org/example/AlertApplication.java` — основная топология Kafka Streams
- `src/main/java/org/example/model/*` — модели `ProductRevenue` и `AlertMessage`
- `src/main/java/org/example/serde/JsonSerde.java` — JSON serde для внутренних моделей и алертов
- `src/main/resources/connectors/*.json` — конфиги Datagen-коннекторов
- `src/main/avro/*.avsc` — Avro-схемы для генерации Java-классов
- `product.avsc`, `purchase.avsc` — исходные схемы для Datagen
## Важные замечания
1. `products` в Datagen приходят с пустым Kafka key, поэтому в приложении сделан re-key по `product.id`.
2. После `selectKey` для `purchases` добавлен `repartition(...)` с явным `Long` serde, иначе Streams падает на internal topic.
3. Datagen-коннекторы имеют лимит `iterations=100000`. После исчерпания лимита task завершится — это нормальное поведение.
4. Если нужно прогнать генерацию заново, проще удалить коннектор и создать его снова или поменять имя коннектора.
5. Для повторного запуска приложения после неудачных прогонов полезно:
   - удалить локальный state: `rm -rf /tmp/kafka-streams-alert-state`
   - при необходимости сменить `application.id`

