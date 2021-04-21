package io.lenses.examples.serde;

import com.landoop.lenses.lsql.serde.Deserializer;
import com.landoop.lenses.lsql.serde.Serde;
import com.landoop.lenses.lsql.serde.Serializer;
import io.lenses.examples.serde.protobuf.generated.CardData;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Properties;

public class CreditCardProtobufSerde implements Serde {

    private Schema fieldBSchema = SchemaBuilder.builder().record("field_b").fields().requiredString("x").endRecord();

    private Schema schema = SchemaBuilder.builder()
            .record("credit_card")
            .fields()
            .requiredInt("a")
            .name("b")
                .type(fieldBSchema)
                .noDefault()
            .endRecord();

    @Override
    public Serializer serializer(Properties properties) {

        return new Serializer() {
            @Override
            public byte[] serialize(GenericRecord record) throws IOException {
                CardData.CreditCard card = CardData.CreditCard.newBuilder()
                        .setName((String) record.get("number"))
                        .setCardNumber((String) record.get("customerFirstName"))
                        .setType((String) record.get("customerLastName"))
                        .setCountry((String) record.get("country"))
                        .setCurrency((String) record.get("currency"))
                        .setBlocked((boolean) record.get("blocked"))
                        .build();
                return card.toByteArray();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public Deserializer deserializer(Properties properties) {
        return new Deserializer() {
            @Override
            public GenericRecord deserialize(byte[] bytes) throws IOException {

                CardData.CreditCard card = CardData.CreditCard.parseFrom(bytes);

                GenericRecord record = new GenericData.Record(schema);
                record.put("number", card.getName());
                record.put("customerFirstName", card.getCardNumber());
                record.put("customerLastName", card.getType());
                record.put("country", card.getCountry());
                record.put("blocked", card.getBlocked());
                record.put("currency", card.getCurrency());
                return record;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
