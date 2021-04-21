package io.lenses.examples.serde;

import com.landoop.lenses.lsql.serde.Deserializer;
import com.landoop.lenses.lsql.serde.Serde;
import com.landoop.lenses.lsql.serde.Serializer;
import io.lenses.examples.serde.domain.CardType;
import io.lenses.examples.serde.domain.CreditCard;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.util.Properties;

public class CreditCardJavaSerializationSerde implements Serde {

    private Schema schema = SchemaBuilder.builder()
            .record("credit_card")
            .fields()
            .requiredString("number")
            .requiredString("customerFirstName")
            .requiredString("customerLastName")
            .requiredString("country")
            .requiredString("currency")
            .requiredBoolean("blocked")
            .endRecord();

    @Override
    public Serializer serializer(Properties properties) {

        return new Serializer() {
            @Override
            public byte[] serialize(GenericRecord record) throws IOException {
                String name = (String) record.get("number");
                String cardNumber = (String) record.get("customerFirstName");
                CardType type = CardType.valueOf((String) record.get("customerLastName"));
                String country = (String) record.get("country");
                String currency = (String) record.get("currency");
                Boolean blocked = (boolean) record.get("blocked");

                CreditCard creditCard = new CreditCard(name, country, currency, cardNumber, blocked, type);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(output);

                // Method for serialization of object
                outputStream.writeObject(creditCard);
                outputStream.close();
                byte[] bytes = output.toByteArray();
                output.close();
                return bytes;
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
                ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                ObjectInputStream in = new ObjectInputStream(inputStream);

                CreditCard card = null;
                try {
                    card = (CreditCard) in.readObject();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to read CreditCard");
                }
                in.close();
                inputStream.close();

                GenericRecord record = new GenericData.Record(schema);
                record.put("number", card.getName());
                record.put("customerFirstName", card.getCardNumber());
                record.put("customerLastName", card.getType().name());
                record.put("country", card.getCountry());
                record.put("currency", card.getCurrency());
                record.put("blocked", card.getBlocked());
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
