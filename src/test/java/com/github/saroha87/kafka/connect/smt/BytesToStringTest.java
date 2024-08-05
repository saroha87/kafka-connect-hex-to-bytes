package com.github.saroha87.kafka.connect.smt;

import static org.junit.Assert.assertArrayEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.saroha87.kafka.connect.smt.HexStringToBytes.Value;

public class BytesToStringTest {
    private final HexStringToBytes<SourceRecord> transform = new Value<>();

    @Before
    public void setUp() {
        final Map<String, Object> props = new HashMap<>();
        props.put(HexStringToBytes.FIELD_NAME, "name");
        transform.configure(props);
    }

    @After
    public void tearDown() {
        transform.close();
    }

    @Test
    public void testWithoutSchema() {
        String test = "0A827570";
        final SourceRecord record = new SourceRecord(null, null, "topic", null,
                Collections.singletonMap("name", test));
        final SourceRecord transformedRecord = transform.apply(record);
        assertArrayEquals(Hex.hexStringToByteArray(test), (byte[]) ((Map) transformedRecord.value()).get("name"));
    }

    @Test
    public void testWithSchema() {
        String test = "0ABB1831";
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.STRING_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("name", test);
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertArrayEquals(Hex.hexStringToByteArray(test),(byte[]) ((Struct) transformedRecord.value()).get("name"));
    }
    
    @Test
    public void testWithSchemaNoFieldValue() {
        String test = "0ABB1831";
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.STRING_SCHEMA).field("ip", Schema.INT32_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("ip", 7);
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertArrayEquals(null,(byte[]) ((Struct) transformedRecord.value()).get("name"));
    }
    
}
