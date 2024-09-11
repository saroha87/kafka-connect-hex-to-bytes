package com.github.saroha87.kafka.connect.smt;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.saroha87.kafka.connect.smt.HexStringToBytes.Value;

public class HexStringToBytesTest {
    private final HexStringToBytes<SourceRecord> transform = new Value<>();
    static final Pattern VALID_STR = Pattern.compile("[A-F0-9]+");

    @Before
    public void setUp() {
        final Map<String, Object> props = new HashMap<>();
        props.put(HexStringToBytes.FIELD_NAME, "name");
        props.put(HexStringToBytes.NULL_NAME, "true");
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
        assertArrayEquals(Hex.hexStringToByteArray(test, false), (byte[]) ((Map) transformedRecord.value()).get("name"));
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
        assertTrue(VALID_STR.matcher(test).matches());
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.STRING_SCHEMA).field("ip", Schema.INT32_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("ip", 7);
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertArrayEquals(null,(byte[]) ((Struct) transformedRecord.value()).get("name"));
    }
    
    @Test
    public void testWithSchemaWrongFieldValue() throws UnknownHostException {
        String test = new String("117.96.117.136");
        assertTrue(!VALID_STR.matcher(test).matches());
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.STRING_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("name", test);
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertArrayEquals(null,(byte[]) ((Struct) transformedRecord.value()).get("name"));
    }
    
}
