package com.github.saroha87.kafka.connect.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class HexStringToBytes<R extends ConnectRecord<R>> implements Transformation<R> {
	static String FIELD_NAME = "field";
	private static final String PURPOSE = "Decode a HEX encoded field";
	static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
	private Set<String> fieldNames = null;
	private final Map<Schema, Schema> schemaLookup = new HashMap<>();

	@Override
	public void configure(Map<String, ?> props) {
		SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
		String fieldName = config.getString(FIELD_NAME);
		if(fieldName == null || fieldName.isBlank()) {
			fieldNames = Collections.emptySet();
		}else {
			fieldNames = Arrays.stream(fieldName.split(",")).collect(Collectors.toSet());
		}
	}

	@Override
	public R apply(R record) {
		if (actualSchema(record) == null) {
			return applySchemaless(record);
		} else {
			return applyWithSchema(record);
		}
	}

	private R applySchemaless(R record) {
		Object recordVal = actualValue(record);
		if (recordVal == null) {
			return newRecord(record, null, null);
		} else if (fieldNames == null || fieldNames.isEmpty()) {
			return newRecord(record, null, recordVal);
		} else {
			final Map<String, Object> value = requireMap(recordVal, PURPOSE);
			final Map<String, Object> updatedValue = new HashMap<>(value);
			fieldNames.forEach(v -> updatedValue.put(v, Hex.hexStringToByteArray((String) value.get(v))));
			return newRecord(record, null, updatedValue);
		}
	}

	private R applyWithSchema(R record) {
		Object recordVal = actualValue(record);
		if (recordVal == null) {
			return newRecord(record, null, null);
		} else if (fieldNames == null || fieldNames.isEmpty()) {
			return newRecord(record, null, recordVal);
		} else {
			final Struct value = requireStruct(recordVal, PURPOSE);
			final Schema updatedSchema = schemaLookup.computeIfAbsent(record.valueSchema(), inputSchema -> {
				SchemaBuilder builder = SchemaBuilder.struct();
				if (null != inputSchema.name()) {
					builder.name(inputSchema.name());
				}
				if (null != inputSchema.doc()) {
					builder.doc(inputSchema.doc());
				}
				if (inputSchema.isOptional()) {
					builder.optional();
				}
				if (null != inputSchema.version()) {
					builder.version(inputSchema.version() + 1);
				}
				for (Field field : inputSchema.fields()) {
					if (fieldNames.contains(field.name())) {
						builder.field(field.name(), Schema.OPTIONAL_BYTES_SCHEMA);
					} else {
						builder.field(field.name(), field.schema());
					}
				}
				return builder.build();
			});
			final Struct updatedValue = new Struct(updatedSchema);

			for (Field field : value.schema().fields()) {
				if (fieldNames.contains(field.name())) {
					updatedValue.put(field.name(), Hex.hexStringToByteArray((String) value.get(field.name())));
				} else {
					updatedValue.put(field.name(), value.get(field));
				}
			}

			return newRecord(record, updatedSchema, updatedValue);
		}
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {

	}

	protected abstract Schema actualSchema(R record);

	protected abstract Object actualValue(R record);

	protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

	public static class Key<R extends ConnectRecord<R>> extends HexStringToBytes<R> {

		@Override
		protected Schema actualSchema(R record) {
			return record.keySchema();
		}

		@Override
		protected Object actualValue(R record) {
			return record.key();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedKey) {
			return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedKey, record.valueSchema(), record.value(), record.timestamp());
		}
	}

	public static class Value<R extends ConnectRecord<R>> extends HexStringToBytes<R> {

		@Override
		protected Schema actualSchema(R record) {
			return record.valueSchema();
		}

		@Override
		protected Object actualValue(R record) {
			return record.value();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
			return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
		}
	}
}
