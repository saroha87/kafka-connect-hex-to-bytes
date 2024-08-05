# kafka connect hex string to bytes
A Kafka Connect Single Message Transform (SMT) to convert a HEX in string representation to byte array. For records with
schemas, this will also update the schema for the specified field from String to BYTES. This utility healp in mapping HEX string to RAW datatype of Oracle Databse

This SMT supports converting fields in the record Key or Value

Properties:

|Name|Description|Type|Importance|
|---|---|---|---|
|`field`| Field name to decode | String| High |

Example configs:

```
transforms=decode
transforms.decode.type=com.github.saroha87.kafka.connect.smt.HexStringToBytes$Value
transforms.decode.field="ipAddress"
```

----------
### How to use this

- Run `mvn clean package` in the repo's root directory
- Copy the created jar from the /target directory to some directory, say `kafka-connect-hex-to-bytes`, in your Connect worker's plugin path
- Create a connector using this transform in its properties!
