/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.spf4j.avro.generic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.spf4j.avro.Conversion;
import org.spf4j.avro.LogicalType;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.DecoderFactory;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.EncoderFactory;

/**
 * Pins the fork's write-path conversion dispatch (stage P7b/B1): a
 * conversion is looked up by logical type name and applies when its
 * converted type is assignable from the datum's class. Conversions whose
 * converted type is a supertype -- {@code Object} for {@code any},
 * {@code Map} for {@code json_record}, {@code Schema} for {@code avsc} in
 * the logical-types library -- were never applied by the exact-class
 * lookup this replaces: the raw datum (a {@code String}, a
 * {@code LinkedHashMap}, a {@code Schema} subclass instance) hit the
 * writer for the underlying type and failed with a
 * {@code ClassCastException}, or, inside a union, failed union resolution.
 */
public class TestSupertypeConversionWriteDispatch {

  /** Conversion with converted type {@code Object} (the "any" shape). */
  public static final class ObjectConversion extends Conversion<Object> {
    @Override
    public Class<Object> getConvertedType() {
      return Object.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "test_object_lt";
    }

    @Override
    public CharSequence toCharSequence(final Object value, final Schema schema, final LogicalType type) {
      return "converted:" + value;
    }

    @Override
    public Object fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
      return value.toString();
    }
  }

  /** Conversion with converted type {@code Map} (the "json_record" shape). */
  public static final class MapConversion extends Conversion<Map> {
    @Override
    @SuppressWarnings("unchecked")
    public Class<Map> getConvertedType() {
      return (Class) Map.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "test_map_lt";
    }

    @Override
    public ByteBuffer toBytes(final Map value, final Schema schema, final LogicalType type) {
      return ByteBuffer.wrap(("map:" + value.size()).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Map fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {
      throw new UnsupportedOperationException("read side not exercised by this test");
    }
  }

  /** Conversion with converted type {@code Schema} (the "avsc" shape). */
  public static final class SchemaConversion extends Conversion<Schema> {
    @Override
    public Class<Schema> getConvertedType() {
      return Schema.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "test_schema_lt";
    }

    @Override
    public CharSequence toCharSequence(final Schema value, final Schema schema, final LogicalType type) {
      return value.getType().getName();
    }

    @Override
    public Schema fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
      throw new UnsupportedOperationException("read side not exercised by this test");
    }
  }

  private static Schema stringSchemaWith(final String logicalTypeName) {
    Schema schema = Schema.create(Schema.Type.STRING);
    new LogicalType(logicalTypeName).addToSchema(schema);
    return schema;
  }

  private static Schema bytesSchemaWith(final String logicalTypeName) {
    Schema schema = Schema.create(Schema.Type.BYTES);
    new LogicalType(logicalTypeName).addToSchema(schema);
    return schema;
  }

  private static Schema recordWith(final Schema fieldType) {
    return Schema.createRecord("R", null, null, false,
        Arrays.asList(new Schema.Field("f", fieldType, null, null)));
  }

  private static byte[] write(final GenericData data, final Schema schema, final Object fieldValue) throws IOException {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", fieldValue);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().binaryEncoder(bos, null);
    new GenericDatumWriter<GenericRecord>(schema, data).write(record, enc);
    enc.flush();
    return bos.toByteArray();
  }

  private static GenericRecord readBack(final Schema schema, final byte[] bytes) throws IOException {
    return new GenericDatumReader<GenericRecord>(schema)
        .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
  }

  @Test
  void lookupByLogicalTypeWithSupertypeConvertedType() {
    GenericData data = new GenericData();
    ObjectConversion objectConversion = new ObjectConversion();
    MapConversion mapConversion = new MapConversion();
    SchemaConversion schemaConversion = new SchemaConversion();
    data.addLogicalTypeConversion(objectConversion);
    data.addLogicalTypeConversion(mapConversion);
    data.addLogicalTypeConversion(schemaConversion);

    // datum class is a strict subtype / implementation of the converted type
    assertSame(objectConversion,
        data.getConversionByClass(String.class, stringSchemaWith("test_object_lt").getLogicalType()));
    assertSame(objectConversion,
        data.getConversionByClass(ArrayList.class, stringSchemaWith("test_object_lt").getLogicalType()));
    assertSame(mapConversion,
        data.getConversionByClass(LinkedHashMap.class, bytesSchemaWith("test_map_lt").getLogicalType()));
    assertSame(schemaConversion, data.getConversionByClass(Schema.create(Schema.Type.DOUBLE).getClass(),
        stringSchemaWith("test_schema_lt").getLogicalType()));
    // ... and a datum class the converted type is not assignable from gets none
    assertNull(data.getConversionByClass(Integer.class, bytesSchemaWith("test_map_lt").getLogicalType()));
  }

  @Test
  void writerAppliesObjectConvertedTypeConversion() throws IOException {
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new ObjectConversion());
    Schema schema = recordWith(stringSchemaWith("test_object_lt"));

    // a String datum: without the conversion the raw value would be written
    GenericRecord back = readBack(schema, write(data, schema, "hello"));
    assertEquals("converted:hello", back.get("f").toString());

    // a datum whose class is not even a CharSequence: without the conversion
    // the writer fails with a ClassCastException on the raw value
    GenericRecord back2 = readBack(schema, write(data, schema, new ArrayList<>(Arrays.asList(1, 2))));
    assertEquals("converted:[1, 2]", back2.get("f").toString());
  }

  @Test
  void writerAppliesMapConvertedTypeConversion() throws IOException {
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new MapConversion());
    Schema schema = recordWith(bytesSchemaWith("test_map_lt"));

    Map<String, Object> datum = new LinkedHashMap<>();
    datum.put("a", 1);
    datum.put("b", 2);
    GenericRecord back = readBack(schema, write(data, schema, datum));
    ByteBuffer bytes = (ByteBuffer) back.get("f");
    assertEquals("map:2", new String(bytes.array(), bytes.position(), bytes.remaining(), StandardCharsets.UTF_8));
  }

  @Test
  void writerAppliesSchemaConvertedTypeConversion() throws IOException {
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new SchemaConversion());
    Schema schema = recordWith(stringSchemaWith("test_schema_lt"));

    // the datum's class is Schema$DoubleSchema, a subclass of Schema
    GenericRecord back = readBack(schema, write(data, schema, Schema.create(Schema.Type.DOUBLE)));
    assertEquals("double", back.get("f").toString());
  }

  @Test
  void unionResolutionUsesSupertypeConversion() throws IOException {
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new MapConversion());
    Schema union = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), bytesSchemaWith("test_map_lt")));
    Schema schema = recordWith(union);

    Map<String, Object> datum = new LinkedHashMap<>();
    datum.put("a", 1);
    GenericRecord back = readBack(schema, write(data, schema, datum));
    ByteBuffer bytes = (ByteBuffer) back.get("f");
    assertEquals("map:1", new String(bytes.array(), bytes.position(), bytes.remaining(), StandardCharsets.UTF_8));
  }
}
