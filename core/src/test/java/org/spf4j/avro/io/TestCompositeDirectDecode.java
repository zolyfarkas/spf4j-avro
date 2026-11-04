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
package org.spf4j.avro.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.spf4j.avro.Conversion;
import org.spf4j.avro.DatumContext;
import org.spf4j.avro.LogicalType;
import org.spf4j.avro.LogicalTypes;
import org.spf4j.avro.Schema;
import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericDatumReader;
import org.spf4j.avro.generic.GenericRecord;
import org.spf4j.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pins direct decode of composite-typed logical types (stage P7b/B3).
 *
 * <p>
 * A conversion whose logical type sits on a record (the logical-types
 * library's {@code any} shape) direct-decodes by consuming the whole
 * record value through the {@link JsonExtensionDecoder} interface
 * ({@code bufferValue} / {@code readValueAsTree}). Through a
 * {@link ResolvingDecoder} that requires the resolving grammar to be
 * advanced past the composite value; {@code ResolvingDecoder.advanceBy}
 * used to throw {@code UnsupportedOperationException} for every composite
 * schema, so such a value inside a map could not be direct-decoded at all
 * on the classic reader path. The fast reader path hands the conversion
 * the raw decoder instead, whose own grammar advance already handled the
 * shape -- the two paths must now agree.
 * </p>
 *
 * <p>
 * The {@code readValueAsTree} conversion variant and the direct
 * {@code ExtendedJsonDecoder} tests also pin the codec defect fixed in
 * the same stage: {@code ExtendedJsonDecoder.readValue} /
 * {@code readValueAsTree} called {@code JsonParser.readValueAs*} on
 * codec-less parsers and always threw {@code IllegalStateException}.
 * </p>
 *
 * <p>
 * Scope note: these tests use identical writer and reader schemas (the
 * logical-types library's scenario). With evolved schemas, the resolving
 * grammar walk above is correct, but the underlying
 * {@code ExtendedJsonDecoder} advanceBy surgery computes its skip counts
 * from the reader schema against a writer-grammar stack and under-skips
 * writer-only fields inside a buffered value -- a pre-existing
 * limitation it shares with the fork, not addressed here.
 * </p>
 */
public class TestCompositeDirectDecode {

  private static final String LT = "test_composite_lt";
  private static final String ARRAY_LT = "test_array_lt";

  static {
    LogicalTypes.register(LT, new LogicalTypes.LogicalTypeFactory() {
      @Override
      public LogicalType fromSchema(Schema schema) {
        return new LogicalType(LT);
      }

      @Override
      public String getTypeName() {
        return LT;
      }
    });
    LogicalTypes.register(ARRAY_LT, new LogicalTypes.LogicalTypeFactory() {
      @Override
      public LogicalType fromSchema(Schema schema) {
        return new LogicalType(ARRAY_LT);
      }

      @Override
      public String getTypeName() {
        return ARRAY_LT;
      }
    });
  }

  /** Direct-decodes a record value by buffering it, as the "any" conversion does. */
  public static class BufferingConversion extends Conversion<String> {
    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return LT;
    }

    @Override
    public Optional<String> tryDirectDecode(Decoder dec, Schema schema, DatumContext context) throws IOException {
      if (dec instanceof JsonExtensionDecoder) {
        JsonParser parser = ((JsonExtensionDecoder) dec).bufferValue(schema);
        JsonNode node = new ObjectMapper().readTree(parser);
        return Optional.of(node.toString());
      }
      return Optional.empty();
    }
  }

  /** Direct-decodes a value by reading it as a tree, through the extension decoder. */
  public static class TreeConversion extends Conversion<String> {
    private final String logicalTypeName;

    public TreeConversion(String logicalTypeName) {
      this.logicalTypeName = logicalTypeName;
    }

    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return logicalTypeName;
    }

    @Override
    public Optional<String> tryDirectDecode(Decoder dec, Schema schema, DatumContext context) throws IOException {
      if (dec instanceof JsonExtensionDecoder) {
        JsonNode node = ((JsonExtensionDecoder) dec).readValueAsTree(schema);
        return Optional.of(node.toString());
      }
      return Optional.empty();
    }
  }

  private static Schema innerSchema() {
    Schema inner = Schema.createRecord("Inner", null, null, false,
        Arrays.asList(new Schema.Field("x", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("y", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("ys", Schema.createArray(Schema.create(Schema.Type.STRING)), null, null)));
    new LogicalType(LT).addToSchema(inner);
    return inner;
  }

  private static Schema mapOuterSchema() {
    return Schema.createRecord("Outer", null, null, false,
        Arrays.asList(new Schema.Field("m", Schema.createMap(innerSchema()), null, null),
            new Schema.Field("after", Schema.create(Schema.Type.STRING), null, null)));
  }

  private static Schema fieldOuterSchema() {
    return Schema.createRecord("Outer", null, null, false,
        Arrays.asList(new Schema.Field("v", innerSchema(), null, null),
            new Schema.Field("after", Schema.create(Schema.Type.STRING), null, null)));
  }

  private static GenericRecord readExtendedJson(Schema schema, String json, GenericData data) throws IOException {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, data);
    return reader.read(null,
        new ExtendedJsonDecoder(schema, new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));
  }

  private static GenericData data(boolean fast, Conversion<?> conversion) {
    GenericData data = new GenericData();
    data.setFastReaderEnabled(fast);
    data.addLogicalTypeConversion(conversion);
    return data;
  }

  private void assertMapScenario(boolean fast) throws IOException {
    Schema schema = mapOuterSchema();
    String json = "{\"m\":{\"k\":{\"x\":1,\"y\":\"a\",\"ys\":[\"p\",\"q\"]},"
        + "\"k2\":{\"x\":2,\"y\":\"b\",\"ys\":[]}},\"after\":\"done\"}";
    GenericRecord rec = readExtendedJson(schema, json, data(fast, new BufferingConversion()));
    Map<?, ?> map = (Map<?, ?>) rec.get("m");
    assertEquals(2, map.size());
    assertEquals("{\"x\":1,\"y\":\"a\",\"ys\":[\"p\",\"q\"]}", map.get(new Utf8("k")).toString());
    assertEquals("{\"x\":2,\"y\":\"b\",\"ys\":[]}", map.get(new Utf8("k2")).toString());
    // the decoder is positioned exactly at the next field after the map
    assertEquals("done", rec.get("after").toString());
  }

  @Test
  public void testRecordLogicalTypeInsideMapClassicReader() throws IOException {
    assertMapScenario(false);
  }

  @Test
  public void testRecordLogicalTypeInsideMapFastReader() throws IOException {
    assertMapScenario(true);
  }

  private void assertFieldScenario(boolean fast, Conversion<String> conversion) throws IOException {
    Schema schema = fieldOuterSchema();
    String json = "{\"v\":{\"x\":3,\"y\":\"c\",\"ys\":[\"z\"]},\"after\":\"next\"}";
    GenericRecord rec = readExtendedJson(schema, json, data(fast, conversion));
    assertEquals("{\"x\":3,\"y\":\"c\",\"ys\":[\"z\"]}", rec.get("v").toString());
    // advanceBy over the composite value leaves the decoder at the next field
    assertEquals("next", rec.get("after").toString());
  }

  @Test
  public void testRecordLogicalTypeFieldClassicReader() throws IOException {
    assertFieldScenario(false, new BufferingConversion());
  }

  @Test
  public void testRecordLogicalTypeFieldFastReader() throws IOException {
    assertFieldScenario(true, new BufferingConversion());
  }

  @Test
  public void testRecordLogicalTypeFieldViaTreeClassicReader() throws IOException {
    assertFieldScenario(false, new TreeConversion(LT));
  }

  @Test
  public void testRecordLogicalTypeFieldViaTreeFastReader() throws IOException {
    assertFieldScenario(true, new TreeConversion(LT));
  }

  @Test
  public void testArrayLogicalTypeClassicAndFastReader() throws IOException {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
    new LogicalType(ARRAY_LT).addToSchema(arraySchema);
    Schema schema = Schema.createRecord("Outer", null, null, false,
        Arrays.asList(new Schema.Field("arr", arraySchema, null, null),
            new Schema.Field("after", Schema.create(Schema.Type.STRING), null, null)));
    String json = "{\"arr\":[1,2,3],\"after\":\"done\"}";
    for (boolean fast : new boolean[] { false, true }) {
      GenericRecord rec = readExtendedJson(schema, json, data(fast, new TreeConversion(ARRAY_LT)));
      assertEquals("[1,2,3]", rec.get("arr").toString());
      assertEquals("done", rec.get("after").toString());
    }
  }

  @Test
  public void testExtendedJsonDecoderReadValueAndTree() throws IOException {
    // readValueAsTree over a composite value (previously IllegalStateException:
    // no ObjectCodec on the parser)
    Schema record = Schema.createRecord("R", null, null, false,
        Arrays.asList(new Schema.Field("x", Schema.create(Schema.Type.INT), null, null)));
    ExtendedJsonDecoder treeDecoder = new ExtendedJsonDecoder(record,
        new ByteArrayInputStream("{\"x\":1}".getBytes(StandardCharsets.UTF_8)));
    JsonNode tree = treeDecoder.readValueAsTree(record);
    assertEquals(1, tree.get("x").asInt());

    // readValue on a non-string token, through the same extension method
    ExtendedJsonDecoder valueDecoder = new ExtendedJsonDecoder(record,
        new ByteArrayInputStream("{\"x\":2}".getBytes(StandardCharsets.UTF_8)));
    @SuppressWarnings("unchecked")
    Map<String, Object> value = valueDecoder.readValue(record, Map.class);
    assertEquals(2, ((Number) value.get("x")).intValue());

    // readValue on a string token whose text is a JSON payload
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    ExtendedJsonDecoder stringDecoder = new ExtendedJsonDecoder(stringSchema,
        new ByteArrayInputStream("\"{\\\"a\\\":1}\"".getBytes(StandardCharsets.UTF_8)));
    @SuppressWarnings("unchecked")
    Map<String, Object> fromString = stringDecoder.readValue(stringSchema, Map.class);
    assertEquals(1, ((Number) fromString.get("a")).intValue());
    assertTrue(fromString.size() == 1);
  }
}
