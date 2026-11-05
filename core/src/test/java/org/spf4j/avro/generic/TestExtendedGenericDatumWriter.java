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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.ExtendedJsonEncoder;
import org.spf4j.avro.io.DatumWriter;
import org.spf4j.avro.io.EncoderFactory;
import org.spf4j.avro.util.Utf8;

/**
 * Re-expression of the fork's {@code ExtendedJsonDecoderTest.testDefaultValueSerialization}
 * against a locally defined schema (the donor test uses spf4j's generated
 * {@code LogRecord}, whose {@code throwable} field is a nullable union with a
 * {@code null} default): a record written through
 * {@link ExtendedGenericDatumWriter} + {@link ExtendedJsonEncoder} omits
 * every field whose value equals its schema default — including the
 * {@code "throwable":null} pair the donor test asserts absent — and writes
 * non-default fields normally.
 */
public class TestExtendedGenericDatumWriter {

  private static final Schema SCHEMA = new Schema.Parser().parse("{"
      + "\"type\":\"record\",\"name\":\"LogLike\",\"namespace\":\"test\",\"fields\":["
      + "{\"name\":\"origin\",\"type\":\"string\"},"
      + "{\"name\":\"level\",\"type\":{\"type\":\"enum\",\"name\":\"Level\",\"symbols\":[\"DEBUG\",\"INFO\"]},"
      + "\"default\":\"DEBUG\"},"
      + "{\"name\":\"count\",\"type\":\"int\",\"default\":0},"
      + "{\"name\":\"note\",\"type\":\"string\",\"default\":\"none\"},"
      + "{\"name\":\"throwable\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Err\","
      + "\"fields\":[{\"name\":\"msg\",\"type\":\"string\"}]}],\"default\":null},"
      + "{\"name\":\"message\",\"type\":\"string\"}]}");

  private static GenericData.Record allDefaultsRecord() {
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("origin", new Utf8("origin1"));
    record.put("level", new GenericData.EnumSymbol(SCHEMA.getField("level").schema(), "DEBUG"));
    record.put("count", 0);
    record.put("note", new Utf8("none"));
    record.put("throwable", null);
    record.put("message", new Utf8("hello"));
    return record;
  }

  private static String write(final GenericData.Record record, final DatumWriter<GenericData.Record> writer,
      final boolean extended)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = extended ? new ExtendedJsonEncoder(SCHEMA, bos) : EncoderFactory.get().jsonEncoder(SCHEMA, bos);
    writer.write(record, encoder);
    encoder.flush();
    return bos.toString(StandardCharsets.UTF_8);
  }

  @Test
  public void testDefaultValuedFieldsAreOmittedFromExtendedJson() throws IOException {
    String json = write(allDefaultsRecord(), new ExtendedGenericDatumWriter<>(SCHEMA), true);
    // the donor test's assertion, re-expressed: no "throwable":null
    assertFalse(json, json.contains("\"throwable\""));
    // and every other default-equal field is omitted as well
    assertFalse(json, json.contains("\"level\""));
    assertFalse(json, json.contains("\"count\""));
    assertFalse(json, json.contains("\"note\""));
    // non-default fields are present with their values
    assertTrue(json, json.contains("\"origin\":\"origin1\""));
    assertTrue(json, json.contains("\"message\":\"hello\""));
  }

  @Test
  public void testNonDefaultFieldsAreWritten() throws IOException {
    GenericData.Record record = allDefaultsRecord();
    record.put("level", new GenericData.EnumSymbol(SCHEMA.getField("level").schema(), "INFO"));
    record.put("count", 3);
    record.put("note", new Utf8("custom"));
    Schema errSchema = SCHEMA.getField("throwable").schema().getTypes().get(1);
    GenericData.Record err = new GenericData.Record(errSchema);
    err.put("msg", new Utf8("boom"));
    record.put("throwable", err);

    String json = write(record, new ExtendedGenericDatumWriter<>(SCHEMA), true);
    assertTrue(json, json.contains("\"level\":\"INFO\""));
    assertTrue(json, json.contains("\"count\":3"));
    assertTrue(json, json.contains("\"note\":\"custom\""));
    // nullable union written bare by the extended encoder
    assertTrue(json, json.contains("\"throwable\":{\"msg\":\"boom\"}"));
    assertTrue(json, json.contains("\"message\":\"hello\""));
  }

  @Test
  public void testPlainWriterWritesDefaultValuedFields() throws IOException {
    // control: the skipping is the extended writer's doing, not the
    // encoder's - the stock writer over the same extended encoder emits
    // the default-valued fields, "throwable":null included
    String json = write(allDefaultsRecord(), new GenericDatumWriter<>(SCHEMA), true);
    assertTrue(json, json.contains("\"throwable\":null"));
    assertTrue(json, json.contains("\"level\":\"DEBUG\""));
    assertTrue(json, json.contains("\"count\":0"));
    assertTrue(json, json.contains("\"note\":\"none\""));
  }

  @Test
  public void testExtendedWriterWithStockEncoderWritesEverything() throws IOException {
    // the skip needs the extended encoder's parser; with a stock
    // JsonEncoder the extended writer falls back to writing every field
    String json = write(allDefaultsRecord(), new ExtendedGenericDatumWriter<>(SCHEMA), false);
    assertTrue(json, json.contains("\"throwable\":null"));
    assertTrue(json, json.contains("\"count\":0"));
    assertTrue(json, json.contains("\"message\":\"hello\""));
  }
}
