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
package org.apache.avro.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ported from the fork's {@code ExtendedJsonDecoderTest}: the
 * encoder&rarr;decoder round trips ({@code testBooleanHandling},
 * {@code testDoubleHandling} and their {@code serDeser} helper) that
 * {@link TestExtendedJsonDecoderPort} did not carry over — it ported only
 * {@code testDecoding} and pinned the read side with hand-written JSON,
 * because the encoder was out of scope for that port.
 *
 * <p>
 * Adaptations vs. the fork test, none of which change what is asserted:
 * Guava's {@code ImmutableMap} becomes {@code Map.of}, the helper's debug
 * print of the JSON is dropped, and the datum reader/writer are
 * generified. The fork's {@code testSchemaParsing} and
 * {@code testDefaultValueSerialization} are not ported here: they depend
 * on spf4j-generated classes (and, for the latter, on
 * {@code ExtendedGenericDatumWriter}), which are separate spec items.
 *
 * <p>
 * Does not compile on the seed: {@link ExtendedJsonEncoder} does not
 * exist there. That is the spec state — these tests go green when the
 * extended-JSON encoder is ported.
 */
public class TestExtendedJsonEncoderPort {

  @Test
  public void testBooleanHandling() throws IOException {
    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
        .name("boolVal").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
        .name("mapVal").type(Schema.createMap(Schema.create(Schema.Type.BOOLEAN))).noDefault()
        .name("defBoolVal").type(Schema.create(Schema.Type.BOOLEAN)).withDefault(false)
        .endRecord();

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("boolVal", true);
    record.put("defBoolVal", false);
    record.put("mapVal", Map.of("a", true, "b", false, "c", true, "d", false));
    Assert.assertTrue((Boolean) serDeser(record).get("boolVal"));
    Assert.assertFalse((Boolean) serDeser(record).get("defBoolVal"));
    Assert.assertFalse(
        (Boolean) ((Map<String, Boolean>) serDeser(record).get("mapVal")).get(new Utf8("b")));
  }

  @Test
  public void testDoubleHandling() throws IOException {
    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
        .name("doubleVal").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
        .name("mapVal").type(Schema.createMap(Schema.create(Schema.Type.DOUBLE))).noDefault()
        .name("defDoubleVal").type(Schema.create(Schema.Type.DOUBLE)).withDefault(Double.NaN)
        .endRecord();

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NaN);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", Map.of("a", 0.1, "b", Double.NaN, "c", Double.POSITIVE_INFINITY, "d", 0));
    Assert.assertTrue(Double.isNaN((double) serDeser(record).get("doubleVal")));
    Assert.assertTrue(Double
        .isNaN((double) ((Map<String, Double>) serDeser(record).get("mapVal")).get(new Utf8("b"))));

    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.POSITIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", Map.of("a", 0.1, "b", Double.NaN, "c", Double.POSITIVE_INFINITY));
    double serDeser = (double) serDeser(record).get("doubleVal");
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser > 0);

    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NEGATIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", Map.of("a", 0.1, "b", Double.NaN, "c", Double.POSITIVE_INFINITY));
    serDeser = (double) serDeser(record).get("doubleVal");
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser < 0);
  }

  public GenericRecord serDeser(GenericData.Record record) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(schema, bos);
    GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(schema, bis);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema);
    return reader.read(null, decoder);
  }
}
