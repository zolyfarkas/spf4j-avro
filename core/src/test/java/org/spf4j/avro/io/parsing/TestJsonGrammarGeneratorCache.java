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
package org.spf4j.avro.io.parsing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.junit.Test;
import org.spf4j.avro.Conversion;
import org.spf4j.avro.Schema;
import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericDatumWriter;
import org.spf4j.avro.generic.GenericRecord;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.ExtendedJsonEncoder;
import org.spf4j.avro.io.JsonExtensionEncoder;

/**
 * Pins the root-symbol cache behind
 * {@link JsonGrammarGenerator#getRootSymbol(Schema)} (audit finding A1):
 * the extended JSON decoder/encoder {@code advanceBy} paths used to
 * regenerate the full JSON grammar of a schema for every value; the grammar
 * of a schema must now be generated once per generator instance and the
 * identical root symbol reused afterwards.
 */
public class TestJsonGrammarGeneratorCache {

  private static final String RECORD_SCHEMA = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
      + "{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}";

  @Test
  public void testRootSymbolIsReusedForSameSchema() {
    JsonGrammarGenerator generator = new JsonGrammarGenerator();
    Schema schema = new Schema.Parser().parse(RECORD_SCHEMA);
    Symbol first = generator.getRootSymbol(schema);
    assertSame("repeated getRootSymbol must return the cached symbol, not a regenerated one",
        first, generator.getRootSymbol(schema));
    assertSame(first, generator.getRootSymbol(schema));
  }

  @Test
  public void testRootSymbolIsSharedForEqualSchemas() {
    // the fork's static cache was keyed by schema equality; the
    // instance-owned cache keeps that keying
    JsonGrammarGenerator generator = new JsonGrammarGenerator();
    Schema first = new Schema.Parser().parse(RECORD_SCHEMA);
    Schema second = new Schema.Parser().parse(RECORD_SCHEMA);
    assertNotSame(first, second);
    assertSame(generator.getRootSymbol(first), generator.getRootSymbol(second));
  }

  @Test
  public void testRootSymbolDiffersPerSchemaAndPerInstance() {
    JsonGrammarGenerator generator = new JsonGrammarGenerator();
    Schema record = new Schema.Parser().parse(RECORD_SCHEMA);
    Schema array = Schema.createArray(Schema.create(Schema.Type.LONG));
    assertNotSame(generator.getRootSymbol(record), generator.getRootSymbol(array));
    // instance-owned: a fresh generator regenerates (no global cache)
    assertNotSame(generator.getRootSymbol(record), new JsonGrammarGenerator().getRootSymbol(record));
  }

  @Test
  public void testRepeatedAdvanceByOnOneEncoder() throws IOException {
    // a record with two fields of the same named fixed-decimal schema:
    // writing it drives ExtendedJsonEncoder.advanceBy twice with the
    // identical Schema instance (FIXED takes the grammar branch), so the
    // second call must be served by the cache and produce identical output
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TwoDec\",\"fields\":["
        + "{\"name\":\"a\",\"type\":{\"type\":\"fixed\",\"name\":\"Dec\",\"size\":8,"
        + "\"logicalType\":\"decimal\",\"precision\":18,\"scale\":2}},"
        + "{\"name\":\"b\",\"type\":\"Dec\"}]}");
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new NaturalDecimalConversion());
    GenericRecord record = new GenericData.Record(schema);
    record.put("a", new BigDecimal("123.45"));
    record.put("b", new BigDecimal("-7.50"));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(schema, bos);
    new GenericDatumWriter<GenericRecord>(schema, data).write(record, encoder);
    encoder.flush();
    assertEquals("{\"a\":123.45,\"b\":-7.50}", bos.toString(StandardCharsets.UTF_8));
  }

  /** Decimal conversion that writes through the extension encoder hook. */
  public static final class NaturalDecimalConversion extends Conversion<BigDecimal> {

    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "decimal";
    }

    @Override
    public boolean tryDirectEncode(final BigDecimal object, final Encoder enc, final Schema schema)
        throws IOException {
      if (enc instanceof JsonExtensionEncoder) {
        ((JsonExtensionEncoder) enc).writeDecimal(object, schema);
        return true;
      }
      return false;
    }
  }
}
