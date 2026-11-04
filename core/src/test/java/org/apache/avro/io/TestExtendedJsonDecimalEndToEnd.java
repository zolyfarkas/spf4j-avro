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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Spec test for the decimal natural-JSON read, end to end: a raw JSON
 * number read through {@link ExtendedJsonDecoder} and
 * {@link GenericDatumReader} with the standard decimal conversion
 * registered must yield the numeric value the JSON denotes, at the
 * schema's scale.
 *
 * <p>
 * The seed's decoder-level behavior is already pinned
 * ({@code TestExtendedJsonDecoderPort.testReadBigDecimal}); this test pins
 * the value that comes out of the full datum path. The seed decoder's
 * {@code readBytes} produces the fork's scale-prefixed byte layout for a
 * JSON number, which the standard {@link Conversions.DecimalConversion}
 * (expecting the bare unscaled value) misinterprets — see the
 * {@link ExtendedJsonDecoder} class javadoc. The fork's ecosystem consumes
 * that layout with its own conversions; whichever conversion semantics the
 * standalone library adopts, the end-to-end numeric value must be the one
 * asserted here.
 */
public class TestExtendedJsonDecimalEndToEnd {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"amount\",\"type\":"
          + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}}]}");

  @Test
  public void testDecimalNaturalReadEndToEndValue() throws IOException {
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA, SCHEMA, data);
    GenericRecord rec = reader.read(null, new ExtendedJsonDecoder(SCHEMA,
        new ByteArrayInputStream("{\"amount\":123.45}".getBytes(StandardCharsets.UTF_8))));
    Object amount = rec.get("amount");
    assertTrue("expected a BigDecimal but got " + (amount == null ? "null" : amount.getClass().getName()),
        amount instanceof BigDecimal);
    assertEquals(new BigDecimal("123.45"), amount);
    assertEquals(2, ((BigDecimal) amount).scale());
  }
}
