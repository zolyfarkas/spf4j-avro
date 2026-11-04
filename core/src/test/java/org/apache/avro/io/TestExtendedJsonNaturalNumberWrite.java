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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Spec tests for the natural (extended) JSON <em>write</em> of decimal and
 * bigint values — behavior that has no test coverage anywhere in the fork
 * or the seed (the seed pins only the read side,
 * {@code TestExtendedJsonDecoderPort.testReadBigDecimal}).
 *
 * <p>
 * Spec source is the fork's {@code ExtendedJsonEncoder}: {@code
 * writeDecimal} and {@code writeBigInteger} emit the value as a raw JSON
 * number (Jackson {@code writeNumber}) — not as a string and not as the
 * standard bytes encoding — after advancing the grammar by the schema.
 * The {@link BigDecimal} is written with its own scale: trailing zeros
 * that carry scale are preserved ({@code 1.10} stays {@code 1.10}).
 *
 * <p>
 * The datum-writer test models the fork's production path:
 * {@code GenericDatumWriter} offers every logical-typed value to its
 * conversion's {@code Conversion.tryDirectEncode} first (fork
 * {@code GenericDatumWriter}), and the fork's {@code Decimal2Converter}
 * answers by delegating to {@code JsonExtensionEncoder.writeDecimal} when
 * the encoder is an extension encoder. The in-test conversion below
 * applies that same pattern to the standard {@code decimal} logical type.
 * Bigint is pinned by direct calls only: no {@code bigint} logical type
 * exists in fork core (it lives in the logical-types library), so there
 * is no core datum-writer path for it to spec here.
 *
 * <p>
 * Does not compile on the seed: {@link ExtendedJsonEncoder},
 * {@link JsonExtensionEncoder} and {@code Conversion.tryDirectEncode} are
 * all absent there.
 */
public class TestExtendedJsonNaturalNumberWrite {

  private static final Schema DECIMAL_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}");

  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);

  private static String writeDecimal(BigDecimal value) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(DECIMAL_SCHEMA, bos);
    encoder.writeDecimal(value, DECIMAL_SCHEMA);
    encoder.flush();
    return bos.toString(StandardCharsets.UTF_8);
  }

  private static String writeBigInteger(BigInteger value) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(LONG_SCHEMA, bos);
    encoder.writeBigInteger(value, LONG_SCHEMA);
    encoder.flush();
    return bos.toString(StandardCharsets.UTF_8);
  }

  @Test
  public void testWriteDecimalAsRawJsonNumber() throws IOException {
    // a raw JSON number token: not "123.45" (string), not a base-unicode
    // bytes string -- exactly the number, nothing else
    assertEquals("123.45", writeDecimal(new BigDecimal("123.45")));
    assertEquals("0.01", writeDecimal(new BigDecimal("0.01")));
    assertEquals("-7.50", writeDecimal(new BigDecimal("-7.50")));
    assertEquals("12345678901234567890.12", writeDecimal(new BigDecimal("12345678901234567890.12")));
  }

  @Test
  public void testWriteDecimalPreservesScale() throws IOException {
    // the BigDecimal's scale is written out, not normalized away:
    // 1.10 (scale 2) must not collapse to 1.1
    assertEquals("1.10", writeDecimal(new BigDecimal("1.10")));
    assertEquals("100.00", writeDecimal(new BigDecimal("100.00")));
    assertEquals("0.00", writeDecimal(new BigDecimal("0.00")));
  }

  @Test
  public void testWriteBigIntegerAsRawJsonNumber() throws IOException {
    assertEquals("123456789012345678901234567890",
        writeBigInteger(new BigInteger("123456789012345678901234567890")));
    assertEquals("-987654321987654321", writeBigInteger(new BigInteger("-987654321987654321")));
    assertEquals("0", writeBigInteger(BigInteger.ZERO));
  }

  /** The fork's Decimal2Converter pattern, applied to standard decimal. */
  public static class NaturalDecimalConversion extends Conversion<BigDecimal> {

    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "decimal";
    }

    @Override
    public boolean tryDirectEncode(BigDecimal object, Encoder enc, Schema schema) throws IOException {
      if (enc instanceof JsonExtensionEncoder) {
        ((JsonExtensionEncoder) enc).writeDecimal(object, schema);
        return true;
      }
      return false;
    }

    @Override
    public BigDecimal fromBytes(java.nio.ByteBuffer value, Schema schema, LogicalType type) {
      byte[] bytes = new byte[value.remaining()];
      value.duplicate().get(bytes);
      return new BigDecimal(new BigInteger(bytes),
          ((org.apache.avro.LogicalTypes.Decimal) type).getScale());
    }

    @Override
    public java.nio.ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
      return java.nio.ByteBuffer
          .wrap(value.setScale(((org.apache.avro.LogicalTypes.Decimal) type).getScale()).unscaledValue()
              .toByteArray());
    }
  }

  @Test
  public void testDecimalThroughDatumWriterIsRawJsonNumber() throws IOException {
    Schema recordSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"amount\",\"type\":"
            + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}}]}");
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new NaturalDecimalConversion());
    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("amount", new BigDecimal("123.45"));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(recordSchema, data);
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(recordSchema, bos);
    writer.write(record, encoder);
    encoder.flush();
    assertEquals("{\"amount\":123.45}", bos.toString(StandardCharsets.UTF_8));
  }
}
