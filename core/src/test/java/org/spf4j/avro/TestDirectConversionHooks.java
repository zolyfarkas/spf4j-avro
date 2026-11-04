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
package org.spf4j.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericDatumReader;
import org.spf4j.avro.generic.GenericDatumWriter;
import org.spf4j.avro.generic.GenericRecord;
import org.spf4j.avro.io.Decoder;
import org.spf4j.avro.io.DecoderFactory;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.EncoderFactory;
import org.spf4j.avro.util.Utf8;
import org.junit.Test;

/**
 * Pins the direct conversion hook contract (stage P1): a {@link Conversion}
 * implementing {@link Conversion#tryDirectDecode} /
 * {@link Conversion#tryDirectEncode} is invoked by
 * {@link GenericDatumReader} / {@link GenericDatumWriter} in preference to
 * the default read-then-convert / convert-then-write path, and receives a
 * {@link DatumContext} carrying the datum model performing the IO.
 *
 * <p>
 * The stub conversion rides on the built-in {@code uuid} logical type (a
 * string schema) and marks every value it touches directly with a
 * {@code "direct:"} prefix; its default-path conversion
 * ({@link Conversion#fromCharSequence} / {@link Conversion#toCharSequence})
 * uses a distinguishable {@code "converted:"} prefix, so the tests can tell
 * which path produced a value.
 * </p>
 */
public class TestDirectConversionHooks {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f\",\"type\":"
          + "{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}");

  /** A conversion whose direct hooks are distinguishable from its default path. */
  public static class StubConversion extends Conversion<String> {
    final AtomicInteger directDecodeCalls = new AtomicInteger();
    final AtomicInteger directEncodeCalls = new AtomicInteger();
    final AtomicReference<DatumContext> lastDecodeContext = new AtomicReference<>();
    final AtomicReference<DatumContext> lastEncodeContext = new AtomicReference<>();

    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "uuid";
    }

    @Override
    public Optional<String> tryDirectDecode(Decoder dec, Schema schema, DatumContext context) throws IOException {
      directDecodeCalls.incrementAndGet();
      lastDecodeContext.set(context);
      return Optional.of("direct:" + dec.readString());
    }

    @Override
    public boolean tryDirectEncode(String object, Encoder enc, Schema schema, DatumContext context)
        throws IOException {
      directEncodeCalls.incrementAndGet();
      lastEncodeContext.set(context);
      enc.writeString("direct:" + object);
      return true;
    }

    @Override
    public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return "converted:" + value;
    }

    @Override
    public CharSequence toCharSequence(String value, Schema schema, LogicalType type) {
      return "converted:" + value;
    }
  }

  private static byte[] writeRaw(String value) throws IOException {
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("f", new Utf8(value));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().binaryEncoder(bos, null);
    new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, enc);
    enc.flush();
    return bos.toByteArray();
  }

  @Test
  public void testDirectDecodePreferredByGenericDatumReader() throws IOException {
    StubConversion stub = new StubConversion();
    GenericData data = new GenericData();
    data.setFastReaderEnabled(false);
    data.addLogicalTypeConversion(stub);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA, SCHEMA, data);
    GenericRecord rec = reader.read(null, DecoderFactory.get().binaryDecoder(writeRaw("hello"), null));
    assertEquals("direct:hello", rec.get("f"));
    assertEquals(1, stub.directDecodeCalls.get());
    assertSame(data, stub.lastDecodeContext.get().getData());
  }

  @Test
  public void testDirectDecodePreferredByFastReader() throws IOException {
    StubConversion stub = new StubConversion();
    GenericData data = new GenericData();
    data.setFastReaderEnabled(true);
    data.addLogicalTypeConversion(stub);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA, SCHEMA, data);
    GenericRecord rec = reader.read(null, DecoderFactory.get().binaryDecoder(writeRaw("hello"), null));
    assertEquals("direct:hello", rec.get("f"));
    assertEquals(1, stub.directDecodeCalls.get());
    assertSame(data, stub.lastDecodeContext.get().getData());
  }

  @Test
  public void testDirectEncodePreferredByGenericDatumWriter() throws IOException {
    StubConversion stub = new StubConversion();
    GenericData data = new GenericData();
    data.addLogicalTypeConversion(stub);
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("f", "hello");
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().binaryEncoder(bos, null);
    new GenericDatumWriter<GenericRecord>(SCHEMA, data).write(record, enc);
    enc.flush();
    assertEquals(1, stub.directEncodeCalls.get());
    assertSame(data, stub.lastEncodeContext.get().getData());
    // and the bytes on the wire are the direct encoding, not the converted one
    GenericRecord readBack = new GenericDatumReader<GenericRecord>(SCHEMA)
        .read(null, DecoderFactory.get().binaryDecoder(bos.toByteArray(), null));
    assertEquals("direct:hello", readBack.get("f").toString());
  }

  @Test
  public void testDefaultPathWhenDirectDecodeDeclines() throws IOException {
    // a conversion that implements only the default path (the base-class
    // hooks decline) still converts exactly as before the hooks existed
    GenericData data = new GenericData();
    data.setFastReaderEnabled(false);
    data.addLogicalTypeConversion(new StubConversion() {
      @Override
      public Optional<String> tryDirectDecode(Decoder dec, Schema schema, DatumContext context) {
        return Optional.empty();
      }
    });
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA, SCHEMA, data);
    GenericRecord rec = reader.read(null, DecoderFactory.get().binaryDecoder(writeRaw("hello"), null));
    assertEquals("converted:hello", rec.get("f"));
  }
}
