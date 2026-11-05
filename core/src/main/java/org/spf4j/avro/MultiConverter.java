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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.spf4j.avro.generic.GenericEnumSymbol;
import org.spf4j.avro.generic.GenericFixed;
import org.spf4j.avro.generic.IndexedRecord;
import org.spf4j.avro.io.Decoder;
import org.spf4j.avro.io.Encoder;

/**
 * A conversion adapter that switches between delegate conversions based on
 * the logical type instance, as chosen by a {@link Chooser}. Every
 * conversion method -- including the direct encode/decode hooks -- is
 * forwarded to the chosen delegate.
 *
 * <p>
 * Ported from the spf4j fork of Avro; the fork's null-tolerant
 * {@code util.Optional} return is expressed with {@link java.util.Optional}
 * here, as in {@link Conversion#tryDirectDecode(Decoder, Schema)}.
 * </p>
 *
 * @param <T> a Java type that generic data is converted to
 */
public final class MultiConverter<T> extends Conversion<T> {

  /** Selects which delegate conversion applies to a logical type. */
  public interface Chooser {
    int chose(LogicalType type);
  }

  private final Conversion<T>[] conversions;

  private final Chooser chooser;

  @SafeVarargs
  public MultiConverter(final Chooser chooser, final Conversion<T>... conversions) {
    this.conversions = conversions;
    this.chooser = chooser;
  }

  private Conversion<T> choose(final LogicalType type) {
    return conversions[chooser.chose(type)];
  }

  @Override
  public Class<T> getConvertedType() {
    return conversions[0].getConvertedType();
  }

  @Override
  public String getLogicalTypeName() {
    return conversions[0].getLogicalTypeName();
  }

  @Override
  public Optional<T> tryDirectDecode(final Decoder dec, final Schema schema) throws IOException {
    return choose(schema.getLogicalType()).tryDirectDecode(dec, schema);
  }

  @Override
  public Optional<T> tryDirectDecode(final Decoder dec, final Schema schema,
      final DatumContext context) throws IOException {
    return choose(schema.getLogicalType()).tryDirectDecode(dec, schema, context);
  }

  @Override
  public boolean tryDirectEncode(final T object, final Encoder enc, final Schema schema) throws IOException {
    return choose(schema.getLogicalType()).tryDirectEncode(object, enc, schema);
  }

  @Override
  public boolean tryDirectEncode(final T object, final Encoder enc, final Schema schema,
      final DatumContext context) throws IOException {
    return choose(schema.getLogicalType()).tryDirectEncode(object, enc, schema, context);
  }

  @Override
  public String adjustAndSetValue(final String varName, final String valParamName) {
    return conversions[0].adjustAndSetValue(varName, valParamName);
  }

  @Override
  public T fromBoolean(final Boolean value, final Schema schema, final LogicalType type) {
    return choose(type).fromBoolean(value, schema, type);
  }

  @Override
  public T fromInt(final Integer value, final Schema schema, final LogicalType type) {
    return choose(type).fromInt(value, schema, type);
  }

  @Override
  public T fromLong(final Long value, final Schema schema, final LogicalType type) {
    return choose(type).fromLong(value, schema, type);
  }

  @Override
  public T fromFloat(final Float value, final Schema schema, final LogicalType type) {
    return choose(type).fromFloat(value, schema, type);
  }

  @Override
  public T fromDouble(final Double value, final Schema schema, final LogicalType type) {
    return choose(type).fromDouble(value, schema, type);
  }

  @Override
  public T fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
    return choose(type).fromCharSequence(value, schema, type);
  }

  @Override
  public T fromEnumSymbol(final GenericEnumSymbol<?> value, final Schema schema, final LogicalType type) {
    return choose(type).fromEnumSymbol(value, schema, type);
  }

  @Override
  public T fromFixed(final GenericFixed value, final Schema schema, final LogicalType type) {
    return choose(type).fromFixed(value, schema, type);
  }

  @Override
  public T fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {
    return choose(type).fromBytes(value, schema, type);
  }

  @Override
  public T fromArray(final Collection<?> value, final Schema schema, final LogicalType type) {
    return choose(type).fromArray(value, schema, type);
  }

  @Override
  public T fromMap(final Map<?, ?> value, final Schema schema, final LogicalType type) {
    return choose(type).fromMap(value, schema, type);
  }

  @Override
  public T fromRecord(final IndexedRecord value, final Schema schema, final LogicalType type) {
    return choose(type).fromRecord(value, schema, type);
  }

  @Override
  public Boolean toBoolean(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toBoolean(value, schema, type);
  }

  @Override
  public Integer toInt(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toInt(value, schema, type);
  }

  @Override
  public Long toLong(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toLong(value, schema, type);
  }

  @Override
  public Float toFloat(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toFloat(value, schema, type);
  }

  @Override
  public Double toDouble(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toDouble(value, schema, type);
  }

  @Override
  public CharSequence toCharSequence(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toCharSequence(value, schema, type);
  }

  @Override
  public GenericEnumSymbol<?> toEnumSymbol(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toEnumSymbol(value, schema, type);
  }

  @Override
  public GenericFixed toFixed(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toFixed(value, schema, type);
  }

  @Override
  public ByteBuffer toBytes(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toBytes(value, schema, type);
  }

  @Override
  public Collection<?> toArray(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toArray(value, schema, type);
  }

  @Override
  public Map<?, ?> toMap(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toMap(value, schema, type);
  }

  @Override
  public IndexedRecord toRecord(final T value, final Schema schema, final LogicalType type) {
    return choose(type).toRecord(value, schema, type);
  }

}
