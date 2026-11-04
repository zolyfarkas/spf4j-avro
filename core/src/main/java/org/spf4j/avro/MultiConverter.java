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
public class MultiConverter<T> extends Conversion<T> {

  /** Selects which delegate conversion applies to a logical type. */
  public interface Chooser {
    int chose(LogicalType type);
  }

  private final Conversion<T>[] conversions;

  private final Chooser chooser;

  @SafeVarargs
  public MultiConverter(Chooser chooser, Conversion<T>... conversions) {
    this.conversions = conversions;
    this.chooser = chooser;
  }

  private Conversion<T> choose(LogicalType type) {
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
  public Optional<T> tryDirectDecode(Decoder dec, Schema schema) throws IOException {
    return choose(schema.getLogicalType()).tryDirectDecode(dec, schema);
  }

  @Override
  public Optional<T> tryDirectDecode(Decoder dec, Schema schema, DatumContext context) throws IOException {
    return choose(schema.getLogicalType()).tryDirectDecode(dec, schema, context);
  }

  @Override
  public boolean tryDirectEncode(T object, Encoder enc, Schema schema) throws IOException {
    return choose(schema.getLogicalType()).tryDirectEncode(object, enc, schema);
  }

  @Override
  public boolean tryDirectEncode(T object, Encoder enc, Schema schema, DatumContext context) throws IOException {
    return choose(schema.getLogicalType()).tryDirectEncode(object, enc, schema, context);
  }

  @Override
  public String adjustAndSetValue(String varName, String valParamName) {
    return conversions[0].adjustAndSetValue(varName, valParamName);
  }

  @Override
  public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
    return choose(type).fromBoolean(value, schema, type);
  }

  @Override
  public T fromInt(Integer value, Schema schema, LogicalType type) {
    return choose(type).fromInt(value, schema, type);
  }

  @Override
  public T fromLong(Long value, Schema schema, LogicalType type) {
    return choose(type).fromLong(value, schema, type);
  }

  @Override
  public T fromFloat(Float value, Schema schema, LogicalType type) {
    return choose(type).fromFloat(value, schema, type);
  }

  @Override
  public T fromDouble(Double value, Schema schema, LogicalType type) {
    return choose(type).fromDouble(value, schema, type);
  }

  @Override
  public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return choose(type).fromCharSequence(value, schema, type);
  }

  @Override
  public T fromEnumSymbol(GenericEnumSymbol<?> value, Schema schema, LogicalType type) {
    return choose(type).fromEnumSymbol(value, schema, type);
  }

  @Override
  public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
    return choose(type).fromFixed(value, schema, type);
  }

  @Override
  public T fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    return choose(type).fromBytes(value, schema, type);
  }

  @Override
  public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
    return choose(type).fromArray(value, schema, type);
  }

  @Override
  public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
    return choose(type).fromMap(value, schema, type);
  }

  @Override
  public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
    return choose(type).fromRecord(value, schema, type);
  }

  @Override
  public Boolean toBoolean(T value, Schema schema, LogicalType type) {
    return choose(type).toBoolean(value, schema, type);
  }

  @Override
  public Integer toInt(T value, Schema schema, LogicalType type) {
    return choose(type).toInt(value, schema, type);
  }

  @Override
  public Long toLong(T value, Schema schema, LogicalType type) {
    return choose(type).toLong(value, schema, type);
  }

  @Override
  public Float toFloat(T value, Schema schema, LogicalType type) {
    return choose(type).toFloat(value, schema, type);
  }

  @Override
  public Double toDouble(T value, Schema schema, LogicalType type) {
    return choose(type).toDouble(value, schema, type);
  }

  @Override
  public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
    return choose(type).toCharSequence(value, schema, type);
  }

  @Override
  public GenericEnumSymbol<?> toEnumSymbol(T value, Schema schema, LogicalType type) {
    return choose(type).toEnumSymbol(value, schema, type);
  }

  @Override
  public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
    return choose(type).toFixed(value, schema, type);
  }

  @Override
  public ByteBuffer toBytes(T value, Schema schema, LogicalType type) {
    return choose(type).toBytes(value, schema, type);
  }

  @Override
  public Collection<?> toArray(T value, Schema schema, LogicalType type) {
    return choose(type).toArray(value, schema, type);
  }

  @Override
  public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
    return choose(type).toMap(value, schema, type);
  }

  @Override
  public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
    return choose(type).toRecord(value, schema, type);
  }

}
