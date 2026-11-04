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
import java.util.ServiceLoader;

import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericEnumSymbol;
import org.spf4j.avro.generic.GenericFixed;
import org.spf4j.avro.generic.IndexedRecord;
import org.spf4j.avro.io.Decoder;
import org.spf4j.avro.io.Encoder;

/**
 * Conversion between generic and logical type instances.
 * <p>
 * Instances of this class can be added to GenericData to convert a logical type
 * to a particular representation. This can be done manually, using
 * {@link GenericData#addLogicalTypeConversion(Conversion)}, or automatically.
 * This last option uses the Java {@link ServiceLoader}, and requires the
 * implementation to be a public class with a public no-arg constructor, be
 * named in a file called {@code /META-INF/services/org.spf4j.avro.Conversion},
 * and both must available in the classpath.</li>
 * <p>
 * Implementations must provide:
 * <ul>
 * <li>{@link #getConvertedType()}: get the Java class used for the logical
 * type</li>
 * <li>{@link #getLogicalTypeName()}: get the logical type this implements</li>
 * </ul>
 * <p>
 * Subclasses must also override the conversion methods for Avro's base types
 * that are valid for the logical type, or else risk causing
 * {@code UnsupportedOperationException} at runtime.
 * <p>
 * Optionally, use {@link #getRecommendedSchema()} to provide a Schema that will
 * be used when generating a Schema for the class. This is useful when using
 * {@code ReflectData} or {@code ProtobufData}, for example.
 *
 * @param <T> a Java type that can represent the named logical type
 * @see ServiceLoader
 */
@SuppressWarnings("unused")
public abstract class Conversion<T> {

  /**
   * Return the Java class representing the logical type.
   *
   * @return a Java class returned by from methods and accepted by to methods
   */
  public abstract Class<T> getConvertedType();

  /**
   * Return the logical type this class converts.
   *
   * @return a String logical type name
   */
  public abstract String getLogicalTypeName();

  /**
   * Certain logical types may require adjusting the code within the "setter"
   * methods to make sure the data that is set is properly formatted. This method
   * allows the Conversion to generate custom setter code if required.
   *
   * @param varName      the name of the variable holding the converted value
   * @param valParamName the name of the parameter with the new converted value
   * @return a String for the body of the setter method
   */
  public String adjustAndSetValue(String varName, String valParamName) {
    return varName + " = " + valParamName + ";";
  }

  public Schema getRecommendedSchema() {
    throw new UnsupportedOperationException("No recommended schema for " + getLogicalTypeName());
  }

  public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromBoolean is not supported for " + type.getName());
  }

  public T fromInt(Integer value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromInt is not supported for " + type.getName());
  }

  public T fromLong(Long value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromLong is not supported for " + type.getName());
  }

  public T fromFloat(Float value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromFloat is not supported for " + type.getName());
  }

  public T fromDouble(Double value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromDouble is not supported for " + type.getName());
  }

  public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromCharSequence is not supported for " + type.getName());
  }

  public T fromEnumSymbol(GenericEnumSymbol<?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromEnumSymbol is not supported for " + type.getName());
  }

  public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromFixed is not supported for " + type.getName());
  }

  public T fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromBytes is not supported for " + type.getName());
  }

  public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromArray is not supported for " + type.getName());
  }

  public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromMap is not supported for " + type.getName());
  }

  public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromRecord is not supported for " + type.getName());
  }

  public Boolean toBoolean(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toBoolean is not supported for " + type.getName());
  }

  public Integer toInt(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toInt is not supported for " + type.getName());
  }

  public Long toLong(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toLong is not supported for " + type.getName());
  }

  public Float toFloat(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toFloat is not supported for " + type.getName());
  }

  public Double toDouble(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toDouble is not supported for " + type.getName());
  }

  public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toCharSequence is not supported for " + type.getName());
  }

  public GenericEnumSymbol<?> toEnumSymbol(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toEnumSymbol is not supported for " + type.getName());
  }

  public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toFixed is not supported for " + type.getName());
  }

  public ByteBuffer toBytes(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toBytes is not supported for " + type.getName());
  }

  public Collection<?> toArray(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toArray is not supported for " + type.getName());
  }

  public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toMap is not supported for " + type.getName());
  }

  public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toRecord is not supported for " + type.getName());
  }

  // The direct encode/decode hooks below are ported from the spf4j fork of
  // Avro (where they returned the fork's null-tolerant util.Optional; here
  // they use java.util.Optional, so a direct decode cannot express a
  // present null value -- returning Optional.empty() falls back to the
  // default path, which handles null through the usual conversion).

  /**
   * Special encoding logic for a logical type. This allows a conversion to
   * take over encoding of a value entirely for a specific encoder -- for
   * example, writing a decimal as a raw JSON number through an extension
   * encoder instead of in the standard bytes-as-string form.
   *
   * <p>
   * Called by the datum writers before the default conversion path
   * ({@link #toBytes(Object, Schema, LogicalType)} and friends).
   * </p>
   *
   * @param object the value to encode
   * @param enc    the encoder to write to
   * @param schema the schema of the value, carrying the logical type
   * @return {@code true} if direct encoding was done (the value has been
   *         fully written to {@code enc}); {@code false} to let the caller
   *         use the default conversion path
   */
  public boolean tryDirectEncode(T object, final Encoder enc, final Schema schema) throws IOException {
    return false;
  }

  /**
   * Context-carrying form of
   * {@link #tryDirectEncode(Object, Encoder, Schema)}, invoked by the datum
   * writers. The default implementation delegates to the three-argument
   * form, so conversions written against that form keep working unchanged;
   * override this form when the encoding needs the IO context (see
   * {@link DatumContext}).
   *
   * @param context the IO context of the current datum operation
   * @return {@code true} if direct encoding was done
   */
  public boolean tryDirectEncode(T object, final Encoder enc, final Schema schema, final DatumContext context)
      throws IOException {
    return tryDirectEncode(object, enc, schema);
  }

  /**
   * Special decoding logic for a logical type. This allows a conversion to
   * take over decoding of a value entirely for a specific decoder -- for
   * example, reading a decimal from its natural JSON number form through
   * an extension decoder instead of from the standard bytes layout.
   *
   * <p>
   * Called by the datum readers before the default path (read the raw
   * value, then convert it with the {@code fromXxx} methods).
   * </p>
   *
   * @param dec    the decoder to read from (a resolving decoder when the
   *               read goes through schema resolution; extension decoder
   *               features of the underlying decoder are reachable through
   *               it, see
   *               {@link org.spf4j.avro.io.ResolvingDecoder})
   * @param schema the reader schema of the value, carrying the logical type
   * @return the directly decoded value, or {@link Optional#empty()} if no
   *         direct decode applies (the caller then uses the default path)
   */
  public Optional<T> tryDirectDecode(final Decoder dec, final Schema schema) throws IOException {
    return Optional.empty();
  }

  /**
   * Context-carrying form of {@link #tryDirectDecode(Decoder, Schema)},
   * invoked by the datum readers. The default implementation delegates to
   * the two-argument form, so conversions written against that form keep
   * working unchanged; override this form when the decoding needs the IO
   * context (see {@link DatumContext}).
   *
   * @param context the IO context of the current datum operation
   * @return the directly decoded value, or {@link Optional#empty()} for
   *         the default path
   */
  public Optional<T> tryDirectDecode(final Decoder dec, final Schema schema, final DatumContext context)
      throws IOException {
    return tryDirectDecode(dec, schema);
  }

}
