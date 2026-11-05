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

import org.spf4j.avro.data.TimeConversions;
import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericEnumSymbol;
import org.spf4j.avro.generic.GenericFixed;
import org.spf4j.avro.generic.IndexedRecord;
import org.spf4j.avro.io.BinaryDecoder;
import org.spf4j.avro.io.BinaryEncoder;
import org.spf4j.avro.io.Decoder;
import org.spf4j.avro.io.DecoderFactory;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.EncoderFactory;
import org.spf4j.avro.io.JsonExtensionDecoder;
import org.spf4j.avro.io.JsonExtensionEncoder;
import org.spf4j.avro.util.TimePeriod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conversions {

  private static final Logger LOG = LoggerFactory.getLogger(Conversions.class);

  private static final Map<String, Conversion<?>> DEFAULT_CONVERSIONS;

  static {
    Map<String, Conversion<?>> map = new LinkedHashMap<>();
    registerConversion(map, new DecimalConversion());
    registerConversion(map, new BigDecimalConversion());
    registerConversion(map, new DurationConversion());
    registerConversion(map, new UUIDConversion());
    registerConversion(map, new TimeConversions.DateConversion());
    registerConversion(map, new TimeConversions.TimeMillisConversion());
    registerConversion(map, new TimeConversions.TimeMicrosConversion());
    registerConversion(map, new TimeConversions.TimestampMillisConversion());
    registerConversion(map, new TimeConversions.TimestampMicrosConversion());
    registerConversion(map, new TimeConversions.TimestampNanosConversion());
    registerConversion(map, new TimeConversions.LocalTimestampMillisConversion());
    registerConversion(map, new TimeConversions.LocalTimestampMicrosConversion());
    registerConversion(map, new TimeConversions.LocalTimestampNanosConversion());
    for (Conversion<?> conversion : ServiceLoader.load(Conversion.class)) {
      registerConversion(map, conversion);
    }
    DEFAULT_CONVERSIONS = map;
  }

  /**
   * The default conversions: this class's and {@link TimeConversions}'
   * built-in conversions, plus every {@link Conversion} provider discovered
   * through {@link ServiceLoader} (that is, named in
   * {@code META-INF/services/org.spf4j.avro.Conversion}).
   *
   * <p>
   * Conversions are keyed by logical type name. Built-ins are registered
   * first and ServiceLoader providers afterwards, so on a logical-type-name
   * collision a provider overwrites the built-in it replaces (and a later
   * provider overwrites an earlier one) -- the fork's registration order,
   * and the same put-by-name overwrite {@code GenericData} applies in its
   * own, separate ServiceLoader discovery.
   *
   * @return a copy of the default conversions, keyed by logical type name;
   *         mutating the returned map does not affect the defaults seen by
   *         other callers
   */
  public static Map<String, Conversion<?>> defaultConversions() {
    return new LinkedHashMap<>(DEFAULT_CONVERSIONS);
  }

  private static void registerConversion(final Map<String, Conversion<?>> map, final Conversion<?> conversion) {
    Conversion<?> existing = map.put(conversion.getLogicalTypeName(), conversion);
    if (existing != null) {
      LOG.info("Conversion: {} is being overwritten with {}", existing, conversion);
    }
  }

  public static class UUIDConversion extends Conversion<UUID> {
    @Override
    public Class<UUID> getConvertedType() {
      return UUID.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    }

    @Override
    public String getLogicalTypeName() {
      return "uuid";
    }

    @Override
    public UUID fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return UUID.fromString(value.toString());
    }

    @Override
    public CharSequence toCharSequence(UUID value, Schema schema, LogicalType type) {
      return value.toString();
    }

    @Override
    public UUID fromFixed(final GenericFixed value, final Schema schema, final LogicalType type) {
      ByteBuffer buffer = ByteBuffer.wrap(value.bytes());
      long mostSigBits = buffer.getLong();
      long leastSigBits = buffer.getLong();
      return new UUID(mostSigBits, leastSigBits);
    }

    @Override
    public GenericFixed toFixed(final UUID value, final Schema schema, final LogicalType type) {
      ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
      buffer.putLong(value.getMostSignificantBits());
      buffer.putLong(value.getLeastSignificantBits());
      return new GenericData.Fixed(schema, buffer.array());
    }
  }

  public static class DecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public Schema getRecommendedSchema() {
      throw new UnsupportedOperationException("No recommended schema for decimal (scale is required)");
    }

    @Override
    public String getLogicalTypeName() {
      return "decimal";
    }

    /**
     * Direct decode of the natural (extended) JSON form: when the decoder
     * is an extension decoder, read the value as the JSON number it is
     * written as, adjusted to the schema's scale. Without this, a natural
     * JSON number goes through {@code readBytes}, whose scale-prefixed
     * layout {@link #fromBytes} would misread as the unscaled value.
     * (The fork's {@code Decimal2Converter} direct path, applied to the
     * standard decimal logical type.)
     */
    @Override
    public Optional<BigDecimal> tryDirectDecode(final Decoder dec, final Schema schema) throws IOException {
      if (dec instanceof JsonExtensionDecoder) {
        BigDecimal bigD = ((JsonExtensionDecoder) dec).readBigDecimal(schema);
        if (bigD != null) {
          int scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
          if (bigD.scale() == scale) {
            return Optional.of(bigD);
          }
          try {
            return Optional.of(bigD.setScale(scale, RoundingMode.UNNECESSARY));
          } catch (ArithmeticException e) {
            throw new AvroTypeException(
                "Cannot decode decimal " + bigD + " as scale " + scale + " without rounding", e);
          }
        }
      }
      return Optional.empty();
    }

    /**
     * Direct encode of the natural (extended) JSON form: when the encoder
     * is an extension encoder, write the value as a raw JSON number
     * through it instead of in the standard bytes-as-string form. The
     * value is first {@link #validate(LogicalTypes.Decimal, BigDecimal)
     * validated} -- adjusted to the schema's scale and checked against
     * its precision -- exactly as the default {@link #toBytes} path does,
     * so the natural form carries the same value the standard form
     * would. (The fork's {@code Decimal2Converter} direct-encode pattern,
     * applied to the standard decimal logical type.)
     */
    @Override
    public boolean tryDirectEncode(final BigDecimal object, final Encoder enc, final Schema schema) throws IOException {
      if (enc instanceof JsonExtensionEncoder) {
        BigDecimal value = validate((LogicalTypes.Decimal) schema.getLogicalType(), object);
        ((JsonExtensionEncoder) enc).writeDecimal(value, schema);
        return true;
      }
      return false;
    }

    @Override
    public BigDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      // always copy the bytes out because BigInteger has no offset/length ctor
      byte[] bytes = new byte[value.remaining()];
      value.duplicate().get(bytes);
      return new BigDecimal(new BigInteger(bytes), scale);
    }

    @Override
    public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
      value = validate((LogicalTypes.Decimal) type, value);

      return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    @Override
    public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      int scale = ((LogicalTypes.Decimal) type).getScale();
      return new BigDecimal(new BigInteger(value.bytes()), scale);
    }

    @Override
    public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
      value = validate((LogicalTypes.Decimal) type, value);

      byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = value.unscaledValue().toByteArray();
      byte[] bytes = new byte[schema.getFixedSize()];
      int unscaledLength = unscaled.length;
      int offset = bytes.length - unscaledLength;

      // Fill the front with the filler and copy the unscaled value into the remainder
      Arrays.fill(bytes, 0, offset, fillByte);
      System.arraycopy(unscaled, 0, bytes, offset, unscaledLength);

      return new GenericData.Fixed(schema, bytes);
    }

    private static BigDecimal validate(final LogicalTypes.Decimal decimal, BigDecimal value) {
      final int scale = decimal.getScale();
      final int valueScale = value.scale();

      boolean scaleAdjusted = false;
      if (valueScale != scale) {
        try {
          value = value.setScale(scale, RoundingMode.UNNECESSARY);
          scaleAdjusted = true;
        } catch (ArithmeticException aex) {
          throw new AvroTypeException(
              "Cannot encode decimal with scale " + valueScale + " as scale " + scale + " without rounding");
        }
      }

      int precision = decimal.getPrecision();
      int valuePrecision = value.precision();
      if (valuePrecision > precision) {
        if (scaleAdjusted) {
          throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision "
              + precision + ". This is after safely adjusting scale from " + valueScale + " to required " + scale);
        } else {
          throw new AvroTypeException(
              "Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision);
        }
      }

      return value;
    }
  }

  public static class BigDecimalConversion extends Conversion<BigDecimal> {

    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "big-decimal";
    }

    @Override
    public BigDecimal fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value.array(), null);

      try {
        BigInteger bg = null;
        ByteBuffer buffer = decoder.readBytes(null);
        byte[] array = buffer.array();
        if (array.length > 0) {
          bg = new BigInteger(array);
        }

        int scale = decoder.readInt();
        return new BigDecimal(bg, scale);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public ByteBuffer toBytes(final BigDecimal value, final Schema schema, final LogicalType type) {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        BigInteger unscaledValue = value.unscaledValue();
        if (unscaledValue != null) {
          encoder.writeBytes(unscaledValue.toByteArray());
        } else {
          encoder.writeBytes(new byte[] {});
        }
        encoder.writeInt(value.scale());
        encoder.flush();
        return ByteBuffer.wrap(out.toByteArray());

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.bigDecimal().addToSchema(Schema.create(Schema.Type.BYTES));
    }
  }

  public static class DurationConversion extends Conversion<TimePeriod> {
    @Override
    public Class<TimePeriod> getConvertedType() {
      return TimePeriod.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "duration";
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.duration().addToSchema(Schema.createFixed("time.Duration",
          "A 12-byte byte array encoding a duration in months, days and milliseconds.", null, 12));
    }

    @Override
    public TimePeriod fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      IntBuffer buffer = ByteBuffer.wrap(value.bytes()).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      long months = Integer.toUnsignedLong(buffer.get());
      long days = Integer.toUnsignedLong(buffer.get());
      long millis = Integer.toUnsignedLong(buffer.get());
      return TimePeriod.of(months, days, millis);
    }

    @Override
    public GenericFixed toFixed(TimePeriod value, Schema schema, LogicalType type) {
      ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
      IntBuffer intBuffer = buffer.asIntBuffer();
      intBuffer.put((int) value.getMonths());
      intBuffer.put((int) value.getDays());
      intBuffer.put((int) value.getMillis());
      return new GenericData.Fixed(schema, buffer.array());
    }
  }

  /**
   * Convert an underlying representation of a logical type (such as a ByteBuffer)
   * to a higher level object (such as a BigDecimal).
   *
   * @param datum      The object to be converted.
   * @param schema     The schema of datum. Cannot be null if datum is not null.
   * @param type       The {@link org.spf4j.avro.LogicalType} of datum. Cannot be
   *                   null if datum is not null.
   * @param conversion The tool used to finish the conversion. Cannot be null if
   *                   datum is not null.
   * @return The result object, which is a high level object of the logical type.
   *         The null datum always converts to a null value.
   * @throws IllegalArgumentException if datum is not null, but schema, type or
   *                                  conversion is.
   */
  public static Object convertToLogicalType(Object datum, Schema schema, LogicalType type, Conversion<?> conversion) {
    if (datum == null) {
      return null;
    }

    if (schema == null || type == null || conversion == null) {
      throw new IllegalArgumentException("Parameters cannot be null! Parameter values:"
          + Arrays.deepToString(new Object[] { datum, schema, type, conversion }));
    }

    try {
      switch (schema.getType()) {
      case RECORD:
        return conversion.fromRecord((IndexedRecord) datum, schema, type);
      case ENUM:
        return conversion.fromEnumSymbol((GenericEnumSymbol<?>) datum, schema, type);
      case ARRAY:
        return conversion.fromArray((Collection<?>) datum, schema, type);
      case MAP:
        return conversion.fromMap((Map<?, ?>) datum, schema, type);
      case FIXED:
        return conversion.fromFixed((GenericFixed) datum, schema, type);
      case STRING:
        return conversion.fromCharSequence((CharSequence) datum, schema, type);
      case BYTES:
        return conversion.fromBytes((ByteBuffer) datum, schema, type);
      case INT:
        return conversion.fromInt((Integer) datum, schema, type);
      case LONG:
        return conversion.fromLong((Long) datum, schema, type);
      case FLOAT:
        return conversion.fromFloat((Float) datum, schema, type);
      case DOUBLE:
        return conversion.fromDouble((Double) datum, schema, type);
      case BOOLEAN:
        return conversion.fromBoolean((Boolean) datum, schema, type);
      }
      return datum;
    } catch (ClassCastException e) {
      throw new AvroRuntimeException(
          "Cannot convert " + datum + ':' + datum.getClass().getSimpleName() + ": expected generic type", e);
    }
  }

  /**
   * Convert a high level representation of a logical type (such as a BigDecimal)
   * to its underlying representation object (such as a ByteBuffer)
   *
   * @param datum      The object to be converted.
   * @param schema     The schema of datum. Cannot be null if datum is not null.
   * @param type       The {@link org.spf4j.avro.LogicalType} of datum. Cannot be
   *                   null if datum is not null.
   * @param conversion The tool used to finish the conversion. Cannot be null if
   *                   datum is not null.
   * @return The result object, which is an underlying representation object of
   *         the logical type. If the input param datum is null, a null value will
   *         be returned.
   * @throws IllegalArgumentException if datum is not null, but schema, type or
   *                                  conversion is.
   */
  public static <T> Object convertToRawType(Object datum, Schema schema, LogicalType type, Conversion<T> conversion) {
    if (datum == null) {
      return null;
    }

    if (schema == null || type == null || conversion == null) {
      throw new IllegalArgumentException("Parameters cannot be null! Parameter values:"
          + Arrays.deepToString(new Object[] { datum, schema, type, conversion }));
    }

    try {
      Class<T> fromClass = conversion.getConvertedType();
      switch (schema.getType()) {
      case RECORD:
        return conversion.toRecord(fromClass.cast(datum), schema, type);
      case ENUM:
        return conversion.toEnumSymbol(fromClass.cast(datum), schema, type);
      case ARRAY:
        return conversion.toArray(fromClass.cast(datum), schema, type);
      case MAP:
        return conversion.toMap(fromClass.cast(datum), schema, type);
      case FIXED:
        return conversion.toFixed(fromClass.cast(datum), schema, type);
      case STRING:
        return conversion.toCharSequence(fromClass.cast(datum), schema, type);
      case BYTES:
        return conversion.toBytes(fromClass.cast(datum), schema, type);
      case INT:
        return conversion.toInt(fromClass.cast(datum), schema, type);
      case LONG:
        return conversion.toLong(fromClass.cast(datum), schema, type);
      case FLOAT:
        return conversion.toFloat(fromClass.cast(datum), schema, type);
      case DOUBLE:
        return conversion.toDouble(fromClass.cast(datum), schema, type);
      case BOOLEAN:
        return conversion.toBoolean(fromClass.cast(datum), schema, type);
      }
      return datum;
    } catch (ClassCastException e) {
      throw new AvroRuntimeException(
          "Cannot convert " + datum + ':' + datum.getClass().getSimpleName() + ": expected logical type", e);
    }
  }

}
