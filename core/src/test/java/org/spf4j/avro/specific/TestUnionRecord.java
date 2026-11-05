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

package org.spf4j.avro.specific;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.spf4j.avro.message.BinaryMessageDecoder;
import org.spf4j.avro.message.BinaryMessageEncoder;
import org.spf4j.avro.message.SchemaStore;

@SuppressFBWarnings(value = "IMC_IMMATURE_CLASS_BAD_SERIALVERSIONUID",
    justification = "checked-in generated record: the serialVersionUID is as emitted by the specific compiler;"
        + "the class's real serialized form is the Avro binary encoding, and Java serialization is"
        + "incidental")
@SuppressWarnings("all")
@AvroGenerated
public class TestUnionRecord extends SpecificRecordBase implements SpecificRecord {
  private static final long serialVersionUID = -3829374192747523457L;

  public static final org.spf4j.avro.Schema SCHEMA$ = org.spf4j.avro.JsonSchemaParser.parseInternal(
      "{\"type\":\"record\",\"name\":\"TestUnionRecord\",\"namespace\":\"org.spf4j.avro.specific\",\"fields\":[{\"name\":\"amount\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":31,\"scale\":8}],\"default\":null}]}");

  public static org.spf4j.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  private static final SpecificData MODEL$ = new SpecificData();
  static {
    MODEL$.addLogicalTypeConversion(new org.spf4j.avro.Conversions.DecimalConversion());
  }

  private static final BinaryMessageEncoder<TestUnionRecord> ENCODER = new BinaryMessageEncoder<TestUnionRecord>(MODEL$,
      SCHEMA$);

  private static final BinaryMessageDecoder<TestUnionRecord> DECODER = new BinaryMessageDecoder<TestUnionRecord>(MODEL$,
      SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   *
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<TestUnionRecord> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   *
   * @return the message decoder used by this class
   */
  @SuppressFBWarnings(value = "MS_EXPOSE_REP",
      justification = "returns the record class's shared datum reader; the reader is configuration-only and shared"
          + "by all instances of the generated record by design")
  public static BinaryMessageDecoder<TestUnionRecord> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the
   * specified {@link SchemaStore}.
   *
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given
   *         SchemaStore
   */
  public static BinaryMessageDecoder<TestUnionRecord> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<TestUnionRecord>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this TestUnionRecord to a ByteBuffer.
   *
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a TestUnionRecord from a ByteBuffer.
   *
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a TestUnionRecord instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into
   *                             an instance of this class
   */
  public static TestUnionRecord fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.math.BigDecimal amount;

  /**
   * Default constructor. Note that this does not initialize fields to their
   * default values from the schema. If that is desired then one should use
   * <code>newBuilder()</code>.
   */
  public TestUnionRecord() {
  }

  /**
   * All-args constructor.
   *
   * @param amount The new value for amount
   */
  public TestUnionRecord(java.math.BigDecimal amount) {
    this.amount = amount;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
          + "and datum objects are shared carriers by design, not defensive-copy value objects")
  @Override
  public SpecificData getSpecificData() {
    return MODEL$;
  }

  @Override
  public org.spf4j.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  @Override
  public Object get(int field$) {
    switch (field$) {
    case 0:
      return amount;
    default:
      throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader. Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0:
      amount = (java.math.BigDecimal) value$;
      break;
    default:
      throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'amount' field.
   *
   * @return The value of the 'amount' field.
   */
  public java.math.BigDecimal getAmount() {
    return amount;
  }

  /**
   * Sets the value of the 'amount' field.
   *
   * @param value the value to set.
   */
  public void setAmount(java.math.BigDecimal value) {
    this.amount = value;
  }

  /**
   * Creates a new TestUnionRecord RecordBuilder.
   *
   * @return A new TestUnionRecord RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new TestUnionRecord RecordBuilder by copying an existing Builder.
   *
   * @param other The existing builder to copy.
   * @return A new TestUnionRecord RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new TestUnionRecord RecordBuilder by copying an existing
   * TestUnionRecord instance.
   *
   * @param other The existing instance to copy.
   * @return A new TestUnionRecord RecordBuilder
   */
  public static Builder newBuilder(TestUnionRecord other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for TestUnionRecord instances.
   */
  @AvroGenerated
  public static class Builder extends SpecificRecordBuilderBase<TestUnionRecord>
      implements org.spf4j.avro.data.RecordBuilder<TestUnionRecord> {

    private java.math.BigDecimal amount;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     *
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.amount)) {
        this.amount = data().deepCopy(fields()[0].schema(), other.amount);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
    }

    /**
     * Creates a Builder by copying an existing TestUnionRecord instance
     *
     * @param other The existing instance to copy.
     */
    private Builder(TestUnionRecord other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.amount)) {
        this.amount = data().deepCopy(fields()[0].schema(), other.amount);
        fieldSetFlags()[0] = true;
      }
    }

    /**
     * Gets the value of the 'amount' field.
     *
     * @return The value.
     */
    public java.math.BigDecimal getAmount() {
      return amount;
    }

    /**
     * Sets the value of the 'amount' field.
     *
     * @param value The value of 'amount'.
     * @return This builder.
     */
    public Builder setAmount(java.math.BigDecimal value) {
      validate(fields()[0], value);
      this.amount = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'amount' field has been set.
     *
     * @return True if the 'amount' field has been set, false otherwise.
     */
    public boolean hasAmount() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'amount' field.
     *
     * @return This builder.
     */
    public Builder clearAmount() {
      amount = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @SuppressFBWarnings(value = "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS",
        justification = "this API's documented error model is unchecked: exceptions are wrapped in"
            + "AvroRuntimeException at the boundary so callers program against the unchecked contract")
    @Override
    @SuppressWarnings("unchecked")
    public TestUnionRecord build() {
      try {
        TestUnionRecord record = new TestUnionRecord();
        record.amount = fieldSetFlags()[0] ? this.amount : (java.math.BigDecimal) defaultValue(fields()[0]);
        return record;
      } catch (org.spf4j.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.spf4j.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.spf4j.avro.io.DatumWriter<TestUnionRecord> WRITER$ = (org.spf4j.avro.io.DatumWriter<TestUnionRecord>) MODEL$
      .createDatumWriter(SCHEMA$);

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.spf4j.avro.io.DatumReader<TestUnionRecord> READER$ = (org.spf4j.avro.io.DatumReader<TestUnionRecord>) MODEL$
      .createDatumReader(SCHEMA$);

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
