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
package org.spf4j.avro.generic;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.spf4j.avro.AvroMissingFieldException;
import org.spf4j.avro.AvroRuntimeException;
import org.spf4j.avro.AvroTypeException;
import org.spf4j.avro.Conversion;
import org.spf4j.avro.Conversions;
import org.spf4j.avro.JsonProperties;
import org.spf4j.avro.LogicalType;
import org.spf4j.avro.Schema;
import org.spf4j.avro.Schema.Field;
import org.spf4j.avro.SchemaResolver;
import org.spf4j.avro.Schema.Type;
import org.spf4j.avro.UnresolvedUnionException;
import org.spf4j.avro.io.BinaryData;
import org.spf4j.avro.io.BinaryDecoder;
import org.spf4j.avro.io.BinaryEncoder;
import org.spf4j.avro.io.DatumReader;
import org.spf4j.avro.io.DatumWriter;
import org.spf4j.avro.io.DecoderFactory;
import org.spf4j.avro.io.EncoderFactory;
import org.spf4j.avro.io.FastReaderBuilder;
import org.spf4j.avro.util.Utf8;
import org.spf4j.avro.util.internal.Accessor;
import org.spf4j.avro.generic.PrimitivesArrays.PrimitiveArray;

import com.fasterxml.jackson.databind.JsonNode;
import org.spf4j.avro.util.springframework.ConcurrentReferenceHashMap;

import static org.spf4j.avro.util.springframework.ConcurrentReferenceHashMap.ReferenceType.WEAK;

/**
 * Utilities for generic Java data. See {@link GenericRecordBuilder} for a
 * convenient way to build {@link GenericRecord} instances.
 *
 * @see GenericRecordBuilder
 */
@SuppressFBWarnings(value = "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY",
    justification = "the classes of this package form one cohesive unit mirroring the Avro type system: model,"
        + "builders, readers and writers are mutually referential by design")
public class GenericData {

  private static final GenericData INSTANCE = new GenericData();

  private static final Map<Class<?>, String> PRIMITIVE_DATUM_TYPES = new IdentityHashMap<>();
  static {
    PRIMITIVE_DATUM_TYPES.put(Integer.class, Type.INT.getName());
    PRIMITIVE_DATUM_TYPES.put(Long.class, Type.LONG.getName());
    PRIMITIVE_DATUM_TYPES.put(Float.class, Type.FLOAT.getName());
    PRIMITIVE_DATUM_TYPES.put(Double.class, Type.DOUBLE.getName());
    PRIMITIVE_DATUM_TYPES.put(Boolean.class, Type.BOOLEAN.getName());
    PRIMITIVE_DATUM_TYPES.put(String.class, Type.STRING.getName());
    PRIMITIVE_DATUM_TYPES.put(Utf8.class, Type.STRING.getName());
  }

  /** Used to specify the Java type for a string schema. */
  public enum StringType {
    CharSequence, String, Utf8
  };

  public static final String STRING_PROP = "avro.java.string";
  protected static final String STRING_TYPE_STRING = "String";

  private final ClassLoader classLoader;

  /**
   * Set the Java type to be used when reading this schema. Meaningful only only
   * string schemas and map schemas (for the keys).
   */
  public static void setStringType(Schema s, StringType stringType) {
    // Utf8 is the default and implements CharSequence, so we only need to add
    // a property when the type is String
    if (stringType == StringType.String)
      s.addProp(GenericData.STRING_PROP, GenericData.STRING_TYPE_STRING);
  }

  /** Return the singleton instance. */
  @SuppressFBWarnings(value = "MS_EXPOSE_REP",
      justification = "returns the shared default model instance; the configurable datum-model singleton is the"
          + "documented access pattern")
  public static GenericData get() {
    return INSTANCE;
  }

  /** For subclasses. Applications normally use {@link GenericData#get()}. */
  public GenericData() {
    this(null);
  }

  /** For subclasses. GenericData does not use a ClassLoader. */
  public GenericData(ClassLoader classLoader) {
    this.classLoader = (classLoader != null) ? classLoader : getClass().getClassLoader();
    loadConversions();
  }

  /** Return the class loader that's used (by subclasses). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
          + "and datum objects are shared carriers by design, not defensive-copy value objects")
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Use the Java 6 ServiceLoader to load conversions.
   *
   * @see #addLogicalTypeConversion(Conversion)
   */
  private void loadConversions() {
    for (Conversion<?> conversion : ServiceLoader.load(Conversion.class, classLoader)) {
      addLogicalTypeConversion(conversion);
    }
  }

  private final Map<String, Conversion<?>> conversions = new HashMap<>();

  private final Map<Class<?>, Map<String, Conversion<?>>> conversionsByClass = new IdentityHashMap<>();

  public Collection<Conversion<?>> getConversions() {
    return conversions.values();
  }

  /**
   * Registers the given conversion to be used when reading and writing with this
   * data model. Conversions can also be registered automatically, as documented
   * on the class {@link Conversion Conversion&lt;T&gt;}.
   *
   * @param conversion a logical type Conversion.
   */
  public void addLogicalTypeConversion(Conversion<?> conversion) {
    conversions.put(conversion.getLogicalTypeName(), conversion);
    Class<?> type = conversion.getConvertedType();
    Map<String, Conversion<?>> conversionsForClass = conversionsByClass.computeIfAbsent(type,
        k -> new LinkedHashMap<>());
    conversionsForClass.put(conversion.getLogicalTypeName(), conversion);
  }

  /**
   * Returns the first conversion found for the given class.
   *
   * <p>
   * The lookup is assignability-based, as in the spf4j avro fork: a
   * conversion whose converted type is a supertype of (or interface
   * implemented by) {@code datumClass} applies to it -- for example a
   * conversion with converted type {@code Object}, {@code Map} or
   * {@code Schema} applies to any datum class, a {@code HashMap} or a
   * {@code Schema} subclass respectively. An exact-class registration wins
   * over a supertype one; among supertype registrations the most specific
   * converted type wins.
   *
   * @param datumClass a Class
   * @return the first registered conversion for the class, or null
   */
  @SuppressWarnings("unchecked")
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass) {
    Map<String, Conversion<?>> exact = conversionsByClass.get(datumClass);
    if (exact != null) {
      return (Conversion<T>) exact.values().iterator().next();
    }
    Conversion<?> best = null;
    Class<?> bestType = null;
    for (Map.Entry<Class<?>, Map<String, Conversion<?>>> entry : conversionsByClass.entrySet()) {
      Class<?> convertedType = entry.getKey();
      if (convertedType.isAssignableFrom(datumClass)
          && (bestType == null || bestType.isAssignableFrom(convertedType))) {
        Conversion<?> candidate = entry.getValue().values().iterator().next();
        best = candidate;
        bestType = convertedType;
      }
    }
    return (Conversion<T>) best;
  }

  /**
   * Returns the conversion for the given class and logical type.
   *
   * <p>
   * The lookup is by logical type name with an assignability check, as in
   * the spf4j avro fork ({@code getConversionFor} there): the conversion
   * registered for the logical type applies when its converted type is
   * assignable from {@code datumClass}
   * ({@code convertedType.isAssignableFrom(datumClass)}), not only when the
   * datum's class is exactly the converted type. This is what the write
   * path ({@link GenericDatumWriter}) dispatches through: conversions
   * whose converted type is a supertype -- {@code Object} for {@code any},
   * {@code Map} for {@code json_record}, {@code Schema} for {@code avsc},
   * an interface such as {@code List} or {@code Temporal} -- were never
   * applied to subclass/implementation datums by the exact-class lookup
   * this replaces, so the raw value hit the writer and failed with a
   * {@code ClassCastException}. An exact-class registration for the same
   * logical type name still wins over a supertype one, and among supertype
   * registrations the most specific converted type wins. The read path is
   * unaffected: it looks conversions up by logical type only
   * ({@link #getConversionFor(LogicalType)}).
   *
   * @param datumClass  a Class
   * @param logicalType a LogicalType
   * @return the conversion for the class and logical type, or null
   */
  @SuppressWarnings("unchecked")
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass, LogicalType logicalType) {
    if (logicalType == null || datumClass == null) {
      return null;
    }
    String logicalTypeName = logicalType.getName();
    Map<String, Conversion<?>> exact = conversionsByClass.get(datumClass);
    if (exact != null) {
      Conversion<?> conversion = exact.get(logicalTypeName);
      if (conversion != null) {
        return (Conversion<T>) conversion;
      }
    }
    Conversion<?> byName = conversions.get(logicalTypeName);
    if (byName != null && byName.getConvertedType().isAssignableFrom(datumClass)) {
      return (Conversion<T>) byName;
    }
    Conversion<?> best = null;
    Class<?> bestType = null;
    for (Map.Entry<Class<?>, Map<String, Conversion<?>>> entry : conversionsByClass.entrySet()) {
      Class<?> convertedType = entry.getKey();
      if (convertedType.isAssignableFrom(datumClass)) {
        Conversion<?> candidate = entry.getValue().get(logicalTypeName);
        if (candidate != null && (bestType == null || bestType.isAssignableFrom(convertedType))) {
          best = candidate;
          bestType = convertedType;
        }
      }
    }
    return (Conversion<T>) best;
  }

  /**
   * Returns the Conversion for the given logical type.
   *
   * @param logicalType a logical type
   * @return the conversion for the logical type, or null
   */
  @SuppressWarnings("unchecked")
  public <T> Conversion<T> getConversionFor(LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    }
    return (Conversion<T>) conversions.get(logicalType.getName());
  }

  /**
   * The default schema resolver carried by this datum model (the IO-time
   * half of the library's resolver design), or {@code null} when none was
   * set. Conversions obtain it at use time through
   * {@link org.spf4j.avro.DatumContext#getEffectiveSchemaResolver(Schema)};
   * it is configured here, on the datum model instance, the same place
   * conversions are registered — never in static or JVM-global state.
   */
  private SchemaResolver schemaResolver;

  /**
   * Named schema resolvers registered on this instance, selected by the
   * {@code "resolver"} property a schema carries (as the {@code "any"}
   * logical type writes it). Instance-scoped: two datum models with
   * different registrations operate independently.
   */
  private final Map<String, SchemaResolver> namedSchemaResolvers = new LinkedHashMap<>();

  /** The default schema resolver of this datum model, or {@code null}. */
  public SchemaResolver getSchemaResolver() {
    return schemaResolver;
  }

  /**
   * Set (or clear, with {@code null}) the default schema resolver of this
   * datum model.
   *
   * @return this datum model, for chaining
   */
  public GenericData setSchemaResolver(final SchemaResolver resolver) {
    this.schemaResolver = resolver;
    return this;
  }

  /**
   * Register a schema resolver under {@code name} on this datum model. A
   * schema whose {@code "resolver"} property names it selects this resolver
   * at conversion time (see
   * {@link org.spf4j.avro.DatumContext#resolveEffectiveSchemaResolver(GenericData, Schema)}).
   * Registering under an existing name replaces the previous registration.
   *
   * @return this datum model, for chaining
   */
  public GenericData registerSchemaResolver(final String name, final SchemaResolver resolver) {
    namedSchemaResolvers.put(Objects.requireNonNull(name, "name"),
        Objects.requireNonNull(resolver, "resolver"));
    return this;
  }

  /**
   * The schema resolver registered under {@code name} on this datum model,
   * or {@code null} when no resolver was registered under that name.
   */
  public SchemaResolver getSchemaResolver(final String name) {
    return namedSchemaResolvers.get(name);
  }

  public static final String FAST_READER_PROP = "org.spf4j.avro.fastread";
  private boolean fastReaderEnabled = Boolean.parseBoolean(System.getProperty(FAST_READER_PROP, "true"));
  private FastReaderBuilder fastReaderBuilder = null;

  public GenericData setFastReaderEnabled(boolean flag) {
    this.fastReaderEnabled = flag;
    return this;
  }

  public boolean isFastReaderEnabled() {
    return fastReaderEnabled && FastReaderBuilder.isSupportedData(this);
  }

  public FastReaderBuilder getFastReaderBuilder() {
    if (fastReaderBuilder == null) {
      fastReaderBuilder = new FastReaderBuilder(this);
    }
    return this.fastReaderBuilder;
  }

  /**
   * Default implementation of {@link GenericRecord}. Note that this
   * implementation does not fill in default values for fields if they are not
   * specified; use {@link GenericRecordBuilder} in that case.
   *
   * @see GenericRecordBuilder
   */
  public static class Record implements GenericRecord, Comparable<Record> {
    private final Schema schema;
    private final Object[] values;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
        justification = "the caller's object is adopted by reference deliberately: the datum/model layer shares"
            + "schemas and configuration objects instead of defensively copying them on construction and"
            + "configuration paths")
    public Record(Schema schema) {
      if (schema == null || Type.RECORD != schema.getType())
        throw new AvroRuntimeException("Not a record schema: " + schema);
      this.schema = schema;
      this.values = new Object[schema.getFields().size()];
    }

    public Record(Record other, boolean deepCopy) {
      schema = other.schema;
      values = new Object[schema.getFields().size()];
      if (deepCopy) {
        for (int ii = 0; ii < values.length; ii++) {
          values[ii] = INSTANCE.deepCopy(schema.getFields().get(ii).schema(), other.values[ii]);
        }
      } else {
        System.arraycopy(other.values, 0, values, 0, other.values.length);
      }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
        justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
            + "and datum objects are shared carriers by design, not defensive-copy value objects")
    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void put(String key, Object value) {
      Schema.Field field = schema.getField(key);
      if (field == null) {
        throw new AvroRuntimeException("Not a valid schema field: " + key);
      }

      values[field.pos()] = value;
    }

    @Override
    public void put(int i, Object v) {
      values[i] = v;
    }

    @Override
    public Object get(String key) {
      Field field = schema.getField(key);
      if (field == null) {
        throw new AvroRuntimeException("Not a valid schema field: " + key);
      }
      return values[field.pos()];
    }

    @Override
    public Object get(int i) {
      return values[i];
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true; // identical object
      if (!(o instanceof Record))
        return false; // not a record
      Record that = (Record) o;
      if (!this.schema.equals(that.schema))
        return false; // not the same schema
      return GenericData.get().compare(this, that, schema, true) == 0;
    }

    @Override
    public int hashCode() {
      return GenericData.get().hashCode(this, schema);
    }

    @Override
    public int compareTo(Record that) {
      return GenericData.get().compare(this, that, schema);
    }

    @Override
    public String toString() {
      return GenericData.get().toString(this);
    }
  }

  public static abstract class AbstractArray<T> extends AbstractList<T>
      implements GenericArray<T>, Comparable<GenericArray<T>> {
    private final Schema schema;

    protected int size = 0;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
        justification = "the caller's object is adopted by reference deliberately: the datum/model layer shares"
            + "schemas and configuration objects instead of defensively copying them on construction and"
            + "configuration paths")
    public AbstractArray(Schema schema) {
      if (schema == null || Type.ARRAY != schema.getType())
        throw new AvroRuntimeException("Not an array schema: " + schema);
      this.schema = schema;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
        justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
            + "and datum objects are shared carriers by design, not defensive-copy value objects")
    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void reset() {
      size = 0;
    }

    @Override
    public int compareTo(GenericArray<T> that) {
      return GenericData.get().compare(this, that, this.getSchema());
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof Collection)) {
        return false;
      }
      return GenericData.get().compare(this, o, this.getSchema(), true) == 0;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public Iterator<T> iterator() {
      return new Iterator<>() {
        private int position = 0;

        @Override
        public boolean hasNext() {
          return position < size;
        }

        @Override
        public T next() {
          return AbstractArray.this.get(position++);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public void reverse() {
      int left = 0;
      int right = size - 1;

      while (left < right) {
        this.swap(left, right);

        left++;
        right--;
      }
    }

    protected abstract void swap(int index1, int index2);
  }

  /** Default implementation of an array. */
  @SuppressWarnings(value = "unchecked")
  public static class Array<T> extends AbstractArray<T> {
    private static final Object[] EMPTY = new Object[0];

    private Object[] elements = EMPTY;

    public Array(int capacity, Schema schema) {
      super(schema);
      if (capacity != 0)
        elements = new Object[capacity];
    }

    public Array(Schema schema, Collection<T> c) {
      super(schema);
      if (c != null) {
        elements = new Object[c.size()];
        addAll(c);
      }
    }

    @Override
    public void clear() {
      // Let GC do its work
      Arrays.fill(elements, 0, size, null);
      size = 0;
    }

    @Override
    public void prune() {
      if (size < elements.length) {
        Arrays.fill(elements, size, elements.length, null);
      }
    }

    @Override
    public T get(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return (T) elements[i];
    }

    @Override
    public void add(int location, T o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length) {
        // Increase size by 1.5x + 1
        final int newSize = size + (size >> 1) + 1;
        elements = Arrays.copyOf(elements, newSize);
      }
      System.arraycopy(elements, location, elements, location + 1, size - location);
      elements[location] = o;
      size++;
    }

    @Override
    public T set(int i, T o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      T response = (T) elements[i];
      elements[i] = o;
      return response;
    }

    @Override
    public T remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      T result = (T) elements[i];
      --size;
      System.arraycopy(elements, i + 1, elements, i, (size - i));
      elements[size] = null;
      return result;
    }

    @Override
    public T peek() {
      return (size < elements.length) ? (T) elements[size] : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      Object tmp = elements[index1];
      elements[index1] = elements[index2];
      elements[index2] = tmp;
    }
  }

  /** Default implementation of {@link GenericFixed}. */
  public static class Fixed implements GenericFixed, Comparable<Fixed> {
    private Schema schema;
    private byte[] bytes;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
        justification = "Fixed.bytes() returns the backing array by design; the zero-copy contract is documented and"
            + "callers must not modify the returned bytes")
    public Fixed(Schema schema) {
      setSchema(schema);
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "PCOA_PARTIALLY_CONSTRUCTED_OBJECT_ACCESS"},
        justification = "the caller's byte array is adopted by reference deliberately: Fixed is a zero-copy  datum"
            + "carrier and its documented contract is that the array backs the value;"
            + "PCOA_PARTIALLY_CONSTRUCTED_OBJECT_ACCESS: construction completes through the class's own"
            + "initialization protocol (its template methods and registration with the owning model); the"
            + "access to the partially constructed object stays inside the class's own constructor chain by"
            + "design")
    public Fixed(Schema schema, byte[] bytes) {
      this.schema = schema;
      this.bytes = bytes;
    }

    protected Fixed() {
    }

    protected void setSchema(Schema schema) {
      this.schema = schema;
      this.bytes = new byte[schema.getFixedSize()];
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
        justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
            + " and datum objects are shared carriers by design, not defensive-copy value objects")
    @Override
    public Schema getSchema() {
      return schema;
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
        justification = "Fixed.bytes() returns the backing array by design; the zero-copy contract is documented"
            + " and callers must not modify the returned bytes")
    public void bytes(byte[] bytes) {
      this.bytes = bytes;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
        justification = "Fixed.bytes() returns the backing array by design; the zero-copy contract is documented and"
            + " callers must not modify the returned bytes")
    @Override
    public byte[] bytes() {
      return bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      return o instanceof GenericFixed && Arrays.equals(bytes, ((GenericFixed) o).bytes());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
      return Arrays.toString(bytes);
    }

    @Override
    public int compareTo(Fixed that) {
      return BinaryData.compareBytes(this.bytes, 0, this.bytes.length, that.bytes, 0, that.bytes.length);
    }
  }

  /** Default implementation of {@link GenericEnumSymbol}. */
  public static class EnumSymbol implements GenericEnumSymbol<EnumSymbol> {
    private final Schema schema;
    private final String symbol;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
        justification = "the caller's object is adopted by reference deliberately: the datum/model layer shares"
            + "schemas and configuration objects instead of defensively copying them on construction and"
            + "configuration paths")
    public EnumSymbol(Schema schema, String symbol) {
      this.schema = schema;
      this.symbol = symbol;
    }

    /**
     * Maps existing Objects into an Avro enum by calling toString(), eg for Java
     * Enums
     */
    public EnumSymbol(Schema schema, Object symbol) {
      this(schema, symbol.toString());
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
        justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
            + "and datum objects are shared carriers by design, not defensive-copy value objects")
    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      return o instanceof GenericEnumSymbol && symbol.equals(o.toString());
    }

    @Override
    public int hashCode() {
      return symbol.hashCode();
    }

    @Override
    public String toString() {
      return symbol;
    }

    @Override
    public int compareTo(EnumSymbol that) {
      return GenericData.get().compare(this, that, schema);
    }
  }

  /** Returns a {@link DatumReader} for this kind of data. */
  public DatumReader createDatumReader(Schema schema) {
    return createDatumReader(schema, schema);
  }

  /** Returns a {@link DatumReader} for this kind of data. */
  public DatumReader createDatumReader(Schema writer, Schema reader) {
    return new GenericDatumReader(writer, reader, this);
  }

  /** Returns a {@link DatumWriter} for this kind of data. */
  public DatumWriter createDatumWriter(Schema schema) {
    return new GenericDatumWriter(schema, this);
  }

  /** Returns true if a Java datum matches a schema. */
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!isRecord(datum))
        return false;
      for (Field f : schema.getFields()) {
        if (!validate(f.schema(), getField(datum, f.name(), f.pos())))
          return false;
      }
      return true;
    case ENUM:
      if (!isEnum(datum))
        return false;
      return schema.getEnumSymbols().contains(datum.toString());
    case ARRAY:
      if (!(isArray(datum)))
        return false;
      for (Object element : getArrayAsCollection(datum))
        if (!validate(schema.getElementType(), element))
          return false;
      return true;
    case MAP:
      if (!(isMap(datum)))
        return false;
      @SuppressWarnings(value = "unchecked")
      Map<Object, Object> map = (Map<Object, Object>) datum;
      for (Map.Entry<Object, Object> entry : map.entrySet())
        if (!validate(schema.getValueType(), entry.getValue()))
          return false;
      return true;
    case UNION:
      try {
        int i = resolveUnion(schema, datum);
        return validate(schema.getTypes().get(i), datum);
      } catch (UnresolvedUnionException e) {
        return false;
      }
    case FIXED:
      return datum instanceof GenericFixed && ((GenericFixed) datum).bytes().length == schema.getFixedSize();
    case STRING:
      return isString(datum);
    case BYTES:
      return isBytes(datum);
    case INT:
      return isInteger(datum);
    case LONG:
      return isLong(datum);
    case FLOAT:
      return isFloat(datum);
    case DOUBLE:
      return isDouble(datum);
    case BOOLEAN:
      return isBoolean(datum);
    case NULL:
      return datum == null;
    default:
      return false;
    }
  }

  /** Renders a Java datum as <a href="https://www.json.org/">JSON</a>. */
  public String toString(Object datum) {
    StringBuilder buffer = new StringBuilder();
    try {
      toString(datum, buffer, new IdentityHashMap<>(128));
      return buffer.toString();
    } catch (StackOverflowError e) {
      return "StackOverflowError for " + datum.getClass() + ": " + buffer.substring(0, 1024) + "...";
    }
  }

  private static final String TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT = " \">>> CIRCULAR REFERENCE CANNOT BE PUT IN JSON STRING, ABORTING RECURSION <<<\" ";

  /** Renders a Java datum as <a href="https://www.json.org/">JSON</a>. */
  protected void toString(Object datum, StringBuilder buffer, IdentityHashMap<Object, Object> seenObjects) {
    if (isRecord(datum)) {
      if (seenObjects.containsKey(datum)) {
        buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
        return;
      }
      seenObjects.put(datum, datum);
      buffer.append('{');
      int count = 0;
      Schema schema = getRecordSchema(datum);
      for (Field f : schema.getFields()) {
        toString(f.name(), buffer, seenObjects);
        buffer.append(": ");
        toString(getField(datum, f.name(), f.pos()), buffer, seenObjects);
        if (++count < schema.getFields().size())
          buffer.append(", ");
      }
      buffer.append('}');
      seenObjects.remove(datum);
    } else if (isArray(datum)) {
      if (seenObjects.containsKey(datum)) {
        buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
        return;
      }
      seenObjects.put(datum, datum);
      Collection<?> array = getArrayAsCollection(datum);
      buffer.append('[');
      long last = array.size() - 1;
      int i = 0;
      for (Object element : array) {
        toString(element, buffer, seenObjects);
        if (i++ < last)
          buffer.append(", ");
      }
      buffer.append(']');
      seenObjects.remove(datum);
    } else if (isMap(datum)) {
      if (seenObjects.containsKey(datum)) {
        buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
        return;
      }
      seenObjects.put(datum, datum);
      buffer.append('{');
      int count = 0;
      @SuppressWarnings(value = "unchecked")
      Map<Object, Object> map = (Map<Object, Object>) datum;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        buffer.append('"');
        writeEscapedString(String.valueOf(entry.getKey()), buffer);
        buffer.append("\": ");
        toString(entry.getValue(), buffer, seenObjects);
        if (++count < map.size())
          buffer.append(", ");
      }
      buffer.append('}');
      seenObjects.remove(datum);
    } else if (isString(datum) || isEnum(datum)) {
      buffer.append('"');
      writeEscapedString(datum.toString(), buffer);
      buffer.append('"');
    } else if (isBytes(datum)) {
      buffer.append('"');
      ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
      writeEscapedString(StandardCharsets.ISO_8859_1.decode(bytes), buffer);
      buffer.append('"');
    } else if (isNanOrInfinity(datum) || isTemporal(datum) || datum instanceof UUID) {
      buffer.append('"');
      buffer.append(datum);
      buffer.append('"');
    } else if (datum instanceof GenericData) {
      if (seenObjects.containsKey(datum)) {
        buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
        return;
      }
      seenObjects.put(datum, datum);
      toString(datum, buffer, seenObjects);
      seenObjects.remove(datum);
    } else {
      buffer.append(datum);
    }
  }

  private boolean isTemporal(Object datum) {
    return datum instanceof Temporal;
  }

  private boolean isNanOrInfinity(Object datum) {
    return ((datum instanceof Float) && (((Float) datum).isInfinite() || ((Float) datum).isNaN()))
        || ((datum instanceof Double) && (((Double) datum).isInfinite() || ((Double) datum).isNaN()));
  }

  /* Adapted from https://code.google.com/p/json-simple */
  private static void writeEscapedString(CharSequence string, StringBuilder builder) {
    for (int i = 0; i < string.length(); i++) {
      char ch = string.charAt(i);
      switch (ch) {
      case '"':
        builder.append("\\\"");
        break;
      case '\\':
        builder.append("\\\\");
        break;
      case '\b':
        builder.append("\\b");
        break;
      case '\f':
        builder.append("\\f");
        break;
      case '\n':
        builder.append("\\n");
        break;
      case '\r':
        builder.append("\\r");
        break;
      case '\t':
        builder.append("\\t");
        break;
      default:
        // Reference: https://www.unicode.org/versions/Unicode5.1.0/
        if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F')
            || (ch >= '\u2000' && ch <= '\u20FF')) {
          String hex = Integer.toHexString(ch);
          builder.append("\\u");
          for (int j = 0; j < 4 - hex.length(); j++)
            builder.append('0');
          builder.append(hex.toUpperCase());
        } else {
          builder.append(ch);
        }
      }
    }
  }

  /** Create a schema given an example datum. */
  public Schema induce(Object datum) {
    if (isRecord(datum)) {
      return getRecordSchema(datum);
    } else if (isArray(datum)) {
      Schema elementType = null;
      for (Object element : getArrayAsCollection(datum)) {
        if (elementType == null) {
          elementType = induce(element);
        } else if (!elementType.equals(induce(element))) {
          throw new AvroTypeException("No mixed type arrays.");
        }
      }
      if (elementType == null) {
        throw new AvroTypeException("Empty array: " + datum);
      }
      return Schema.createArray(elementType);

    } else if (isMap(datum)) {
      @SuppressWarnings(value = "unchecked")
      Map<Object, Object> map = (Map<Object, Object>) datum;
      Schema value = null;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        if (value == null) {
          value = induce(entry.getValue());
        } else if (!value.equals(induce(entry.getValue()))) {
          throw new AvroTypeException("No mixed type map values.");
        }
      }
      if (value == null) {
        throw new AvroTypeException("Empty map: " + datum);
      }
      return Schema.createMap(value);
    } else if (datum instanceof GenericFixed) {
      return Schema.createFixed(null, null, null, ((GenericFixed) datum).bytes().length);
    } else if (isString(datum))
      return Schema.create(Type.STRING);
    else if (isBytes(datum))
      return Schema.create(Type.BYTES);
    else if (isInteger(datum))
      return Schema.create(Type.INT);
    else if (isLong(datum))
      return Schema.create(Type.LONG);
    else if (isFloat(datum))
      return Schema.create(Type.FLOAT);
    else if (isDouble(datum))
      return Schema.create(Type.DOUBLE);
    else if (isBoolean(datum))
      return Schema.create(Type.BOOLEAN);
    else if (datum == null)
      return Schema.create(Type.NULL);

    else
      throw new AvroTypeException("Can't create schema for: " + datum);
  }

  /**
   * Called by {@link GenericDatumReader#readRecord} to set a record fields value
   * to a record instance. The default implementation is for
   * {@link IndexedRecord}.
   */
  public void setField(Object record, String name, int position, Object value) {
    ((IndexedRecord) record).put(position, value);
  }

  /**
   * Called by {@link GenericDatumReader#readRecord} to retrieve a record field
   * value from a reused instance. The default implementation is for
   * {@link IndexedRecord}.
   */
  public Object getField(Object record, String name, int position) {
    return ((IndexedRecord) record).get(position);
  }

  /**
   * Produce state for repeated calls to
   * {@link #getField(Object,String,int,Object)} and
   * {@link #setField(Object,String,int,Object,Object)} on the same record.
   */
  protected Object getRecordState(Object record, Schema schema) {
    return null;
  }

  /** Version of {@link #setField} that has state. */
  protected void setField(Object record, String name, int position, Object value, Object state) {
    setField(record, name, position, value);
  }

  /** Version of {@link #getField} that has state. */
  protected Object getField(Object record, String name, int pos, Object state) {
    return getField(record, name, pos);
  }

  /**
   * Return the index for a datum within a union. Implemented with
   * {@link Schema#getIndexNamed(String)} and {@link #getSchemaName(Object)}.
   */
  public int resolveUnion(Schema union, Object datum) {
    // if there is a logical type that works, use it first
    // this allows logical type concrete classes to overlap with supported ones
    // for example, a conversion could return a map
    if (datum != null) {
      List<Schema> candidates = union.getTypes();
      for (int i = 0; i < candidates.size(); i += 1) {
        LogicalType candidateType = candidates.get(i).getLogicalType();
        if (candidateType != null) {
          // Assignability-based lookup, like the write path: a conversion
          // whose converted type is a supertype of the datum's class (for
          // example Object for the "any" logical type) selects the branch.
          Conversion<?> conversion = getConversionByClass(datum.getClass(), candidateType);
          if (conversion != null) {
            return i;
          }
        }
      }
    }

    Integer i = union.getIndexNamed(getSchemaName(datum));
    if (i != null) {
      return i;
    }
    throw new UnresolvedUnionException(union, datum);
  }

  /**
   * Return the schema full name for a datum. Called by
   * {@link #resolveUnion(Schema,Object)}.
   */
  protected String getSchemaName(Object datum) {
    if (datum == null || datum == JsonProperties.NULL_VALUE)
      return Type.NULL.getName();
    String primativeType = getPrimitiveTypeCache().get(datum.getClass());
    if (primativeType != null)
      return primativeType;
    if (isRecord(datum))
      return getRecordSchema(datum).getFullName();
    if (isEnum(datum))
      return getEnumSchema(datum).getFullName();
    if (isArray(datum))
      return Type.ARRAY.getName();
    if (isMap(datum))
      return Type.MAP.getName();
    if (isFixed(datum))
      return getFixedSchema(datum).getFullName();
    if (isString(datum))
      return Type.STRING.getName();
    if (isBytes(datum))
      return Type.BYTES.getName();
    if (isInteger(datum))
      return Type.INT.getName();
    if (isLong(datum))
      return Type.LONG.getName();
    if (isFloat(datum))
      return Type.FLOAT.getName();
    if (isDouble(datum))
      return Type.DOUBLE.getName();
    if (isBoolean(datum))
      return Type.BOOLEAN.getName();
    throw new AvroRuntimeException(String.format("Unknown datum type %s: %s", datum.getClass().getName(), datum));
  }

  /**
   * Called to obtain the primitive type cache. May be overridden for alternate
   * record representations.
   */
  protected Map<Class<?>, String> getPrimitiveTypeCache() {
    return PRIMITIVE_DATUM_TYPES;
  }

  /**
   * Called by {@link #resolveUnion(Schema,Object)}. May be overridden for
   * alternate data representations.
   */
  protected boolean instanceOf(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!isRecord(datum))
        return false;
      return (schema.getFullName() == null) ? getRecordSchema(datum).getFullName() == null
          : schema.getFullName().equals(getRecordSchema(datum).getFullName());
    case ENUM:
      if (!isEnum(datum))
        return false;
      return schema.getFullName().equals(getEnumSchema(datum).getFullName());
    case ARRAY:
      return isArray(datum);
    case MAP:
      return isMap(datum);
    case FIXED:
      if (!isFixed(datum))
        return false;
      return schema.getFullName().equals(getFixedSchema(datum).getFullName());
    case STRING:
      return isString(datum);
    case BYTES:
      return isBytes(datum);
    case INT:
      return isInteger(datum);
    case LONG:
      return isLong(datum);
    case FLOAT:
      return isFloat(datum);
    case DOUBLE:
      return isDouble(datum);
    case BOOLEAN:
      return isBoolean(datum);
    case NULL:
      return datum == null;
    default:
      throw new AvroRuntimeException("Unexpected type: " + schema);
    }
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isArray(Object datum) {
    return datum instanceof Collection;
  }

  /** Called to access an array as a collection. */
  protected Collection getArrayAsCollection(Object datum) {
    return (Collection) datum;
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isRecord(Object datum) {
    return datum instanceof IndexedRecord;
  }

  /**
   * Called to obtain the schema of a record. By default calls
   * {GenericContainer#getSchema(). May be overridden for alternate record
   * representations.
   */
  protected Schema getRecordSchema(Object record) {
    return ((GenericContainer) record).getSchema();
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isEnum(Object datum) {
    return datum instanceof GenericEnumSymbol;
  }

  /**
   * Called to obtain the schema of a enum. By default calls
   * {GenericContainer#getSchema(). May be overridden for alternate enum
   * representations.
   */
  protected Schema getEnumSchema(Object enu) {
    return ((GenericContainer) enu).getSchema();
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isMap(Object datum) {
    return datum instanceof Map;
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isFixed(Object datum) {
    return datum instanceof GenericFixed;
  }

  /**
   * Called to obtain the schema of a fixed. By default calls
   * {GenericContainer#getSchema(). May be overridden for alternate fixed
   * representations.
   */
  protected Schema getFixedSchema(Object fixed) {
    return ((GenericContainer) fixed).getSchema();
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isString(Object datum) {
    return datum instanceof CharSequence;
  }

  /** Called by the default implementation of {@link #instanceOf}. */
  protected boolean isBytes(Object datum) {
    return datum instanceof ByteBuffer;
  }

  /**
   * Called by the default implementation of {@link #instanceOf}.
   */
  protected boolean isInteger(Object datum) {
    return datum instanceof Integer;
  }

  /**
   * Called by the default implementation of {@link #instanceOf}.
   */
  protected boolean isLong(Object datum) {
    return datum instanceof Long;
  }

  /**
   * Called by the default implementation of {@link #instanceOf}.
   */
  protected boolean isFloat(Object datum) {
    return datum instanceof Float;
  }

  /**
   * Called by the default implementation of {@link #instanceOf}.
   */
  protected boolean isDouble(Object datum) {
    return datum instanceof Double;
  }

  /**
   * Called by the default implementation of {@link #instanceOf}.
   */
  protected boolean isBoolean(Object datum) {
    return datum instanceof Boolean;
  }

  /**
   * Compute a hash code according to a schema, consistent with
   * {@link #compare(Object,Object,Schema)}.
   */
  public int hashCode(Object o, Schema s) {
    HashCodeCalculator calculator = new HashCodeCalculator();
    return calculator.hashCode(o, s);
  }

  class HashCodeCalculator {
    private int counter = 10;

    private int currentHashCode = 1;

    public int hashCode(Object o, Schema s) {
      if (o == null)
        return 0; // incomplete datum

      switch (s.getType()) {
      case RECORD:
        for (Field f : s.getFields()) {
          if (this.shouldStop()) {
            return this.currentHashCode;
          }
          if (f.order() == Field.Order.IGNORE)
            continue;
          Object fieldValue = ((IndexedRecord) o).get(f.pos());
          this.currentHashCode = this.hashCodeAdd(fieldValue, f.schema());
        }
        return currentHashCode;
      case ARRAY:
        Collection<?> a = (Collection<?>) o;
        Schema elementType = s.getElementType();
        for (Object e : a) {
          if (this.shouldStop()) {
            return currentHashCode;
          }
          currentHashCode = this.hashCodeAdd(e, elementType);
        }
        return currentHashCode;
      case UNION:
        return hashCode(o, s.getTypes().get(GenericData.this.resolveUnion(s, o)));
      case ENUM:
        return s.getEnumOrdinal(o.toString());
      case NULL:
        return 0;
      case STRING:
        return (o instanceof Utf8 ? o : new Utf8(o.toString())).hashCode();
      default:
        return o.hashCode();
      }
    }

    /** Add the hash code for an object into an accumulated hash code. */
    protected int hashCodeAdd(Object o, Schema s) {
      return 31 * this.currentHashCode + hashCode(o, s);
    }

    private boolean shouldStop() {
      return --counter <= 0;
    }
  }

  /**
   * Compare objects according to their schema. If equal, return zero. If
   * greater-than, return 1, if less than return -1. Order is consistent with that
   * of {@link BinaryData#compare(byte[], int, byte[], int, Schema)}.
   */
  public int compare(Object o1, Object o2, Schema s) {
    return compare(o1, o2, s, false);
  }

  protected int compareMaps(final Map<?, ?> m1, final Map<?, ?> m2) {
    if (m1 == m2) {
      return 0;
    }

    if (m1.isEmpty() && m2.isEmpty()) {
      return 0;
    }

    if (m1.size() != m2.size()) {
      return 1;
    }

    /**
     * Peek at keys, assuming they're all the same type within a Map
     */
    final Object key1 = m1.keySet().iterator().next();
    final Object key2 = m2.keySet().iterator().next();
    boolean utf8ToString = false;
    boolean stringToUtf8 = false;

    if (key1 instanceof Utf8 && key2 instanceof String) {
      utf8ToString = true;
    } else if (key1 instanceof String && key2 instanceof Utf8) {
      stringToUtf8 = true;
    }

    try {
      for (Map.Entry e : m1.entrySet()) {
        final Object key = e.getKey();
        Object lookupKey = key;
        if (utf8ToString) {
          lookupKey = key.toString();
        } else if (stringToUtf8) {
          lookupKey = new Utf8((String) lookupKey);
        }
        final Object value = e.getValue();
        if (value == null) {
          if (!(m2.get(lookupKey) == null && m2.containsKey(lookupKey))) {
            return 1;
          }
        } else {
          final Object value2 = m2.get(lookupKey);
          if (value instanceof Utf8 && value2 instanceof String) {
            if (!value.toString().equals(value2)) {
              return 1;
            }
          } else if (value instanceof String && value2 instanceof Utf8) {
            if (!new Utf8((String) value).equals(value2)) {
              return 1;
            }
          } else {
            if (!value.equals(value2)) {
              return 1;
            }
          }
        }
      }
    } catch (ClassCastException | NullPointerException unused) {
      return 1;
    }

    return 0;
  }

  /**
   * Comparison implementation. When equals is true, only checks for equality, not
   * for order.
   */
  @SuppressWarnings(value = "unchecked")
  protected int compare(Object o1, Object o2, Schema s, boolean equals) {
    if (o1 == o2)
      return 0;
    if (s.getLogicalType() != null) {
      // Logical-typed values are compared as their converted representation
      // (the fork's compare head): for equality that representation's own
      // equality decides -- the raw type's ordering must not be consulted,
      // as the converted value need not be Comparable at all (for example a
      // json_record bytes value converts to a Map). For ordering, a
      // Comparable converted value compares directly; a non-Comparable one
      // falls through to the raw-type comparison below.
      if (equals) {
        if (Objects.deepEquals(o1, o2)) {
          return 0;
        }
        // Adaptation to this tree: a same-class Comparable converted value
        // whose compareTo is 0 also counts as equal. The fork's pure
        // deepEquals is scale-sensitive for BigDecimal (0 != 0E-8), while
        // this tree's decimal equality (pinned by
        // TestSpecificRecordWithUnion) follows the Comparable semantics
        // the raw comparison below has always given it.
        if (o1 != null && o2 != null && o1.getClass() == o2.getClass() && o1 instanceof Comparable
            && o2 instanceof Comparable) {
          return ((Comparable) o1).compareTo(o2) == 0 ? 0 : 1;
        }
        return 1;
      } else if (o1 instanceof Comparable && o2 instanceof Comparable) {
        return ((Comparable) o1).compareTo(o2);
      }
    }
    switch (s.getType()) {
    case RECORD:
      for (Field f : s.getFields()) {
        if (f.order() == Field.Order.IGNORE)
          continue; // ignore this field
        int pos = f.pos();
        String name = f.name();
        int compare = compare(getField(o1, name, pos), getField(o2, name, pos), f.schema(), equals);
        if (compare != 0) // not equal
          return f.order() == Field.Order.DESCENDING ? -compare : compare;
      }
      return 0;
    case ENUM:
      return s.getEnumOrdinal(o1.toString()) - s.getEnumOrdinal(o2.toString());
    case ARRAY:
      Collection a1 = (Collection) o1;
      Collection a2 = (Collection) o2;
      Iterator e1 = a1.iterator();
      Iterator e2 = a2.iterator();
      Schema elementType = s.getElementType();
      while (e1.hasNext() && e2.hasNext()) {
        int compare = compare(e1.next(), e2.next(), elementType, equals);
        if (compare != 0)
          return compare;
      }
      return e1.hasNext() ? 1 : (e2.hasNext() ? -1 : 0);
    case MAP:
      if (equals)
        return compareMaps((Map) o1, (Map) o2);
      throw new AvroRuntimeException("Can't compare maps!");
    case UNION:
      int i1 = resolveUnion(s, o1);
      int i2 = resolveUnion(s, o2);
      return (i1 == i2) ? compare(o1, o2, s.getTypes().get(i1), equals) : Integer.compare(i1, i2);
    case NULL:
      return 0;
    case STRING:
      CharSequence cs1 = o1 instanceof CharSequence ? (CharSequence) o1 : o1.toString();
      CharSequence cs2 = o2 instanceof CharSequence ? (CharSequence) o2 : o2.toString();
      return Utf8.compareSequences(cs1, cs2);
    default:
      return ((Comparable) o1).compareTo(o2);
    }
  }

  private final ConcurrentMap<Field, Object> defaultValueCache = new ConcurrentReferenceHashMap<>(128, WEAK);

  /**
   * Gets the default value of the given field, if any.
   *
   * @param field the field whose default value should be retrieved.
   * @return the default value associated with the given field, or null if none is
   *         specified in the schema.
   */
  @SuppressWarnings({ "unchecked" })
  public Object getDefaultValue(Field field) {
    JsonNode json = Accessor.defaultValue(field);
    if (json == null)
      throw new AvroMissingFieldException("Field " + field + " not set and has no default value", field);
    if (json.isNull() && (field.schema().getType() == Type.NULL
        || (field.schema().getType() == Type.UNION && field.schema().getTypes().get(0).getType() == Type.NULL))) {
      return null;
    }

    // Check the cache
    // If not cached, get the default Java value by encoding the default JSON
    // value and then decoding it:
    return defaultValueCache.computeIfAbsent(field, fieldToGetValueFor -> {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        Accessor.encode(encoder, fieldToGetValueFor.schema(), json);
        encoder.flush();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
        return createDatumReader(fieldToGetValueFor.schema()).read(null, decoder);
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    });
  }

  private static final Schema STRINGS = Schema.create(Type.STRING);

  /**
   * Makes a deep copy of a value given its schema.
   * <P>
   * Logical types are converted to raw types, copied, then converted back.
   *
   * @param schema the schema of the value to deep copy.
   * @param value  the value to deep copy.
   * @return a deep copy of the given value.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <T> T deepCopy(Schema schema, T value) {
    if (value == null)
      return null;
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == null) // not a logical type -- use raw copy
      return (T) deepCopyRaw(schema, value);
    Conversion conversion = getConversionByClass(value.getClass(), logicalType);
    if (conversion == null) // no conversion defined -- try raw copy
      return (T) deepCopyRaw(schema, value);
    // logical type with conversion: convert to raw, copy, then convert back to
    // logical
    Object raw = Conversions.convertToRawType(value, schema, logicalType, conversion);
    Object copy = deepCopyRaw(schema, raw); // copy raw
    return (T) Conversions.convertToLogicalType(copy, schema, logicalType, conversion);
  }

  private Object deepCopyRaw(Schema schema, Object value) {
    if (value == null) {
      return null;
    }

    switch (schema.getType()) {
    case ARRAY:
      List<Object> arrayValue = (List) value;
      List<Object> arrayCopy = new GenericData.Array<>(arrayValue.size(), schema);
      for (Object obj : arrayValue) {
        arrayCopy.add(deepCopy(schema.getElementType(), obj));
      }
      return arrayCopy;
    case BOOLEAN:
      return value; // immutable
    case BYTES:
      ByteBuffer byteBufferValue = (ByteBuffer) value;
      int start = byteBufferValue.position();
      int length = byteBufferValue.limit() - start;
      byte[] bytesCopy = new byte[length];
      byteBufferValue.get(bytesCopy, 0, length);
      ((Buffer) byteBufferValue).position(start);
      return ByteBuffer.wrap(bytesCopy, 0, length);
    case DOUBLE:
      return value; // immutable
    case ENUM:
      return createEnum(value.toString(), schema);
    case FIXED:
      return createFixed(null, ((GenericFixed) value).bytes(), schema);
    case FLOAT:
      return value; // immutable
    case INT:
      return value; // immutable
    case LONG:
      return value; // immutable
    case MAP:
      Map<Object, Object> mapValue = (Map) value;
      Map<Object, Object> mapCopy = new HashMap<>(mapValue.size());
      for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
        mapCopy.put(deepCopy(STRINGS, entry.getKey()), deepCopy(schema.getValueType(), entry.getValue()));
      }
      return mapCopy;
    case NULL:
      return null;
    case RECORD:
      Object oldState = getRecordState(value, schema);
      Object newRecord = newRecord(null, schema);
      Object newState = getRecordState(newRecord, schema);
      for (Field f : schema.getFields()) {
        int pos = f.pos();
        String name = f.name();
        Object newValue = deepCopy(f.schema(), getField(value, name, pos, oldState));
        setField(newRecord, name, pos, newValue, newState);
      }
      return newRecord;
    case STRING:
      return createString(value);
    case UNION:
      return deepCopy(schema.getTypes().get(resolveUnion(schema, value)), value);
    default:
      throw new AvroRuntimeException("Deep copy failed for schema \"" + schema + "\" and value \"" + value + "\"");
    }
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   */
  public Object createFixed(Object old, Schema schema) {
    if ((old instanceof GenericFixed) && ((GenericFixed) old).bytes().length == schema.getFixedSize())
      return old;
    return new GenericData.Fixed(schema);
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   */
  public Object createFixed(Object old, byte[] bytes, Schema schema) {
    GenericFixed fixed = (GenericFixed) createFixed(old, schema);
    System.arraycopy(bytes, 0, fixed.bytes(), 0, schema.getFixedSize());
    return fixed;
  }

  /**
   * Called to create an enum value. May be overridden for alternate enum
   * representations. By default, returns a GenericEnumSymbol.
   */
  public Object createEnum(String symbol, Schema schema) {
    return new EnumSymbol(schema, symbol);
  }

  /**
   * Called to create new record instances. Subclasses may override to use a
   * different record implementation. The returned instance must conform to the
   * schema provided. If the old object contains fields not present in the schema,
   * they should either be removed from the old object, or it should create a new
   * instance that conforms to the schema. By default, this returns a
   * {@link GenericData.Record}.
   */
  @SuppressFBWarnings(value = "URV_CHANGE_RETURN_TYPE",
      justification = "the method is inherited as part of this class's published or generated API surface; its"
          + "signature is fixed by the supertype's contract")
  public Object newRecord(Object old, Schema schema) {
    if (old instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) old;
      if (record.getSchema() == schema)
        return record;
    }
    return new GenericData.Record(schema);
  }

  /**
   * Called to create an string value. May be overridden for alternate string
   * representations.
   */
  public Object createString(Object value) {
    // Strings are immutable
    if (value instanceof String) {
      return value;
    }

    // Some CharSequence subclasses are mutable, so we still need to make
    // a copy
    else if (value instanceof Utf8) {
      // Utf8 copy constructor is more efficient than converting
      // to string and then back to Utf8
      return new Utf8((Utf8) value);
    }
    return new Utf8(value.toString());

  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different array implementation. By default, this returns a
   * {@link GenericData.Array}.
   *
   * @param old    the old array instance to reuse, if possible. If the old array
   *               is an appropriate type, it may be cleared and returned.
   * @param size   the size of the array to create.
   * @param schema the schema of the array elements.
   */
  @SuppressFBWarnings(value = "CFS_CONFUSING_FUNCTION_SEMANTICS",
      justification = "the published contract is to complete/adapt and return the argument (fluent builder and"
          + "logical-type API); callers rely on receiving the same reference back")
  public Object newArray(Object old, int size, Schema schema) {
    final var logicalType = schema.getElementType().getLogicalType();
    final var conversion = getConversionFor(logicalType);
    final var optimalValueType = optimalValueType(schema, logicalType,
        conversion == null ? null : conversion.getConvertedType());

    if (old != null) {
      if (old instanceof GenericData.Array<?>) {
        ((GenericData.Array<?>) old).reset();
        return old;
      } else if (old instanceof PrimitiveArray) {
        var primitiveOld = (PrimitiveArray<?>) old;
        if (primitiveOld.valueType() == optimalValueType) {
          primitiveOld.reset();
          return old;
        }
      } else if (old instanceof Collection) {
        ((Collection<?>) old).clear();
        return old;
      }
    }
    // we can't reuse the old array, so we create a new one
    return PrimitivesArrays.createOptimizedArray(size, schema, optimalValueType);
  }

  /**
   * Determine the optimal value type for an array. The value type is determined
   * form the convertedElementType if supplied, otherwise the underlying type from
   * the schema
   *
   * @param schema               the schema of the array
   * @param convertedElementType the converted elements value type. This may not
   *                             be the same and the schema if for instance there
   *                             is a logical type, and a convertor is use
   * @return an indicator for the type of the array, useful for
   *         {@link PrimitivesArrays#createOptimizedArray(int, Schema, Schema.Type)}.
   *         May be null if the type is not optimised
   */
  public static Schema.Type optimalValueType(Schema schema, LogicalType logicalType, Class<?> convertedElementType) {
    if (logicalType == null)
      // if there are no logical types- use the schema type
      return schema.getElementType().getType();
    else if (convertedElementType == null)
      // if there is no convertor
      return null;
    else
      // use the converted type
      return PRIMITIVE_TYPES_WITH_SPECIALISED_ARRAYS.get(convertedElementType);
  }

  private final static Map<Class<?>, Schema.Type> PRIMITIVE_TYPES_WITH_SPECIALISED_ARRAYS = Map.of(//
      Long.TYPE, Schema.Type.LONG, //
      Integer.TYPE, Schema.Type.INT, //
      Float.TYPE, Schema.Type.FLOAT, //
      Double.TYPE, Schema.Type.DOUBLE, //
      Boolean.TYPE, Schema.Type.BOOLEAN);

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different map implementation. By default, this returns a {@link HashMap}.
   */
  @SuppressFBWarnings(value = "CFS_CONFUSING_FUNCTION_SEMANTICS",
      justification = "the published contract is to complete/adapt and return the argument (fluent builder and"
          + "logical-type API); callers rely on receiving the same reference back")
  public Object newMap(Object old, int size) {
    if (old instanceof Map) {
      ((Map<?, ?>) old).clear();
      return old;
    } else
      return new HashMap<>(size);
  }

  /**
   * create a supplier that allows to get new record instances for a given schema
   * in an optimized way
   */
  public InstanceSupplier getNewRecordSupplier(Schema schema) {
    return this::newRecord;
  }

  public interface InstanceSupplier {
    public Object newInstance(Object oldInstance, Schema schema);
  }
}
