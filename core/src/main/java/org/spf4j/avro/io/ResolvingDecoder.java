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
package org.spf4j.avro.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.spf4j.avro.AvroTypeException;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.parsing.ResolvingGrammarGenerator;
import org.spf4j.avro.io.parsing.Symbol;
import org.spf4j.avro.util.Utf8;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * {@link Decoder} that performs type-resolution between the reader's and
 * writer's schemas.
 *
 * <p>
 * When resolving schemas, this class will return the values of fields in
 * _writer's_ order, not the reader's order. (However, it returns _only_ the
 * reader's fields, not any extra fields the writer may have written.) To help
 * clients handle fields that appear to be coming out of order, this class
 * defines the method {@link #readFieldOrder}.
 *
 * <p>
 * See the <a href="parsing/doc-files/parsing.html">parser documentation</a> for
 * information on how this works.
 */
public class ResolvingDecoder extends ValidatingDecoder implements JsonExtensionDecoder {

  private Decoder backup;

  ResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    this(resolve(writer, reader), in);
  }

  /**
   * Constructs a <tt>ResolvingDecoder</tt> using the given resolver. The resolver
   * must have been returned by a previous call to
   * {@link #resolve(Schema, Schema)}.
   *
   * @param resolver The resolver to use.
   * @param in       The underlying decoder.
   * @throws IOException
   */
  private ResolvingDecoder(Object resolver, Decoder in) throws IOException {
    super((Symbol) resolver, in);
  }

  /**
   * Produces an opaque resolver that can be used to construct a new
   * {@link ResolvingDecoder#ResolvingDecoder(Object, Decoder)}. The returned
   * Object is immutable and hence can be simultaneously used in many
   * ResolvingDecoders. This method is reasonably expensive, the users are
   * encouraged to cache the result.
   *
   * @param writer The writer's schema. Cannot be null.
   * @param reader The reader's schema. Cannot be null.
   * @return The opaque resolver.
   * @throws IOException
   * @throws NullPointerException if {@code writer} or {@code reader} is
   *                              {@code null}
   */
  public static Object resolve(Schema writer, Schema reader) throws IOException {
    Objects.requireNonNull(writer, "Writer schema cannot be null");
    Objects.requireNonNull(reader, "Reader schema cannot be null");
    return new ResolvingGrammarGenerator().generate(writer, reader);
  }

  /**
   * Returns the actual order in which the reader's fields will be returned to the
   * reader.
   *
   * This method is useful because {@link ResolvingDecoder} returns values in the
   * order written by the writer, rather than the order expected by the reader.
   * This method allows readers to figure out what fields to expect. Let's say the
   * reader is expecting a three-field record, the first field is a long, the
   * second a string, and the third an array. In this case, a typical usage might
   * be as follows:
   *
   * <pre>
   *   Schema.Fields[] fieldOrder = in.readFieldOrder();
   *   for (int i = 0; i &lt; 3; i++) {
   *     switch (fieldOrder[i].pos()) {
   *     case 1:
   *       foo(in.readLong());
   *       break;
   *     case 2:
   *       someVariable = in.readString();
   *       break;
   *     case 3:
   *       bar(in); // The code of "bar" will read an array-of-int
   *       break;
   *     }
   * </pre>
   *
   * Note that {@link ResolvingDecoder} will return only the fields expected by
   * the reader, not other fields that may have been written by the writer. Thus,
   * the iteration-count of "3" in the above loop will always be correct.
   *
   * Throws a runtime exception if we're not just about to read the first field of
   * a record. (If the client knows the order of incoming fields, then the client
   * does <em>not</em> need to call this method but rather can just start reading
   * the field values.)
   *
   * @throws AvroTypeException If we're not starting a new record
   *
   */
  public final Schema.Field[] readFieldOrder() throws IOException {
    return ((Symbol.FieldOrderAction) parser.advance(Symbol.FIELD_ACTION)).fields;
  }

  /**
   * Same as {@link #readFieldOrder} except that it returns <tt>null</tt> if there
   * was no reordering of fields, i.e., if the correct thing for the reader to do
   * is to read (all) of its fields in the order specified by its own schema
   * (useful for optimizations).
   */
  public final Schema.Field[] readFieldOrderIfDiff() throws IOException {
    Symbol.FieldOrderAction top = (Symbol.FieldOrderAction) parser.advance(Symbol.FIELD_ACTION);
    return (top.noReorder ? null : top.fields);
  }

  /**
   * Consume any more data that has been written by the writer but not needed by
   * the reader so that the underlying decoder is in proper shape for the next
   * record. This situation happens when, for example, the writer writes a record
   * with two fields and the reader needs only the first field.
   *
   * This function should be called after completely decoding an object but before
   * next object can be decoded from the same underlying decoder either directly
   * or through another resolving decoder. If the same resolving decoder is used
   * for the next object as well, calling this method is optional; the state of
   * this resolving decoder ensures that any leftover portions are consumed before
   * the next object is decoded.
   *
   * @throws IOException
   */
  public final void drain() throws IOException {
    parser.processImplicitActions();
  }

  // JsonExtensionDecoder support. The fork carried this in a separate
  // JsonExtensionResolvingDecoder subclass selected by DecoderFactory;
  // here the (single, schema-cached) ResolvingDecoder implements the
  // interface itself and delegates to the underlying decoder only when
  // that decoder is itself an extension decoder, so a resolver created
  // for one input can be reconfigured for any other. Each method lets the
  // underlying decoder consume the value first (advancing its own
  // grammar) and only then advances this decoder's resolving grammar by
  // the same schema. When the underlying decoder is not an extension
  // decoder at all, the number reads return null without touching either
  // grammar, so the caller's default read path is unaffected; when it is
  // one, a null result carries the caveat documented on
  // ExtendedJsonDecoder.readBigDecimal (the underlying grammar has
  // advanced to the value, so that value is not fallback-readable).

  private JsonExtensionDecoder extensionDecoder() {
    return in instanceof JsonExtensionDecoder ? (JsonExtensionDecoder) in : null;
  }

  private JsonExtensionDecoder requireExtensionDecoder() {
    JsonExtensionDecoder ext = extensionDecoder();
    if (ext == null) {
      throw new UnsupportedOperationException(
          "Underlying decoder " + in + " does not implement JsonExtensionDecoder");
    }
    return ext;
  }

  /**
   * Advance this decoder's resolving grammar past one value of the given
   * schema, mirroring what the corresponding {@code readXxx} method would
   * do. Only terminal (primitive) schemas are supported: a direct decode
   * of a composite schema through a resolving decoder is not (yet)
   * supported -- such conversions read their value through the terminal
   * fields instead.
   */
  private void advanceBy(Schema schema) throws IOException {
    switch (schema.getType()) {
    case NULL:
      parser.advance(Symbol.NULL);
      break;
    case BOOLEAN:
      parser.advance(Symbol.BOOLEAN);
      break;
    case INT:
      parser.advance(Symbol.INT);
      break;
    case LONG:
      parser.advance(Symbol.LONG);
      break;
    case FLOAT:
      parser.advance(Symbol.FLOAT);
      break;
    case DOUBLE:
      parser.advance(Symbol.DOUBLE);
      break;
    case STRING:
      parser.advance(Symbol.STRING);
      break;
    case BYTES:
      parser.advance(Symbol.BYTES);
      break;
    case FIXED:
      parser.advance(Symbol.FIXED);
      parser.popSymbol();
      break;
    case ENUM:
      parser.advance(Symbol.ENUM);
      parser.popSymbol();
      break;
    default:
      throw new UnsupportedOperationException(
          "Direct extension decode of composite schema " + schema + " through a ResolvingDecoder is not supported");
    }
  }

  @Override
  public BigDecimal readBigDecimal(Schema schema) throws IOException {
    JsonExtensionDecoder ext = extensionDecoder();
    if (ext == null) {
      return null;
    }
    BigDecimal result = ext.readBigDecimal(schema);
    if (result != null) {
      advanceBy(schema);
    }
    return result;
  }

  @Override
  public BigInteger readBigInteger(Schema schema) throws IOException {
    JsonExtensionDecoder ext = extensionDecoder();
    if (ext == null) {
      return null;
    }
    BigInteger result = ext.readBigInteger(schema);
    if (result != null) {
      advanceBy(schema);
    }
    return result;
  }

  @Override
  public <T> T readValue(Schema schema, Class<T> clasz) throws IOException {
    T result = requireExtensionDecoder().readValue(schema, clasz);
    advanceBy(schema);
    return result;
  }

  @Override
  public JsonNode readValueAsTree(Schema schema) throws IOException {
    JsonNode result = requireExtensionDecoder().readValueAsTree(schema);
    advanceBy(schema);
    return result;
  }

  @Override
  public JsonParser bufferValue(Schema schema) throws IOException {
    JsonParser result = requireExtensionDecoder().bufferValue(schema);
    advanceBy(schema);
    return result;
  }

  @Override
  public long readLong() throws IOException {
    Symbol actual = parser.advance(Symbol.LONG);
    if (actual == Symbol.INT) {
      return in.readInt();
    } else if (actual == Symbol.DOUBLE) {
      return (long) in.readDouble();
    } else {
      assert actual == Symbol.LONG;
      return in.readLong();
    }
  }

  @Override
  public float readFloat() throws IOException {
    Symbol actual = parser.advance(Symbol.FLOAT);
    if (actual == Symbol.INT) {
      return (float) in.readInt();
    } else if (actual == Symbol.LONG) {
      return (float) in.readLong();
    } else {
      assert actual == Symbol.FLOAT;
      return in.readFloat();
    }
  }

  @Override
  public double readDouble() throws IOException {
    Symbol actual = parser.advance(Symbol.DOUBLE);
    if (actual == Symbol.INT) {
      return in.readInt();
    } else if (actual == Symbol.LONG) {
      return (double) in.readLong();
    } else if (actual == Symbol.FLOAT) {
      return in.readFloat();
    } else {
      assert actual == Symbol.DOUBLE;
      return in.readDouble();
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    Symbol actual = parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      return new Utf8(in.readBytes(null).array());
    } else {
      assert actual == Symbol.STRING;
      return in.readString(old);
    }
  }

  @Override
  public String readString() throws IOException {
    Symbol actual = parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      return new String(in.readBytes(null).array(), StandardCharsets.UTF_8);
    } else {
      assert actual == Symbol.STRING;
      return in.readString();
    }
  }

  @Override
  public void skipString() throws IOException {
    Symbol actual = parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      in.skipBytes();
    } else {
      assert actual == Symbol.STRING;
      in.skipString();
    }
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    Symbol actual = parser.advance(Symbol.BYTES);
    if (actual == Symbol.STRING) {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    } else {
      assert actual == Symbol.BYTES;
      return in.readBytes(old);
    }
  }

  @Override
  public void skipBytes() throws IOException {
    Symbol actual = parser.advance(Symbol.BYTES);
    if (actual == Symbol.STRING) {
      in.skipString();
    } else {
      assert actual == Symbol.BYTES;
      in.skipBytes();
    }
  }

  @Override
  public int readEnum() throws IOException {
    parser.advance(Symbol.ENUM);
    Symbol.EnumAdjustAction top = (Symbol.EnumAdjustAction) parser.popSymbol();
    int n = in.readEnum();
    if (top.noAdjustments) {
      // n is used directly as an index into the reader enum's symbols, so it
      // must fall within the reader symbol count.
      if (n < 0 || n >= top.size) {
        throw new AvroTypeException("Enumeration out of range: must be in [0, " + top.size + "), but received " + n);
      }
      return n;
    }
    // Otherwise n indexes the writer-to-reader adjustment table; reject an index
    // outside it rather than letting the array access throw.
    if (n < 0 || n >= top.adjustments.length) {
      throw new AvroTypeException(
          "Enumeration out of range: must be in [0, " + top.adjustments.length + "), but received " + n);
    }
    Object o = top.adjustments[n];
    if (o instanceof Integer) {
      return (Integer) o;
    } else {
      throw new AvroTypeException((String) o);
    }
  }

  @Override
  public int readIndex() throws IOException {
    parser.advance(Symbol.UNION);
    Symbol top = parser.popSymbol();
    final int result;
    if (top instanceof Symbol.UnionAdjustAction) {
      result = ((Symbol.UnionAdjustAction) top).rindex;
      top = ((Symbol.UnionAdjustAction) top).symToParse;
    } else {
      result = in.readIndex();
      top = ((Symbol.Alternative) top).getSymbol(result);
    }
    parser.pushSymbol(top);
    return result;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldOrderAction) {
      return input == Symbol.FIELD_ACTION ? top : null;
    }
    if (top instanceof Symbol.ResolvingAction) {
      Symbol.ResolvingAction t = (Symbol.ResolvingAction) top;
      if (t.reader != input) {
        throw new AvroTypeException("Found " + t.reader + " while looking for " + input);
      } else {
        return t.writer;
      }
    } else if (top instanceof Symbol.SkipAction) {
      Symbol symToSkip = ((Symbol.SkipAction) top).symToSkip;
      parser.skipSymbol(symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = DecoderFactory.get().binaryDecoder(dsa.contents, null);
    } else if (top == Symbol.DEFAULT_END_ACTION) {
      in = backup;
    } else {
      throw new AvroTypeException("Unknown action: " + top);
    }
    return null;
  }

  @Override
  public void skipAction() throws IOException {
    Symbol top = parser.popSymbol();
    if (top instanceof Symbol.ResolvingAction) {
      parser.pushSymbol(((Symbol.ResolvingAction) top).writer);
    } else if (top instanceof Symbol.SkipAction) {
      parser.pushSymbol(((Symbol.SkipAction) top).symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = DecoderFactory.get().binaryDecoder(dsa.contents, null);
    } else if (top == Symbol.DEFAULT_END_ACTION) {
      in = backup;
    }
  }
}
