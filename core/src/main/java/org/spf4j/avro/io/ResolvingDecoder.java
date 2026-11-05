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
   * (reader) schema. The underlying extension decoder has already consumed
   * the value from the stream, so this is a grammar-only walk: it consumes
   * exactly the symbols the value's segment occupies in the resolving
   * grammar, without reading anything from the underlying decoder.
   *
   * <p>
   * Terminal schemas advance as the corresponding {@code readXxx} method
   * would. Composite schemas -- previously unsupported here, which broke
   * direct decode of a record-typed logical type (for example inside a
   * map) -- are walked schema-first, in the spirit of the fork's
   * {@code JsonExtensionResolvingDecoder} production-skipping
   * {@code advanceBy}, but over this decoder's resolving grammar instead
   * of the JSON grammar:
   * </p>
   * <ul>
   * <li>a record consumes its {@code FieldOrderAction} and then, in the
   * order that action prescribes (writer order, reader fields only), each
   * field's segment;</li>
   * <li>an array or map consumes its start terminal and then closes its
   * repeater with the end terminal (the repeater represents all elements,
   * however many the extension decoder consumed);</li>
   * <li>a union consumes the {@code UNION} terminal and, for a
   * reader-union, the deterministically adjusted branch. A writer-selected
   * branch (an {@code Alternative} on the stack) cannot be advanced
   * grammar-only -- the branch index lived in the stream the extension
   * decoder has already consumed -- and still throws
   * {@link UnsupportedOperationException}; datum readers never reach it,
   * because they select the branch with {@link #readIndex()} before any
   * direct decode of the branch.</li>
   * </ul>
   *
   * <p>
   * Implicit actions encountered during the walk are dropped, not
   * executed: a {@code SkipAction}'s writer-only field is part of the
   * value the extension decoder already consumed, so performing its
   * stream skip would consume the <em>next</em> value; likewise default
   * start/end actions need no decoder switch for a grammar-only pass. A
   * {@code ResolvingAction} whose reader symbol is the expected terminal
   * (a promotion) occupies that terminal's slot and is consumed as it.
   * </p>
   */
  private void advanceBy(final Schema schema) throws IOException {
    switch (schema.getType()) {
    case NULL:
      advanceTerminal(Symbol.NULL);
      break;
    case BOOLEAN:
      advanceTerminal(Symbol.BOOLEAN);
      break;
    case INT:
      advanceTerminal(Symbol.INT);
      break;
    case LONG:
      advanceTerminal(Symbol.LONG);
      break;
    case FLOAT:
      advanceTerminal(Symbol.FLOAT);
      break;
    case DOUBLE:
      advanceTerminal(Symbol.DOUBLE);
      break;
    case STRING:
      advanceTerminal(Symbol.STRING);
      break;
    case BYTES:
      advanceTerminal(Symbol.BYTES);
      break;
    case FIXED:
      advanceTerminal(Symbol.FIXED);
      break;
    case ENUM:
      advanceTerminal(Symbol.ENUM);
      break;
    case RECORD:
      for (Schema.Field field : advanceToFieldOrder()) {
        advanceBy(field.schema());
      }
      skipTrailingImplicitActions();
      break;
    case ARRAY:
      advanceTerminal(Symbol.ARRAY_START);
      advanceTerminal(Symbol.ARRAY_END);
      break;
    case MAP:
      advanceTerminal(Symbol.MAP_START);
      advanceTerminal(Symbol.MAP_END);
      break;
    case UNION:
      advanceByUnion(schema);
      break;
    default:
      throw new UnsupportedOperationException(
          "Direct extension decode of schema " + schema + " through a ResolvingDecoder is not supported");
    }
  }

  /**
   * Advance the grammar past a union value the extension decoder already
   * consumed. Only the reader-selected form is supported: the grammar's
   * {@code UnionAdjustAction} names the branch to walk. A writer-selected
   * branch cannot be walked, because the extension decoder has already
   * consumed the writer's branch index from the stream.
   */
  private void advanceByUnion(final Schema schema) throws IOException {
    advanceTerminal(Symbol.UNION);
    Symbol top = parser.popSymbol();
    if (top instanceof Symbol.UnionAdjustAction) {
      Symbol.UnionAdjustAction adjust = (Symbol.UnionAdjustAction) top;
      parser.pushSymbol(adjust.symToParse);
      advanceBy(schema.getTypes().get(adjust.rindex));
    } else {
      throw new UnsupportedOperationException(
          "Direct extension decode of a writer-selected union branch of " + schema
              + " through a ResolvingDecoder is not supported: the branch was selected"
              + " by the writer, whose index the extension decoder has already consumed");
    }
  }

  /**
   * Consume one terminal of the resolving grammar, grammar-only: expand
   * sequences, drop implicit/explicit actions without executing them, and
   * match either the terminal itself or a {@code ResolvingAction} carrying
   * it as its reader symbol (a promotion). The end terminal of an array or
   * map matches (and consumes) its repeater. {@code FIXED} and
   * {@code ENUM} also consume the check/adjust action that follows the
   * terminal in the grammar.
   */
  private void advanceTerminal(final Symbol expected) throws IOException {
    for (;;) {
      if (parser.depth() <= 0) {
        throw new AvroTypeException("Attempt to process a " + expected + " but the grammar is exhausted.");
      }
      Symbol top = parser.popSymbol();
      if (top == expected) {
        if (expected == Symbol.FIXED || expected == Symbol.ENUM) {
          parser.popSymbol();
        }
        return;
      }
      if (top instanceof Symbol.ResolvingAction) {
        if (((Symbol.ResolvingAction) top).reader == expected) {
          return;
        }
        throw new AvroTypeException(
            "Found " + ((Symbol.ResolvingAction) top).reader + " while looking for " + expected);
      }
      if (top.kind == Symbol.Kind.TERMINAL) {
        throw new AvroTypeException("Attempt to process a " + expected + " when a " + top + " was expected.");
      }
      if (top.kind == Symbol.Kind.REPEATER) {
        if (expected == ((Symbol.Repeater) top).end) {
          return;
        }
        parser.pushProduction(top);
        continue;
      }
      if (top == Symbol.WRITER_UNION_ACTION) {
        throw new UnsupportedOperationException(
            "Direct extension decode of a writer-selected union through a ResolvingDecoder"
                + " is not supported: the branch was selected by the writer, whose index"
                + " the extension decoder has already consumed");
      }
      if (top.kind == Symbol.Kind.IMPLICIT_ACTION || top.kind == Symbol.Kind.EXPLICIT_ACTION) {
        // Drop without executing: see advanceBy's javadoc. (Enum/fixed
        // check actions land here only when orphaned, which a well-formed
        // grammar pass does not produce.)
        continue;
      }
      if (top.production != null) {
        // A ROOT top is the not-yet-expanded grammar of a root-level
        // value (the value is the whole input, e.g. a logical-typed bytes
        // schema decoded directly or as a field default): expand it like
        // Parser.advance does -- its production carries the root itself
        // as the bottom sentinel, with the value's symbols on top -- and
        // match the terminal they hold.
        parser.pushProduction(top);
        continue;
      }
      throw new AvroTypeException("Attempt to process a " + expected + " when a " + top + " was expected.");
    }
  }

  /** Consume the record segment's field-order action and return its fields. */
  private Schema.Field[] advanceToFieldOrder() throws IOException {
    for (;;) {
      if (parser.depth() <= 0) {
        throw new AvroTypeException("Attempt to process a record but the grammar is exhausted.");
      }
      Symbol top = parser.popSymbol();
      if (top instanceof Symbol.FieldOrderAction) {
        return ((Symbol.FieldOrderAction) top).fields;
      }
      if (top instanceof Symbol.ResolvingAction || top.kind == Symbol.Kind.TERMINAL
          || top.kind == Symbol.Kind.ALTERNATIVE) {
        throw new AvroTypeException("Attempt to process a record when a " + top + " was expected.");
      }
      if (top == Symbol.WRITER_UNION_ACTION) {
        throw new UnsupportedOperationException(
            "Direct extension decode of a writer-selected union through a ResolvingDecoder"
                + " is not supported: the branch was selected by the writer, whose index"
                + " the extension decoder has already consumed");
      }
      if (top.kind == Symbol.Kind.IMPLICIT_ACTION || top.kind == Symbol.Kind.EXPLICIT_ACTION
          || top.kind == Symbol.Kind.REPEATER) {
        if (top.kind == Symbol.Kind.REPEATER) {
          parser.pushProduction(top);
        }
        continue;
      }
      if (top.production != null) {
        // ROOT included, as in advanceTerminal: a root-level record
        // value's grammar is expanded on first use.
        parser.pushProduction(top);
        continue;
      }
      throw new AvroTypeException("Attempt to process a record when a " + top + " was expected.");
    }
  }

  /**
   * Drop implicit actions left on top of the grammar after a record's
   * reader fields have been consumed: the {@code SkipAction}s of
   * writer-only fields that follow them (part of the value the extension
   * decoder already consumed, so their stream skips must not run) and the
   * end action of a default segment.
   */
  private void skipTrailingImplicitActions() {
    while (parser.depth() > 0 && parser.topSymbol().kind == Symbol.Kind.IMPLICIT_ACTION) {
      parser.popSymbol();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * This implementation delegates the read to the wrapped
   * {@link JsonExtensionDecoder} and then advances the resolving grammar
   * past {@code schema}; without an extension decoder it returns
   * {@code null} and leaves the grammar untouched. Subclasses that
   * override it must preserve that pairing: a value consumed from the
   * stream must be matched by exactly one grammar advance.
   * </p>
   */
  @Override
  public BigDecimal readBigDecimal(final Schema schema) throws IOException {
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

  /**
   * {@inheritDoc}
   *
   * <p>
   * This implementation delegates the read to the wrapped
   * {@link JsonExtensionDecoder} and then advances the resolving grammar
   * past {@code schema}; without an extension decoder it returns
   * {@code null} and leaves the grammar untouched. Subclasses that
   * override it must preserve that pairing: a value consumed from the
   * stream must be matched by exactly one grammar advance.
   * </p>
   */
  @Override
  public BigInteger readBigInteger(final Schema schema) throws IOException {
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

  /**
   * {@inheritDoc}
   *
   * <p>
   * This implementation delegates the read to the wrapped
   * {@link JsonExtensionDecoder} and then advances the resolving grammar
   * past {@code schema}. Subclasses that override it must preserve that
   * pairing: a value consumed from the stream must be matched by exactly
   * one grammar advance.
   * </p>
   */
  @Override
  public <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException {
    T result = requireExtensionDecoder().readValue(schema, clasz);
    advanceBy(schema);
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * This implementation delegates the read to the wrapped
   * {@link JsonExtensionDecoder} and then advances the resolving grammar
   * past {@code schema}. Subclasses that override it must preserve that
   * pairing: a value consumed from the stream must be matched by exactly
   * one grammar advance.
   * </p>
   */
  @Override
  public JsonNode readValueAsTree(final Schema schema) throws IOException {
    JsonNode result = requireExtensionDecoder().readValueAsTree(schema);
    advanceBy(schema);
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * This implementation delegates the buffering to the wrapped
   * {@link JsonExtensionDecoder} and then advances the resolving grammar
   * past {@code schema}. Subclasses that override it must preserve that
   * pairing: a value consumed from the stream must be matched by exactly
   * one grammar advance.
   * </p>
   */
  @Override
  public JsonParser bufferValue(final Schema schema) throws IOException {
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
