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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.spf4j.avro.AvroTypeException;
import org.spf4j.avro.LogicalType;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.parsing.JsonGrammarGenerator;
import org.spf4j.avro.io.parsing.Parser;
import org.spf4j.avro.io.parsing.Symbol;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

/**
 * This class extends the JsonDecoder to:
 * <ul>
 * <li>Decode JSON with "natural" encoding of union {null, type} encoding</li>
 * <li>Infer not present attributes by using the default defined in the writer
 * schema</li>
 * <li>Ability to decode the decimal logicalType when naturally encoded in
 * JSON.</li>
 * </ul>
 *
 * <p>
 * Transplanted from the spf4j avro fork (where it backs
 * {@code Schema.validateDefault}), adapted to upstream main:
 * </p>
 * <ul>
 * <li>The fork's {@code ExtendedJsonEncoder.isNullableSingle} /
 * {@code getNullableSingle} helpers are private statics here; the encoder
 * itself is not ported.</li>
 * <li>{@code JsonGrammarGenerator.getRootSymbol} (a fork static) becomes
 * {@code new JsonGrammarGenerator().generate(schema)}.</li>
 * <li>Default-value injection sets the buffered parser's codec from the
 * current parser when it has one; the fork unconditionally used
 * {@code Schema.MAPPER}, which is not visible from this package on
 * main.</li>
 * <li>The fork's scale-prefixed byte layout produced by {@link #readBytes}
 * for JSON numbers (see {@code toBytes}) is preserved verbatim: it is the
 * layout the fork's/spf4j's decimal conversions consume. Upstream's
 * {@code Conversions.DecimalConversion} expects the bare unscaled value
 * instead, so a decimal decoded this way validates (the use
 * {@code Schema.validateDefault} makes of it) but does not yield the same
 * numeric value through upstream's conversion.</li>
 * <li>The byte encoding charset is the fork's
 * {@code JsonDecoder.CHARSET} (ISO-8859-1), inlined; main's JsonDecoder has
 * no such constant. Likewise {@code readValue} / {@code readValueAsTree}
 * read through a static {@code ObjectMapper} where the fork used
 * {@code Schema.FACTORY} / {@code Schema.MAPPER}, which are
 * package-private on main; the parsers here are codec-less, so calling
 * {@code JsonParser.readValueAs*} on them would always throw
 * {@code IllegalStateException}.</li>
 * </ul>
 */
public final class ExtendedJsonDecoder extends JsonDecoder implements JsonExtensionDecoder {

  /**
   * Codec for {@link #readValue} / {@link #readValueAsTree}: the parsers
   * this decoder works with are created from codec-less
   * {@code JsonFactory} instances (see {@link JsonDecoder}), so calling
   * {@code JsonParser.readValueAs*} on them always threw
   * {@code IllegalStateException} ("No ObjectCodec defined"). Reading
   * through this mapper instead works regardless of the parser's codec;
   * it mirrors the fork's use of its {@code Schema.MAPPER}. (The
   * logical-types library worked around the defect by buffering values
   * and converting them with its own mapper.)
   */
  private static final ObjectMapper MAPPER = new ObjectMapper(
      new JsonFactory().enable(JsonParser.Feature.ALLOW_COMMENTS));

  private final boolean lenient;

  public ExtendedJsonDecoder(final Schema schema, final InputStream in) throws IOException {
    this(schema, in, true);
  }

  public ExtendedJsonDecoder(final Schema schema, final InputStream in, boolean lenient) throws IOException {
    super(schema, in);
    this.lenient = lenient;
  }

  public ExtendedJsonDecoder(final Schema schema, final JsonParser in, boolean lenient) throws IOException {
    super(schema, in);
    this.lenient = lenient;
  }

  public ExtendedJsonDecoder(final Schema schema, final String in) throws IOException {
    this(schema, new ByteArrayInputStream(in.getBytes(StandardCharsets.UTF_8)));
  }

  private static boolean isNullableSingle(final Symbol.Alternative top) {
    return top.size() == 2 && ("null".equals(top.getLabel(0)) || "null".equals(top.getLabel(1)));
  }

  private static String getNullableSingle(final Symbol.Alternative top) {
    final String label = top.getLabel(0);
    return "null".equals(label) ? top.getLabel(1) : label;
  }

  /**
   * Overwrite this function to support decoding of: union {null, anyType}
   */
  @Override
  public int readIndex() throws IOException {
    super.advance(Symbol.UNION);
    JsonParser lin = this.in;
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    final JsonToken currentToken = lin.getCurrentToken();
    if (currentToken == JsonToken.VALUE_NULL) {
      label = "null";
    } else if (isNullableSingle(a)) {
      label = getNullableSingle(a);
    } else if (currentToken == JsonToken.START_OBJECT && lin.nextToken() == JsonToken.FIELD_NAME) {
      label = lin.getText();
      lin.nextToken();
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw error("start-union");
    }
    int n = a.findLabel(label);
    if (n < 0) {
      throw new AvroTypeException("Unknown union branch " + label);
    }
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

  /**
   * Overwrite to inject default values.
   */
  @Override
  public Symbol doAction(final Symbol input, final Symbol top) throws IOException {
    final JsonParser in = this.in;
    if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      String name = fa.fname;
      if (currentReorderBuffer != null) {
        TokenBuffer node = currentReorderBuffer.savedFields.remove(name);
        if (node != null) {
          currentReorderBuffer.origParser = in;
          this.in = node.asParserOnFirstToken();
          return null;
        }
        for (String alias : fa.aliases) {
          node = currentReorderBuffer.savedFields.remove(alias);
          if (node != null) {
            currentReorderBuffer.origParser = in;
            this.in = node.asParserOnFirstToken();
            return null;
          }
        }
      }
      if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
        do {
          String fn = in.getText();
          in.nextToken();
          if (name.equals(fn) || fa.aliases.contains(fn)) {
            return null;
          } else {
            if (currentReorderBuffer == null) {
              currentReorderBuffer = new JsonDecoder.ReorderBuffer();
            }
            TokenBuffer tokenBuffer = TokenBuffer.asCopyOfValue(in);
            // Moves the parser to the end of the current event e.g. END_OBJECT
            currentReorderBuffer.savedFields.put(fn, tokenBuffer);
            in.nextToken();
          }
        } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
        if (injectDefaultValueIfAvailable(in, fa)) {
          return null;
        }
        throw new AvroTypeException(
            "Expected field name not found: " + fa.fname + " instead found " + in.getCurrentToken());
      } else {
        if (injectDefaultValueIfAvailable(in, fa)) {
          return null;
        }
        throw new AvroTypeException(
            "Expected field name not found: " + fa.fname + " instead found " + in.getCurrentToken());
      }
    } else if (top == Symbol.FIELD_END) {
      if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
        this.in = currentReorderBuffer.origParser;
        currentReorderBuffer.origParser = null;
      }
    } else if (top == Symbol.RECORD_START) {
      if (in.getCurrentToken() == JsonToken.START_OBJECT) {
        in.nextToken();
        reorderBuffers.push(currentReorderBuffer);
        currentReorderBuffer = null;
      } else {
        throw error("record-start");
      }
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      if (in.getCurrentToken() == JsonToken.END_OBJECT) {
        in.nextToken();
        if (top == Symbol.RECORD_END) {
          if (!lenient && currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
            throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
          }
          currentReorderBuffer = reorderBuffers.pop();
        }
      } else {
        if (lenient && top == Symbol.RECORD_END) {
          if (in.getCurrentToken() != JsonToken.END_OBJECT) {
            int level = 1;
            while (level > 0) {
              JsonToken nextToken = in.nextToken();
              if (nextToken == null) {
                break;
              } else if (nextToken == JsonToken.START_OBJECT) {
                level++;
              } else if (nextToken == JsonToken.END_OBJECT) {
                level--;
              }
            }
          }
          in.nextToken();
          currentReorderBuffer = reorderBuffers.pop();
        } else {
          throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
        }
      }
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  @SuppressFBWarnings(value = "EXS_EXCEPTION_SOFTENING_NO_CHECKED",
      justification = "this API's documented error model is unchecked: exceptions are wrapped in"
          + "AvroRuntimeException at the boundary so callers program against the unchecked contract"
      + " The real fix is a major rework, postponed per the user's 2026-11-05 decision.")
  @Override
  AvroTypeException error(String type) {
    try {
      return new AvroTypeException("Expected " + type + ". Got " + in.getCurrentToken() + " token value = "
          + in.getText() + " at " + in.getCurrentLocation());
    } catch (IOException ex) {
      UncheckedIOException ioEx = new UncheckedIOException(ex);
      ioEx.addSuppressed(super.error(type));
      throw ioEx;
    }
  }

  @SuppressFBWarnings(value = "BL_BURYING_LOGIC",
      justification = "the compound condition is the specification's rule for this decision, kept in one expression"
          + "so the rule reads as written")
  private boolean injectDefaultValueIfAvailable(final JsonParser in, Symbol.FieldAdjustAction action)
      throws IOException {
    JsonNode defVal = action.defaultValue;
    if (null != defVal) {
      JsonParser traverse = defVal.traverse();
      traverse.nextToken();
      TokenBuffer result = TokenBuffer.asCopyOfValue(traverse);
      ObjectCodec codec = in.getCodec();
      if (codec != null) {
        result.setCodec(codec);
      }
      if (currentReorderBuffer == null) {
        currentReorderBuffer = new ReorderBuffer();
      }
      currentReorderBuffer.origParser = in;
      this.in = result.asParserOnFirstToken();
      return true;
    }
    return false;
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
          && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
          && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    in.nextToken();
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    switch (currentToken) {
    case VALUE_STRING:
      byte[] result = readByteArray();
      in.nextToken();
      return ByteBuffer.wrap(result);
    case VALUE_NUMBER_INT:
      BigInteger bigIntegerValue = in.getBigIntegerValue();
      in.nextToken();
      return ByteBuffer.wrap(bigIntegerValue.toByteArray());
    case VALUE_NUMBER_FLOAT:
      BigDecimal decimalValue = in.getDecimalValue();
      in.nextToken();
      return toBytes(decimalValue);
    default:
      throw error("bytes");
    }
  }

  private static ByteBuffer toBytes(BigDecimal decimal) {
    byte[] unscaledValue = decimal.unscaledValue().toByteArray();
    ByteBuffer buf = ByteBuffer.allocate(5 + unscaledValue.length);
    writeInt(decimal.scale(), buf);
    buf.put(unscaledValue);
    buf.flip();
    return buf;
  }

  @SuppressFBWarnings(value = "ICAST_QUESTIONABLE_UNSIGNED_RIGHT_SHIFT",
      justification = "zigzag varint encoding: the unsigned shift and the byte truncation are the encoding's"
          + " definition, and the branch above bounds the value to the two-byte form")
  private static void writeInt(final int n, final ByteBuffer buf) {
    int val = (n << 1) ^ (n >> 31);
    if ((val & ~0x7F) == 0) {
      buf.put((byte) val);
      return;
    } else if ((val & ~0x3FFF) == 0) {
      buf.put((byte) (0x80 | val));
      buf.put((byte) (val >>> 7));
      return;
    }
    byte[] tmp = new byte[5];
    int len = BinaryData.encodeInt(n, tmp, 0);
    buf.put(tmp, 0, len);
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    if (currentToken == JsonToken.VALUE_STRING || currentToken == JsonToken.VALUE_NUMBER_INT
        || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }

  // readBigInteger/readBigDecimal advance the grammar to the value BEFORE
  // inspecting the token (the fork inspected the token first). Only a
  // grammar advance positions the token at a nested value -- e.g. a record
  // field -- so the fork's order could only ever see a value that a
  // previous advance had already positioned, and returned null for every
  // record field read through a datum reader. The advance-first order is
  // the one every other read method on this decoder uses. Consequence: a
  // null return ("the value is not a JSON number") leaves the grammar
  // advanced past the value's terminal with the value token unconsumed,
  // so a caller must not fall back to a grammar-driven read of the same
  // value through this decoder; through the datum readers this narrows
  // extension-decoder decimal/bigint reads to the natural (number) form.

  @Override
  public BigInteger readBigInteger(final Schema schema) throws IOException {
    advanceBy(schema);
    JsonToken currentToken = in.getCurrentToken();
    BigInteger result;
    if (currentToken == JsonToken.VALUE_NUMBER_INT) {
      result = in.getBigIntegerValue();
    } else {
      return null;
    }
    in.nextToken();
    return result;
  }

  @Override
  public BigDecimal readBigDecimal(final Schema schema) throws IOException {
    advanceBy(schema);
    JsonToken currentToken = in.getCurrentToken();
    BigDecimal result;
    switch (currentToken) {
    case VALUE_NUMBER_INT:
    case VALUE_NUMBER_FLOAT:
      result = in.getDecimalValue();
      break;
    default:
      return null;
    }
    in.nextToken();
    return result;
  }

  @Override
  public <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException {
    advanceBy(schema);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      // probably encoded with regular encoder, will be best effort here.
      String text = in.getText();
      T result;
      LogicalType lt = schema.getLogicalType();
      if (lt != null && "json_any".equals(lt.getName())) {
        result = clasz.cast(text);
      } else {
        switch (schema.getType()) {
        case STRING:
          result = MAPPER.readValue(text, clasz);
          break;
        case BYTES:
          result = MAPPER.readValue(text.getBytes(StandardCharsets.ISO_8859_1), clasz);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported schema " + schema);
        }
      }
      in.nextToken();
      return result;
    }
    T result = MAPPER.readValue(in, clasz);
    in.nextToken();
    return result;
  }

  public void advanceBy(final Schema schema) throws IOException {
    Schema.Type type = schema.getType();
    switch (type) {
    case BYTES:
      advance(Symbol.BYTES);
      break;
    case STRING:
      advance(Symbol.STRING);
      break;
    case LONG:
      advance(Symbol.LONG);
      break;
    case INT:
      advance(Symbol.INT);
      break;
    default:
      Symbol rootSymbol = new JsonGrammarGenerator().generate(schema);
      Parser p = new Parser(rootSymbol, null);
      int countToFirstTerminal = p.countToFirstTerminal();
      Symbol theTerminal = p.lastSymbol();
      int sleft = p.countToEnd();
      int countToFirstTerminal2 = parser.countToFirstTerminal();
      Symbol theTerminal2 = parser.lastSymbol();
      if (theTerminal != theTerminal2) {
        throw new IllegalStateException("expected " + theTerminal + " got " + theTerminal2);
      }
      parser.goBack(countToFirstTerminal2);
      int advance = countToFirstTerminal2 - countToFirstTerminal;
      parser.advance(advance);
      if (advance >= 0) {
        parser.skip(sleft + countToFirstTerminal - 1);
      } else {
        parser.skip(sleft + countToFirstTerminal2 - 1);
      }
      break;
    }
  }

  @Override
  public JsonNode readValueAsTree(final Schema schema) throws IOException {
    advanceBy(schema);
    JsonNode result = MAPPER.readTree(in);
    in.nextToken();
    return result;
  }

  @Override
  public JsonParser bufferValue(Schema schema) throws IOException {
    advanceBy(schema);
    JsonParser parser = TokenBuffer.asCopyOfValue(in).asParserOnFirstToken();
    in.nextToken();
    return parser;
  }

  @Override
  public String toString() {
    return "ExtendedJsonDecoder{" + "lenient=" + lenient + " at " + in.getCurrentLocation() + '}';
  }

}
