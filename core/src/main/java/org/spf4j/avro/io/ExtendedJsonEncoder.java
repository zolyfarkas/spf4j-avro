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
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.spf4j.avro.AvroTypeException;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.parsing.JsonGrammarGenerator;
import org.spf4j.avro.io.parsing.Parser;
import org.spf4j.avro.io.parsing.Symbol;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * This class extends the JsonEncoder to write the "natural" JSON form
 * that {@link ExtendedJsonDecoder} reads:
 * <ul>
 * <li>Unions of exactly {@code [null, T]} / {@code [T, null]} are written
 * as the bare value (or bare {@code null}), not Avro's standard
 * {@code {"T": value}} wrapper; all other unions keep the wrapper.</li>
 * <li>Decimal and bigint values can be written as raw JSON numbers via
 * {@link JsonExtensionEncoder}.</li>
 * </ul>
 *
 * <p>
 * Ported from the spf4j avro fork's encoder of the same name, adapted to
 * upstream main the way {@link ExtendedJsonDecoder} adapted the decoder:
 * </p>
 * <ul>
 * <li>{@code JsonGrammarGenerator.getRootSymbol} (a fork static) becomes
 * {@code new JsonGrammarGenerator().generate(schema)}.</li>
 * <li>The fork accessed {@code JsonEncoder}'s generator directly; on main
 * that field is private, so it is widened to package-private in
 * {@link JsonEncoder} as the minimal seam (the grammar {@code parser}
 * field already is package-private). Upstream's public API is
 * unchanged.</li>
 * <li>The donor's {@code writeIndex} does not consult main's
 * {@code includeNamespace} flag: a non-nullable union branch is always
 * wrapped, a nullable-single branch never is. Kept as the donor has
 * it.</li>
 * </ul>
 */
public final class ExtendedJsonEncoder extends JsonEncoder implements JsonExtensionEncoder {

  public ExtendedJsonEncoder(final Schema sc, final OutputStream out) throws IOException {
    super(sc, out);
  }

  public ExtendedJsonEncoder(final Schema sc, final OutputStream out, final boolean pretty) throws IOException {
    super(sc, out, pretty);
  }

  public ExtendedJsonEncoder(final Schema sc, final JsonGenerator out) throws IOException {
    super(sc, out);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the accessor is the point of this encoder: interleaved writers share the one"
          + " Jackson parser/generator positioned on the stream being written; the parser is not"
          + " internal state separable from the output itself")
  public Parser getParser() {
    return parser;
  }

  public static boolean isNullableSingle(final Symbol.Alternative top) {
    return top.size() == 2 && ("null".equals(top.getLabel(0)) || "null".equals(top.getLabel(1)));
  }

  public static String getNullableSingle(final Symbol.Alternative top) {
    final String label = top.getLabel(0);
    return "null".equals(label) ? top.getLabel(1) : label;
  }

  /**
   * Overwritten to support natural encoding of union {null, anyType}: a
   * nullable-single union is written bare; any other union keeps the
   * standard {@code {"branch": value}} wrapper.
   */
  @Override
  public void writeIndex(final int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbol.NULL && !isNullableSingle(top)) {
      out.writeStartObject();
      out.writeFieldName(top.getLabel(unionIndex));
      parser.pushSymbol(Symbol.UNION_END);
    }
    parser.pushSymbol(symbol);
  }

  @Override
  public void writeDecimal(final BigDecimal decimal, final Schema schema) throws IOException {
    advanceBy(schema);
    out.writeNumber(decimal);
  }

  @Override
  public void writeBigInteger(final BigInteger value, final Schema schema) throws IOException {
    advanceBy(schema);
    out.writeNumber(value);
  }

  @Override
  public void writeValue(final Object value, final Schema schema) throws IOException {
    if (value == null) {
      throw new AvroTypeException("value cannot be null, must be " + schema);
    }
    advanceBy(schema);
    out.writeObject(value);
  }

  public void advanceBy(final Schema schema) throws IOException {
    Schema.Type type = schema.getType();
    switch (type) {
    case BYTES:
      parser.advance(Symbol.BYTES);
      break;
    case STRING:
      parser.advance(Symbol.STRING);
      break;
    case LONG:
      parser.advance(Symbol.LONG);
      break;
    case INT:
      parser.advance(Symbol.INT);
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

}
