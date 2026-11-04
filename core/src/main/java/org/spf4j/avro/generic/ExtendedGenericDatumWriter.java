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

import static org.spf4j.avro.Schema.Field.NULL_DEFAULT_VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.spf4j.avro.JsonProperties;
import org.spf4j.avro.Schema;
import org.spf4j.avro.io.Encoder;
import org.spf4j.avro.io.ExtendedJsonEncoder;
import org.spf4j.avro.io.parsing.Parser;
import org.spf4j.avro.io.parsing.Symbol;

/**
 * Extension of the Avro {@link GenericDatumWriter} that does the following
 * extras over the base class: if the value being written equals the schema
 * default we don't write the field or value at all. (The union-of-null
 * unwrapping the fork's class comment also mentions is a property of
 * {@link ExtendedJsonEncoder}, not of this writer.)
 *
 * <p>
 * Ported from the fork (commit d68e6404c's class, as of tag 1.10.0.12p).
 * The default-skipping applies only when the encoder is an
 * {@link ExtendedJsonEncoder}: the skip is implemented by advancing the
 * encoder's validating parser past the field's grammar symbols without
 * emitting anything, a trick only the extended JSON grammar supports.
 * With any other encoder this writer behaves exactly as
 * {@link GenericDatumWriter}.
 * </p>
 *
 * <p>
 * Adaptations vs. the fork, none of which change the skipping semantics:
 * </p>
 * <ul>
 * <li>The {@code (GenericData)} constructor delegates to this tree's
 * {@code protected GenericDatumWriter(GenericData)} (the fork's base class
 * has a public one).</li>
 * <li>The field value is read through this tree's state-carrying
 * {@code GenericData.getField(datum, name, pos, state)}, matching this
 * tree's {@code GenericDatumWriter.writeField}.</li>
 * <li>Null-default sentinel: the fork redefined
 * {@code Field.NULL_DEFAULT_VALUE} to <em>be</em>
 * {@code JsonProperties.NULL_VALUE}; this tree keeps upstream's distinct
 * {@code NULL_DEFAULT_VALUE} marker and {@code Field.defaultVal()} returns
 * {@code JsonProperties.NULL_VALUE} for a JSON {@code null} default, so
 * {@link #equals(Object, Object)} treats both sentinels as "the default is
 * null".</li>
 * <li>The fork's {@code javax.annotation.Nonnull} markers are dropped, as
 * in the earlier ports.</li>
 * </ul>
 *
 * @author zfarkas
 */
public class ExtendedGenericDatumWriter<D> extends GenericDatumWriter<D> {

  public ExtendedGenericDatumWriter(final GenericData data) {
    super(data);
  }

  public ExtendedGenericDatumWriter(final Schema root) {
    super(root);
  }

  public ExtendedGenericDatumWriter(final Schema root, final GenericData data) {
    super(root, data);
  }

  public static boolean equals(Object a, Object defaultValue) {
    if (a == defaultValue) {
      return true;
    }
    if ((NULL_DEFAULT_VALUE == defaultValue || JsonProperties.NULL_VALUE == defaultValue) && a == null) {
      return true;
    }
    boolean result = a != null && a.equals(defaultValue);
    if (result) {
      return true;
    }
    if ((a instanceof Enum || a instanceof GenericEnumSymbol) && defaultValue != null) {
      return a.toString().equals(defaultValue.toString());
    } else if (a instanceof CharSequence && defaultValue instanceof CharSequence) {
      return equals((CharSequence) a, (CharSequence) defaultValue);
    } else {
      return false;
    }
  }

  public static boolean equals(final CharSequence s, final CharSequence t) {
    final int sl = s.length();
    final int tl = t.length();
    if (sl != tl) {
      return false;
    } else {
      for (int i = 0; i < sl; i++) {
        if (s.charAt(i) != t.charAt(i)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Scratch space for the parser-stack surgery in {@link #writeField}: the
   * symbols popped while locating the field's {@code FieldAdjustAction},
   * held per writing thread (the donor's design; the list is cleared at the
   * start of every skip, so no state survives a write).
   */
  private static final ThreadLocal<List<Symbol>> HOLDINGS = new ThreadLocal<List<Symbol>>() {
    @Override
    protected List<Symbol> initialValue() {
      return new ArrayList<>(8);
    }
  };

  /**
   * Overridden to skip serializing fields that have default values.
   */
  @Override
  protected void writeField(final Object datum, final Schema.Field f, final Encoder out, final Object state)
      throws IOException {
    GenericData data = getData();
    Object defaultValue = f.defaultVal();
    if (defaultValue != null) {
      Object value = data.getField(datum, f.name(), f.pos(), state);

      if (equals(value, defaultValue) && out instanceof ExtendedJsonEncoder) {
        Parser parser = ((ExtendedJsonEncoder) out).getParser();
        Symbol topSymbol = parser.topSymbol();
        switch (topSymbol.kind) {
        case TERMINAL:
        case IMPLICIT_ACTION:
          break;
        default:
          // expand production
          final Symbol nextSymbol = topSymbol.production[topSymbol.production.length - 1];
          if (nextSymbol instanceof Symbol.ImplicitAction) {
            if (((Symbol.ImplicitAction) nextSymbol).isTrailing) {
              throw new IllegalStateException("Cannot start with a trailing implicit" + topSymbol);
            } else {
              parser.advance(nextSymbol);
            }
          } else {
            throw new IllegalStateException("Invalid state " + topSymbol);
          }
          parser.pushSymbol(nextSymbol);
        }
        List<Symbol> holdings = HOLDINGS.get();
        holdings.clear();
        Symbol advanceTo = null;
        boolean done = false;
        while (!done) {
          if (parser.depth() > 0) {
            advanceTo = parser.popSymbol();
            if (advanceTo instanceof Symbol.FieldAdjustAction
                && ((Symbol.FieldAdjustAction) advanceTo).fname.equals(f.name())
                && ((Symbol.FieldAdjustAction) advanceTo).rindex == f.pos()) {
              done = true;
            }
            holdings.add(advanceTo);
          } else {
            done = true;
          }
        }
        for (int i = holdings.size() - 1; i >= 0; i--) {
          parser.pushSymbol(holdings.get(i));
        }
        parser.advance(advanceTo);
        int count = 1;
        while (count > 0) {
          Symbol currentSymbol = parser.popSymbol();
          if (currentSymbol.getClass() == Symbol.ImplicitAction.class) {
            if (((Symbol.ImplicitAction) currentSymbol).isTrailing) {
              count--;
            } else {
              count++;
            }
          }
        }
      } else {
        super.writeField(datum, f, out, state);
      }
    } else {
      super.writeField(datum, f, out, state);
    }
  }
}
