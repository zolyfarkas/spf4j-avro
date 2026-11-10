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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.spf4j.avro.Conversion;
import org.spf4j.avro.LogicalType;
import org.spf4j.avro.Schema;

/**
 * Pins the memoization of the assignability-based conversion lookups
 * (audit finding A2): {@link GenericData#getConversionByClass} results --
 * including negative ones -- are memoized per datum class (and logical
 * type), and {@link GenericData#addLogicalTypeConversion} must invalidate
 * the memos, since a registration can change the outcome of every prior
 * lookup.
 */
public class TestGenericDataConversionLookupMemo {

  private static final String LT_NAME = "test_memo_lt";

  /** Marker datum base type no default conversion can target. */
  public abstract static class DatumBase {
  }

  /** Datum class: a strict subtype of the base conversion's type. */
  public static final class DatumSub extends DatumBase {
  }

  /** Conversion whose converted type is the datum's supertype. */
  public static final class BaseConversion extends Conversion<DatumBase> {
    @Override
    public Class<DatumBase> getConvertedType() {
      return DatumBase.class;
    }

    @Override
    public String getLogicalTypeName() {
      return LT_NAME;
    }
  }

  /** Conversion whose converted type is the datum class itself. */
  public static final class ExactConversion extends Conversion<DatumSub> {
    @Override
    public Class<DatumSub> getConvertedType() {
      return DatumSub.class;
    }

    @Override
    public String getLogicalTypeName() {
      return LT_NAME;
    }
  }

  private static LogicalType logicalType() {
    Schema schema = Schema.create(Schema.Type.STRING);
    new LogicalType(LT_NAME).addToSchema(schema);
    return schema.getLogicalType();
  }

  @Test
  void twoArgLookupInvalidatesOnRegistration() {
    GenericData data = new GenericData();
    LogicalType lt = logicalType();
    // negative result first: it is memoized, so only invalidation on
    // registration can make the next lookup see the conversion
    assertNull(data.getConversionByClass(DatumSub.class, lt));
    BaseConversion base = new BaseConversion();
    data.addLogicalTypeConversion(base);
    Conversion<DatumSub> first = data.getConversionByClass(DatumSub.class, lt);
    Conversion<DatumSub> second = data.getConversionByClass(DatumSub.class, lt);
    assertSame(base, first);
    // steady state: repeated lookups keep returning the memoized instance
    assertSame(first, second);
    // a more specific (exact-class) registration replaces the memoized
    // supertype result: the exact registration wins
    ExactConversion exact = new ExactConversion();
    data.addLogicalTypeConversion(exact);
    assertSame(exact, data.getConversionByClass(DatumSub.class, lt));
    // the memo is instance state: another model is unaffected
    assertNull(new GenericData().getConversionByClass(DatumSub.class, lt));
  }

  @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
      justification = "the repeated identical lookups are the behavior under test: after the first call the"
          + " result is memoized, so a repeated call must be served by the memo and return the"
          + " identical instance")
  @Test
  void oneArgLookupInvalidatesOnRegistration() {
    GenericData data = new GenericData();
    assertNull(data.getConversionByClass(DatumSub.class));
    BaseConversion base = new BaseConversion();
    data.addLogicalTypeConversion(base);
    Conversion<DatumSub> first = data.getConversionByClass(DatumSub.class);
    Conversion<DatumSub> second = data.getConversionByClass(DatumSub.class);
    assertSame(base, first);
    assertSame(first, second,
        "repeated lookup must be served by the memo, returning the identical instance");
    ExactConversion exact = new ExactConversion();
    data.addLogicalTypeConversion(exact);
    assertSame(exact, data.getConversionByClass(DatumSub.class));
  }
}
