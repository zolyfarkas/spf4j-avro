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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

/**
 * Spec tests for the fork's parse-time validation of the enum
 * {@code stringSymbols} property.
 *
 * <p>
 * In the fork, {@code Schema.EnumSchema.addProp("stringSymbols", ...)}
 * dispatches to {@code setStringSymbols}, which validates while the schema
 * is being parsed: a {@code stringSymbols} key that is not a declared enum
 * symbol is rejected, and a string value that would map two different
 * ordinals (a duplicate assignment, or a collision with another symbol's
 * own name) is rejected, both with {@link AvroTypeException}.
 *
 * <p>
 * The seed computes string symbols from the property on demand (the ported
 * a49c5c2b3 codegen contract) but performs no parse-time validation, so the
 * rejection tests below fail on the seed: the invalid schemas parse
 * silently. The acceptance test pins the part of the contract the seed
 * already honors.
 */
public class TestEnumStringSymbolsValidation {

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
  }

  @Test
  public void testValidStringSymbolsAccepted() {
    Schema schema = parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"],"
        + "\"stringSymbols\":{\"A\":\"alpha\"}}");
    assertEquals("alpha", schema.getEnumStringSymbol("A"));
    assertEquals("B", schema.getEnumStringSymbol("B"));
    assertEquals(Arrays.asList("alpha", "B"), schema.getEnumStringSymbols());
  }

  @Test
  public void testUnknownStringSymbolKeyRejected() {
    try {
      parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"],"
          + "\"stringSymbols\":{\"ZZZ\":\"x\"}}");
      fail("expected AvroTypeException for a stringSymbols key that is not an enum symbol");
    } catch (AvroTypeException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains("enum value referenced in stringSymbol not defined: ZZZ"));
    }
  }

  @Test
  public void testDuplicateStringSymbolValueRejected() {
    try {
      parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"],"
          + "\"stringSymbols\":{\"A\":\"same\",\"B\":\"same\"}}");
      fail("expected AvroTypeException for two symbols mapped to the same string");
    } catch (AvroTypeException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Same string same pointing to"));
    }
  }

  @Test
  public void testStringSymbolCollidingWithSymbolNameRejected() {
    // "B" is already the string symbol of symbol B (ordinal 1); assigning it
    // to A (ordinal 0) must be rejected just like a duplicate assignment.
    try {
      parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"],"
          + "\"stringSymbols\":{\"A\":\"B\"}}");
      fail("expected AvroTypeException for a string symbol colliding with another symbol's name");
    } catch (AvroTypeException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Same string B pointing to"));
    }
  }
}
