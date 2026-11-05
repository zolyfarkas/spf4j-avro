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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

/**
 * Pins the {@code allowUndefinedLogicalTypes} semantics (spf4j avro fork,
 * commit ef9086f52, with the ambient opt-ins removed): by default an
 * undefined logical type fails schema parsing; the only opt-in is the
 * per-instance {@link Schema.Parser#setAllowUndefinedLogicalTypes(boolean)}
 * setting. There is no thread-local, system property or other shared state,
 * so one parser's setting cannot leak into another parser, on this thread
 * or any other.
 */
public class TestAllowUndefinedLogicalTypes {

  private static final String UNDEFINED_LT_SCHEMA = "{\"type\":\"string\",\"logicalType\":\"undefined-test-lt\"}";

  private static final String DEFINED_LT_SCHEMA = "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}";

  private static void assertParseThrows(final Schema.Parser parser) {
    try {
      parser.parse(UNDEFINED_LT_SCHEMA);
      fail("expected IllegalArgumentException for an undefined logical type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Undefined logical type undefined-test-lt for "));
    }
  }

  private static void assertTolerantParses(final Schema.Parser parser) {
    assertNull(parser.parse(UNDEFINED_LT_SCHEMA).getLogicalType());
  }

  @Test
  public void defaultParseOfUndefinedLogicalTypeThrows() {
    assertParseThrows(new Schema.Parser());
  }

  @Test
  public void perParserOptInSucceedsAndRetainsLogicalTypeName() {
    Schema schema = new Schema.Parser().setAllowUndefinedLogicalTypes(true).parse(UNDEFINED_LT_SCHEMA);
    // tolerated: no LogicalType instance, but the name stays on the schema
    assertNull(schema.getLogicalType());
    assertTrue(schema.toString(), schema.toString().contains("\"logicalType\":\"undefined-test-lt\""));
  }

  @Test
  public void definedLogicalTypeStillResolvesByDefault() {
    Schema schema = new Schema.Parser().parse(DEFINED_LT_SCHEMA);
    assertEquals("timestamp-millis", schema.getLogicalType().getName());
  }

  @Test
  public void definedLogicalTypeStillResolvesWithOptIn() {
    Schema schema = new Schema.Parser().setAllowUndefinedLogicalTypes(true).parse(DEFINED_LT_SCHEMA);
    assertEquals("timestamp-millis", schema.getLogicalType().getName());
  }

  @Test
  public void parserSettingIsInstanceScoped() {
    Schema.Parser strict = new Schema.Parser();
    Schema.Parser tolerant = new Schema.Parser().setAllowUndefinedLogicalTypes(true);
    // alternating use: each parser keeps its own setting
    assertParseThrows(strict);
    assertTolerantParses(tolerant);
    assertParseThrows(strict);
    assertTolerantParses(tolerant);
    // changing one parser's setting does not touch the other
    tolerant.setAllowUndefinedLogicalTypes(false);
    assertParseThrows(tolerant);
    strict.setAllowUndefinedLogicalTypes(true);
    assertNull(strict.parse(UNDEFINED_LT_SCHEMA).getLogicalType());
    assertParseThrows(tolerant);
  }

  @Test
  @SuppressFBWarnings(value = "UTAO_JUNIT_ASSERTION_ODDITIES_NO_ASSERT",
      justification = "the assertions execute in the two worker threads (each parse outcome is checked"
          + " there); this thread fails the test via Assertions.fail on any captured worker error --"
          + " the detector counts neither")
  public void parsersWithDifferentSettingsDoNotInterfereAcrossThreads() throws Exception {
    final int iterations = 50;
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> tolerantError = new AtomicReference<>();
    final AtomicReference<Throwable> strictError = new AtomicReference<>();
    Thread tolerant = new Thread(() -> {
      try {
        Schema.Parser parser = new Schema.Parser().setAllowUndefinedLogicalTypes(true);
        start.await();
        for (int i = 0; i < iterations; i++) {
          if (parser.parse(UNDEFINED_LT_SCHEMA).getLogicalType() != null) {
            throw new AssertionError("tolerant parse produced a LogicalType");
          }
        }
      } catch (Throwable t) {
        tolerantError.set(t);
      }
    });
    Thread strict = new Thread(() -> {
      try {
        Schema.Parser parser = new Schema.Parser();
        start.await();
        for (int i = 0; i < iterations; i++) {
          try {
            parser.parse(UNDEFINED_LT_SCHEMA);
            throw new AssertionError("strict parse tolerated an undefined logical type");
          } catch (IllegalArgumentException expected) {
            // strict behavior, every time
          }
        }
      } catch (Throwable t) {
        strictError.set(t);
      }
    });
    tolerant.start();
    strict.start();
    start.countDown();
    long joinMillis = TimeUnit.SECONDS.toMillis(30);
    tolerant.join(joinMillis);
    strict.join(joinMillis);
    Throwable tolerantFailure = tolerantError.get();
    if (tolerantFailure != null) {
      org.junit.jupiter.api.Assertions.fail("tolerant thread failed", tolerantFailure);
    }
    Throwable strictFailure = strictError.get();
    if (strictFailure != null) {
      org.junit.jupiter.api.Assertions.fail("strict thread failed", strictFailure);
    }
  }
}
