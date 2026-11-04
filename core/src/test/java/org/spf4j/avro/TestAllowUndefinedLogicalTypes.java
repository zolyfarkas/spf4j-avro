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

import java.io.StringReader;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;

/**
 * Pins the {@code allowUndefinedLogicalTypes} port (spf4j avro fork, commit
 * ef9086f52): by default an undefined logical type fails schema parsing; the
 * opt-in surfaces (per-parser setting, per-parse argument, thread-local
 * override, system property) tolerate it, scoped so the tolerance cannot
 * leak into other parses or threads.
 */
public class TestAllowUndefinedLogicalTypes {

  private static final String UNDEFINED_LT_SCHEMA = "{\"type\":\"string\",\"logicalType\":\"undefined-test-lt\"}";

  @After
  public void clearAmbientOptIns() {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(null);
    System.clearProperty("allowUndefinedLogicalTypes");
  }

  private static void assertDefaultParseThrows() {
    try {
      new Schema.Parser().parse(UNDEFINED_LT_SCHEMA);
      fail("expected IllegalArgumentException for an undefined logical type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Undefined logical type undefined-test-lt for "));
    }
  }

  @Test
  public void defaultParseOfUndefinedLogicalTypeThrows() {
    assertDefaultParseThrows();
  }

  @Test
  public void perParserOptInSucceedsAndRetainsLogicalTypeName() {
    Schema schema = new Schema.Parser().setAllowUndefinedLogicalTypes(true).parse(UNDEFINED_LT_SCHEMA);
    // tolerated: no LogicalType instance, but the name stays on the schema
    assertNull(schema.getLogicalType());
    assertTrue(schema.toString(), schema.toString().contains("\"logicalType\":\"undefined-test-lt\""));
  }

  @Test
  public void perParseOptInSucceedsAndDoesNotLeakIntoParser() throws Exception {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(new StringReader(UNDEFINED_LT_SCHEMA), true);
    assertNull(schema.getLogicalType());
    // the same parser is back to strict afterwards
    try {
      parser.parse(UNDEFINED_LT_SCHEMA);
      fail("expected IllegalArgumentException after the per-parse opt-in was restored");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Undefined logical type undefined-test-lt"));
    }
  }

  @Test
  public void threadLocalOptInDoesNotLeakIntoLaterDefaultParseOnSameThread() {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(Boolean.TRUE);
    Schema schema = new Schema.Parser().parse(UNDEFINED_LT_SCHEMA);
    assertNull(schema.getLogicalType());
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(null);
    assertDefaultParseThrows();
  }

  @Test
  public void threadLocalOptInDoesNotLeakAcrossThreads() throws Exception {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(Boolean.TRUE);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    Thread other = new Thread(() -> {
      try {
        new Schema.Parser().parse(UNDEFINED_LT_SCHEMA);
      } catch (Throwable t) {
        error.set(t);
      }
    });
    other.start();
    other.join();
    assertTrue("expected IllegalArgumentException on the other thread, got: " + error.get(),
        error.get() instanceof IllegalArgumentException);
    // ... while this thread still parses under its own opt-in
    assertNull(new Schema.Parser().parse(UNDEFINED_LT_SCHEMA).getLogicalType());
  }

  @Test
  public void systemPropertyFallbackOptIn() {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(null);
    System.setProperty("allowUndefinedLogicalTypes", "true");
    try {
      assertNull(new Schema.Parser().parse(UNDEFINED_LT_SCHEMA).getLogicalType());
    } finally {
      System.clearProperty("allowUndefinedLogicalTypes");
    }
    assertDefaultParseThrows();
  }

  @Test
  public void definedLogicalTypeStillResolvesByDefault() {
    Schema schema = new Schema.Parser().parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
    assertEquals("timestamp-millis", schema.getLogicalType().getName());
  }
}
