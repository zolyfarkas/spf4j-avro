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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Pins the behavior of fork commit 4724e681a's {@code Schema.Field}
 * copy-constructor hunk: a field copied onto a different schema must
 * report the default value <em>as converted for the copy's schema</em>,
 * not the Java value computed for the original field's schema.
 *
 * <p>
 * There is nothing to port for this in this tree: unlike the fork, this
 * tree's {@link Schema.Field} caches no {@code defaultJavaVal} at all —
 * {@link Schema.Field#defaultVal()} recomputes
 * {@code JacksonUtils.toObject(defaultValue, schema)} against the
 * field's own schema on every call, so a copy (which carries the new
 * schema) recomputes by construction, for every caller and not just at
 * copy time. The fork's bug — the copy constructor carrying the
 * original's cached {@code defaultJavaVal} over verbatim — therefore
 * cannot occur here. These tests exist so that a future caching
 * optimization cannot silently reintroduce it: each case uses a schema
 * pair whose Java conversions of the same JSON default observably
 * differ, so a stale cached value would fail the assertion.
 * </p>
 */
public class TestFieldDefaultJavaValRecompute {

  @Test
  public void testCopiedFieldDefaultRecomputedIntToLong() {
    Schema.Field original = new Schema.Field("n", Schema.create(Schema.Type.INT), null, 42);
    assertEquals(Integer.valueOf(42), original.defaultVal());

    Schema.Field copy = new Schema.Field(original, Schema.create(Schema.Type.LONG));
    // the JSON default is unchanged; its Java form must follow the copy's schema
    assertEquals(original.defaultValue(), copy.defaultValue());
    Long copyDefault = assertInstanceOf(Long.class, copy.defaultVal(),
        "expected a Long default for a long field");
    assertEquals(Long.valueOf(42L), copyDefault);
    // and the original is unaffected by the copy
    assertEquals(Integer.valueOf(42), original.defaultVal());
  }

  @Test
  public void testCopiedFieldDefaultRecomputedStringToBytes() {
    Schema.Field original = new Schema.Field("s", Schema.create(Schema.Type.STRING), null, "abc");
    assertEquals("abc", original.defaultVal());

    Schema.Field copy = new Schema.Field(original, Schema.create(Schema.Type.BYTES));
    byte[] copyDefault = assertInstanceOf(byte[].class, copy.defaultVal(),
        "expected a byte[] default for a bytes field");
    assertArrayEquals("abc".getBytes(StandardCharsets.ISO_8859_1), copyDefault);
  }

  @Test
  public void testCopiedFieldDefaultRecomputedIntToDouble() {
    Schema.Field original = new Schema.Field("d", Schema.create(Schema.Type.INT), null, 7);
    Schema.Field copy = new Schema.Field(original, Schema.create(Schema.Type.DOUBLE));
    Double copyDefault = assertInstanceOf(Double.class, copy.defaultVal(),
        "expected a Double default for a double field");
    assertEquals(Double.valueOf(7.0d), copyDefault);
  }
}
