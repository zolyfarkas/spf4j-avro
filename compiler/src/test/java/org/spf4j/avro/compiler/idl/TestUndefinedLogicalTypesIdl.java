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
package org.spf4j.avro.compiler.idl;

import org.spf4j.avro.LogicalTypes;
import org.spf4j.avro.Protocol;
import org.spf4j.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mirrors the donor fork's IDL intent (donor compiler tests
 * {@code TestSchemaResolver}/{@code TestProtocol} drove the fork toggle
 * {@code Idl.setIsAllowUndefinedLogicalTypes}): an IDL protocol whose schema
 * uses an undefined logical type (a {@code logicalType} name nothing
 * registers) must fail compilation by default, and must compile with the
 * {@code allowUndefinedLogicalTypes} opt-in active. The opt-in is driven
 * through this library's actual surfaces: the {@link LogicalTypes}
 * thread-local from core, captured when the {@link Idl} parser is
 * constructed, and the compiler-level {@code Idl} toggle that overrides it
 * (the minimal equivalent of the fork's toggle, needed because the IDL
 * parser builds schemas programmatically and does not go through
 * {@code Schema.Parser}/{@code ParseContext}).
 */
public class TestUndefinedLogicalTypesIdl {

  private static final String RESOURCE = "undefinedLogicalType.avdl";

  @AfterEach
  public void clearAmbientOptIn() {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(null);
  }

  private static Idl newIdl() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    return new Idl(cl.getResourceAsStream(RESOURCE), "UTF-8");
  }

  private static void assertUndefinedLtRetained(Protocol protocol) {
    Schema record = protocol.getType("org.spf4j.avro.test.UndefinedLtRecord");
    Schema value = record.getField("value").schema();
    assertEquals("undefined-idl-lt", value.getProp("logicalType"));
    assertNull(value.getLogicalType());
    Schema values = record.getField("values").schema();
    assertEquals("undefined-idl-lt", values.getProp("logicalType"));
    assertNull(values.getLogicalType());
  }

  @Test
  void undefinedLogicalTypeFailsCompilationByDefault() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
      try (Idl idl = newIdl()) {
        idl.CompilationUnit();
      }
    });
    assertTrue(e.getMessage().startsWith("Undefined logical type undefined-idl-lt for "), e.getMessage());
  }

  @Test
  void undefinedLogicalTypeCompilesWithThreadLocalOptIn() throws Exception {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(Boolean.TRUE);
    try (Idl idl = newIdl()) {
      assertUndefinedLtRetained(idl.CompilationUnit());
    }
  }

  @Test
  void undefinedLogicalTypeCompilesWithIdlToggleOptIn() throws Exception {
    try (Idl idl = newIdl()) {
      idl.setIsAllowUndefinedLogicalTypes(true);
      assertUndefinedLtRetained(idl.CompilationUnit());
    }
  }

  @Test
  void idlToggleFalseOverridesAmbientThreadLocalOptIn() {
    LogicalTypes.setAllowUndefinedLogicalTypesThreadLocal(Boolean.TRUE);
    assertThrows(IllegalArgumentException.class, () -> {
      try (Idl idl = newIdl()) {
        idl.setIsAllowUndefinedLogicalTypes(false);
        idl.CompilationUnit();
      }
    });
  }
}
