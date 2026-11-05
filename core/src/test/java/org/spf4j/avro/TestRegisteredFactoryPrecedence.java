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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * Pins the registered-factory precedence of
 * {@link LogicalTypes#fromSchema(Schema)} (stage P7b/B2): a factory
 * registered for a built-in logical type name is consulted for that name
 * instead of being shadowed by the built-in switch. This is the library
 * half of the logical-types library's extended {@code date} semantics: its
 * {@code DateLogicalTypeFactory} (registered via ServiceLoader) supplies
 * dates backed by a string with a {@code format} property (and other
 * extended forms), which the built-in {@link LogicalTypes.Date} validation
 * rejects outright.
 *
 * <p>
 * The factory registered here is deliberately conservative, because the
 * registration remains in the static registry after this test: for the
 * standard int-backed date it returns {@link LogicalTypes#date()} itself,
 * and for any other non-extended backing type it throws, which the
 * fromSchema invalid-type handling turns into the same ignore-and-warn
 * outcome the built-in validation produces (a long-backed date stays
 * invalid, as {@code TestSchemaWarnings} pins). Only the extended form --
 * a string-backed date carrying a {@code format} property, the form the
 * logical-types library's factory adds -- returns the sentinel, a form no
 * other library test uses.
 * </p>
 */
public class TestRegisteredFactoryPrecedence {

  private static final LogicalType EXTENDED_DATE = new LogicalType("date");

  private static final AtomicBoolean CONSULTED = new AtomicBoolean();

  private static final LogicalTypes.LogicalTypeFactory EXTENDED_DATE_FACTORY = new LogicalTypes.LogicalTypeFactory() {
    @Override
    public LogicalType fromSchema(final Schema schema) {
      CONSULTED.set(true);
      if (schema.getType() == Schema.Type.INT) {
        return LogicalTypes.date();
      }
      if (schema.getType() == Schema.Type.STRING && schema.getProp("format") != null) {
        return EXTENDED_DATE;
      }
      throw new IllegalArgumentException(
          "Date can only be used with an underlying int or string type, got " + schema.getType());
    }

    @Override
    public String getTypeName() {
      return "date";
    }
  };

  @Test
  void registeredFactoryForDateIsConsulted() {
    LogicalTypes.register(EXTENDED_DATE_FACTORY);

    // standard int-backed date: the factory is consulted and supplies the type
    CONSULTED.set(false);
    Schema intDate = Schema.create(Schema.Type.INT);
    intDate.addProp(LogicalType.LOGICAL_TYPE_PROP, "date");
    assertSame(LogicalTypes.date(), LogicalTypes.fromSchema(intDate));
    assertTrue(CONSULTED.get(), "registered factory should be consulted for the built-in name \"date\"");

    // extended string-backed date with a format: the built-in Date type
    // rejects this schema ("Date can only be used with an underlying int
    // type"), so fromSchema would throw and fromSchemaIgnoreInvalid would
    // return null if the built-in switch shadowed the registered factory
    CONSULTED.set(false);
    Schema stringDate = Schema.create(Schema.Type.STRING);
    stringDate.addProp(LogicalType.LOGICAL_TYPE_PROP, "date");
    stringDate.addProp("format", "yyyy-MM-dd");
    assertSame(EXTENDED_DATE, LogicalTypes.fromSchema(stringDate));
    assertSame(EXTENDED_DATE, LogicalTypes.fromSchemaIgnoreInvalid(stringDate));
    assertTrue(CONSULTED.get(), "registered factory should be consulted for the extended date form");
  }

  @Test
  void builtinDateStillAppliesWhenFactoryReturnsBuiltin() {
    LogicalTypes.register(EXTENDED_DATE_FACTORY);
    Schema parsed = new Schema.Parser().parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    assertSame(LogicalTypes.date(), parsed.getLogicalType());
  }
}
