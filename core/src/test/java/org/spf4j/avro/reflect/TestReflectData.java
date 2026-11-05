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

package org.spf4j.avro.reflect;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.spf4j.avro.AvroTypeException;
import org.spf4j.avro.JsonSchemaParser;
import org.spf4j.avro.Protocol;
import org.spf4j.avro.Schema;
import org.spf4j.avro.SchemaParser;
import org.spf4j.avro.util.internal.JacksonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestReflectData {
  @Test
  void getConversionByClassWithNullLogicalTypeReturnsNull() {
    // BigDecimal has a conversion registered by default but a null logicalType
    assertNull(ReflectData.get().getConversionByClass(java.math.BigDecimal.class, null));
  }

  @SuppressFBWarnings(value = "DM_GC",
      justification = "the test verifies that entries are evicted from the weak-reference schema cache; explicitly"
          + "requesting garbage collection is the behavior under test")
  @Test
  @SuppressWarnings("unchecked")
  void weakSchemaCaching() throws Exception {
    int numSchemas = 1000000;
    for (int i = 0; i < numSchemas; i++) {
      // Create schema
      Schema schema = Schema.createRecord("schema", null, null, false);
      schema.setFields(Collections.emptyList());

      ReflectData.get().getRecordState(new Object(), schema);
    }

    // Reflect the number of schemas currently in the cache
    ReflectData.ClassAccessorData classData = ReflectData.ACCESSOR_CACHE.get(Object.class);

    System.gc(); // Not guaranteed, but seems to be reliable enough

    assertThat("ReflectData cache should release references", classData.bySchema.size(), lessThan(numSchemas));
  }

  @Test
  void genericProtocol() {
    Protocol protocol = ReflectData.get().getProtocol(FooBarProtocol.class);
    Schema recordSchema = ReflectData.get().getSchema(FooBarReflectiveRecord.class);

    assertThat(protocol.getTypes(), contains(recordSchema));

    assertThat(protocol.getMessages().keySet(), containsInAnyOrder("store", "findById", "exists"));

    Schema.Field storeArgument = protocol.getMessages().get("store").getRequest().getFields().get(0);
    assertThat(storeArgument.schema(), equalTo(recordSchema));

    Schema.Field findByIdArgument = protocol.getMessages().get("findById").getRequest().getFields().get(0);
    assertThat(findByIdArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));

    Schema findByIdResponse = protocol.getMessages().get("findById").getResponse();
    assertThat(findByIdResponse, equalTo(recordSchema));

    Schema.Field existsArgument = protocol.getMessages().get("exists").getRequest().getFields().get(0);
    assertThat(existsArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));
  }

  private interface CrudProtocol<R, I> extends OtherProtocol<I> {
    void store(R record);

    R findById(I id);
  }

  private interface OtherProtocol<G> {
    boolean exists(G id);
  }

  private interface FooBarProtocol extends OtherProtocol<String>, CrudProtocol<FooBarReflectiveRecord, String> {
  }

  private static class FooBarReflectiveRecord {
    private String bar;
    private int baz;
  }

  static class User {
    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
        justification = "the field is populated or read reflectively (or by the framework under test); there is no"
            + "direct source reference by design")
    public String first = "Avro";
    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
        justification = "the field is populated or read reflectively (or by the framework under test); there is no"
            + "direct source reference by design")
    public String last = "Apache";
  }

  static class Meta {
    public int f1 = 55;
    public int f4;
    public String f2 = "a-string";
    public List<String> f3 = Arrays.asList("one", "two", "three");
    // public User usr = new User();
  }

  @Test
  void createSchemaDefaultValue() {
    Meta meta = new Meta();
    validateSchema(meta);

    meta.f4 = 0x1987;
    validateSchema(meta);
  }

  private void validateSchema(Meta meta) {
    Schema schema = new ReflectData().setDefaultsGenerated(true).setDefaultGeneratedValue(Meta.class, meta)
        .getSchema(Meta.class);

    final String schemaString = schema.toString(true);

    Schema cloneSchema = SchemaParser.parseSingle(schemaString);

    Map testCases = JacksonUtils.objectToMap(meta);

    for (Schema.Field field : cloneSchema.getFields()) {
      assertEquals(field.defaultVal(), testCases.get(field.name()), "Invalid field " + field.name());
    }
  }

  public class Definition {
    public Map<String, String> tokens;
  }

  @Test
  // FIXME: Why does this test fail under JDK 21?
  @EnabledForJreRange(min = JRE.JAVA_8, max = JRE.JAVA_17, disabledReason = "Doesn't work under JRE 21, no clue why")
  void nonStaticInnerClasses() {
    assertThrows(AvroTypeException.class, () -> {
      ReflectData.get().getSchema(Definition.class);
    });
  }

  @Test
  void staticInnerClasses() {
    ReflectData.get().getSchema(Meta.class);
  }
}
