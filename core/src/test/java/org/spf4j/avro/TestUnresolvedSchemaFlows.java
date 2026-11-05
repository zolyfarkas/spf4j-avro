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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.spf4j.avro.util.SchemaResolver;

/**
 * Flow tests for parse-now-resolve-later over this tree's placeholder
 * machinery ({@link ParseContext} + {@code org.spf4j.avro.util.SchemaResolver}),
 * re-expressing the flows spf4j's lib-ext layer implements on the fork with
 * its {@code UnresolvedExtendedParser} (a names map that fabricates an
 * unresolved placeholder for any not-yet-defined name, so parsing proceeds
 * and resolution is deferred until the referenced schema is supplied).
 * lib-ext has no tests of its own on spf4j master; these are written, not
 * ported.
 *
 * <p>
 * The mechanisms used are the ones this tree actually offers:
 * {@link Schema.Parser#parseInternal(String)} parses without resolving (the
 * public {@code parse(...)} resolves eagerly and fails on a still-undefined
 * reference), the shared {@link ParseContext} accumulates named schemas
 * across parses (and via {@link Schema.Parser#addTypes}), and
 * {@link ParseContext#resolve(Schema)} / {@link ParseContext#resolveAllSchemas()}
 * perform the deferred resolution, substituting the supplied definition for
 * the placeholder. One test also exercises the external-resolver surface
 * ({@link Schema.Parser#setSchemaResolver(org.spf4j.avro.SchemaResolver)} with a
 * {@code {"$ref": id}} node), the other registration mechanism the P5 design
 * provides.
 * </p>
 *
 * <p>
 * {@link #testTextualDefaultForForwardReferencedEnumIsDeferredThenRecomputed()}
 * is the evaluation of fork commit 4724e681a's {@code JacksonUtils} tolerance
 * hunk: in the fork, a textual default against an unresolved placeholder
 * threw out of {@code JacksonUtils.toObject} (the fork's {@code Field}
 * computes the Java default eagerly), and the hunk made it return
 * {@code null} ("will be recomputed again at resolution time") instead. In
 * this tree the same flow needs no port: {@code Field} computes the Java
 * default lazily, and this tree's {@code JacksonUtils.toObject} already
 * falls through to {@code null} for a textual node against a record-typed
 * (placeholder) schema. The test pins both halves: no throw and a deferred
 * ({@code null}) default while unresolved, and the correctly recomputed
 * default after resolution.
 * </p>
 */
public class TestUnresolvedSchemaFlows {

  private static final String OUTER = "{\"type\":\"record\",\"name\":\"Outer\",\"namespace\":\"test\","
      + "\"fields\":[{\"name\":\"later\",\"type\":\"Later\"},{\"name\":\"label\",\"type\":\"string\"}]}";

  private static final String LATER = "{\"type\":\"record\",\"name\":\"Later\",\"namespace\":\"test\","
      + "\"fields\":[{\"name\":\"v\",\"type\":\"long\"}]}";

  @Test
  public void testForwardReferenceParsesAsPlaceholder() {
    ParseContext context = new ParseContext();
    Schema.Parser parser = new Schema.Parser(context);

    // parse-now: the forward reference does not fail the parse
    Schema outer = parser.parseInternal(OUTER);

    Schema laterFieldSchema = outer.getField("later").schema();
    assertTrue(SchemaResolver.isUnresolvedSchema(laterFieldSchema),
        "expected an unresolved placeholder, got " + laterFieldSchema);
    assertEquals("test.Later", SchemaResolver.getUnresolvedSchemaName(laterFieldSchema));
    assertFalse(SchemaResolver.isFullyResolvedSchema(outer));
  }

  @Test
  public void testForwardReferenceResolvesAfterSecondParse() {
    ParseContext context = new ParseContext();
    Schema.Parser parser = new Schema.Parser(context);
    Schema outer = parser.parseInternal(OUTER);
    assertFalse(SchemaResolver.isFullyResolvedSchema(outer));

    // resolve-later: supply the referenced schema with a second parse into
    // the same context, then resolve
    parser.parseInternal(LATER);
    context.commit();
    Schema resolved = context.resolve(outer);

    assertTrue(SchemaResolver.isFullyResolvedSchema(resolved));
    Schema laterFieldSchema = resolved.getField("later").schema();
    assertFalse(SchemaResolver.isUnresolvedSchema(laterFieldSchema));
    assertEquals("test.Later", laterFieldSchema.getFullName());
    assertEquals(Schema.Type.LONG, laterFieldSchema.getField("v").schema().getType());
    assertEquals(new Schema.Parser().parse(LATER), laterFieldSchema);

    List<Schema> all = context.resolveAllSchemas();
    assertEquals(2, all.size());
    for (Schema schema : all) {
      assertTrue(SchemaResolver.isFullyResolvedSchema(schema), "unresolved: " + schema);
    }
  }

  @Test
  public void testForwardReferenceResolvesWhenSuppliedViaAddTypes() {
    Schema later = new Schema.Parser().parse(LATER);

    ParseContext context = new ParseContext();
    Schema.Parser parser = new Schema.Parser(context);
    Schema outer = parser.parseInternal(OUTER);
    assertFalse(SchemaResolver.isFullyResolvedSchema(outer));

    // the referenced schema supplied programmatically (registered with the
    // parser) instead of by a second parse
    parser.addTypes(Collections.singletonList(later));
    context.commit();
    Schema resolved = context.resolve(outer);

    assertTrue(SchemaResolver.isFullyResolvedSchema(resolved));
    assertEquals(later, resolved.getField("later").schema());
  }

  @Test
  public void testResolutionBeforeDefinitionFailsAtResolveTimeNotParseTime() {
    ParseContext context = new ParseContext();
    Schema.Parser parser = new Schema.Parser(context);
    // parsing succeeds (that is the parse-now half of the flow) ...
    Schema outer = parser.parseInternal(OUTER);
    context.commit();
    // ... and the deferred error surfaces only when resolution is attempted
    // before the referenced schema has been supplied
    AvroTypeException e = assertThrows(AvroTypeException.class, () -> context.resolve(outer));
    assertTrue(e.getMessage().contains("Undefined schema: test.Later"), e.getMessage());
  }

  @Test
  public void testForwardReferenceResolvesViaExternalResolverRef() {
    Schema later = new Schema.Parser().parse(LATER);
    org.spf4j.avro.SchemaResolver resolver = new org.spf4j.avro.SchemaResolver() {
      @Override
      public Schema resolveSchema(final String id) {
        if ("test.Later".equals(id)) {
          return later;
        }
        throw new IllegalArgumentException("Unknown schema with id " + id);
      }

      @Override
      public String getId(final Schema schema) {
        return null;
      }
    };
    String refOuter = "{\"type\":\"record\",\"name\":\"RefOuter\",\"namespace\":\"test\","
        + "\"fields\":[{\"name\":\"later\",\"type\":{\"$ref\":\"test.Later\"}}]}";

    Schema parsed = new Schema.Parser().setSchemaResolver(resolver).parse(refOuter);

    assertFalse(SchemaResolver.isUnresolvedSchema(parsed.getField("later").schema()));
    assertEquals(later, parsed.getField("later").schema());
    assertTrue(SchemaResolver.isFullyResolvedSchema(parsed));
  }

  @Test
  public void testTextualDefaultForForwardReferencedEnumIsDeferredThenRecomputed() {
    // a field whose type is a forward reference (later defined as an enum)
    // and whose default is textual - the exact shape the fork's
    // JacksonUtils tolerance hunk in 4724e681a exists for. lib-ext's
    // ExtendedParser parses with default validation off by default, and so
    // does this flow.
    String outerEnum = "{\"type\":\"record\",\"name\":\"WithKind\",\"namespace\":\"test\","
        + "\"fields\":[{\"name\":\"kind\",\"type\":\"Kind\",\"default\":\"PLAIN\"}]}";
    String kind = "{\"type\":\"enum\",\"name\":\"Kind\",\"namespace\":\"test\","
        + "\"symbols\":[\"JSON\",\"BASE64BIN\",\"PLAIN\"]}";

    ParseContext context = new ParseContext();
    Schema.Parser parser = new Schema.Parser(context);
    parser.setValidateDefaults(false);
    Schema outer = parser.parseInternal(outerEnum);

    Schema.Field kindField = outer.getField("kind");
    assertTrue(SchemaResolver.isUnresolvedSchema(kindField.schema()));
    // deferred, not an error: computing the Java default against the
    // placeholder yields null (it is recomputed at resolution time)
    assertNull(kindField.defaultVal());

    parser.parseInternal(kind);
    context.commit();
    Schema resolved = context.resolve(outer);

    Schema.Field resolvedKind = resolved.getField("kind");
    assertEquals(Schema.Type.ENUM, resolvedKind.schema().getType());
    assertEquals("test.Kind", resolvedKind.schema().getFullName());
    // recomputed against the resolved enum schema
    assertEquals("PLAIN", resolvedKind.defaultVal());
    assertTrue(SchemaResolver.isFullyResolvedSchema(resolved));
  }
}
