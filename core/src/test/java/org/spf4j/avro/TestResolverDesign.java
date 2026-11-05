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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;
import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.util.SchemaVisitor;
import org.spf4j.avro.util.Schemas;

/**
 * Pins the resolver design (option 1+2, no global registry): the
 * schema-carried stamp, the GenericData-carried resolver with named
 * selection, and the precedence between them at conversion time
 * ({@link DatumContext#getEffectiveSchemaResolver(Schema)}).
 */
public class TestResolverDesign {

  /** A resolver that resolves nothing and assigns no ids. */
  private static SchemaResolver stubResolver() {
    return new SchemaResolver() {
      @Override
      public Schema resolveSchema(final String id) {
        throw new IllegalArgumentException("Unknown schema with id " + id);
      }

      @Override
      public String getId(final Schema schema) {
        return null;
      }
    };
  }

  private static final String GRAPH = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
      + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
      + "{\"name\":\"v\",\"type\":\"long\"}]}},"
      + "{\"name\":\"label\",\"type\":\"string\"},"
      + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"Inner\"}},"
      + "{\"name\":\"maybe\",\"type\":[\"null\",\"Inner\"]}]}";

  private static List<Schema> allNodes(final Schema root) {
    List<Schema> nodes = new ArrayList<>();
    Set<Schema> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Schemas.visit(root, new SchemaVisitor<Void>() {
      @Override
      public SchemaVisitorAction visitTerminal(final Schema terminal) {
        if (seen.add(terminal)) {
          nodes.add(terminal);
        }
        return SchemaVisitorAction.CONTINUE;
      }

      @Override
      public SchemaVisitorAction visitNonTerminal(final Schema nonTerminal) {
        if (seen.add(nonTerminal)) {
          nodes.add(nonTerminal);
        }
        return SchemaVisitorAction.CONTINUE;
      }

      @Override
      public SchemaVisitorAction afterVisitNonTerminal(final Schema nonTerminal) {
        return SchemaVisitorAction.CONTINUE;
      }

      @Override
      public Void get() {
        return null;
      }
    });
    return nodes;
  }

  @Test
  public void testStampReachableFromLeafSchema() {
    SchemaResolver resolver = stubResolver();
    Schema schema = new Schema.Parser().setSchemaResolver(resolver).parse(GRAPH);
    // the root, and in particular the leaf field schemas an "any"-typed
    // field's logical type factory would read the stamp from
    assertSame(resolver, schema.getSchemaResolver());
    Schema inner = schema.getField("inner").schema();
    assertSame(resolver, inner.getSchemaResolver());
    assertSame(resolver, inner.getField("v").schema().getSchemaResolver());
    assertSame(resolver, schema.getField("label").schema().getSchemaResolver());
    assertSame(resolver, schema.getField("items").schema().getSchemaResolver());
    assertSame(resolver, schema.getField("maybe").schema().getSchemaResolver());
    // and every node of the graph carries it
    List<Schema> nodes = allNodes(schema);
    assertTrue("expected a non-trivial graph, got " + nodes.size(), nodes.size() >= 7);
    for (Schema node : nodes) {
      assertSame("no stamp on " + node, resolver, node.getSchemaResolver());
    }
  }

  @Test
  public void testParseWithoutResolverHasNoStamp() {
    Schema schema = new Schema.Parser().parse(GRAPH);
    assertNull(schema.getSchemaResolver());
    assertNull(schema.getField("inner").schema().getSchemaResolver());
    for (Schema node : allNodes(schema)) {
      assertNull("unexpected stamp on " + node, node.getSchemaResolver());
    }
  }

  @Test
  public void testStampDoesNotLeakIntoSerialization() throws IOException {
    Schema stamped = new Schema.Parser().setSchemaResolver(stubResolver()).parse(GRAPH);
    Schema unstamped = new Schema.Parser().parse(GRAPH);
    assertEquals(unstamped, stamped);
    String unstampedJson = unstamped.toString();
    assertEquals(unstampedJson, stamped.toString());
    // also through the resolver-aware write entry point with a resolver
    // that assigns no ids: byte-identical, no stamp attribute emitted
    StringWriter writer = new StringWriter();
    JsonGenerator gen = Schema.FACTORY.createGenerator(writer);
    stamped.toJson(SchemaResolver.NONE, gen);
    gen.flush();
    assertEquals(unstampedJson, writer.toString());
  }

  @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
      justification = "each stubResolver() call must return a distinct resolver instance -- telling"
          + " the stamp and the context resolver apart by identity is the behavior under test")
  @Test
  public void testPrecedenceGenericDataBeatsStampBeatsNamesOnly() {
    SchemaResolver stamp = stubResolver();
    Schema stamped = new Schema.Parser().setSchemaResolver(stamp).parse(GRAPH);
    Schema unstamped = new Schema.Parser().parse(GRAPH);

    // 1. GenericData (IO context) resolver beats the schema stamp
    SchemaResolver context = stubResolver();
    GenericData withResolver = new GenericData();
    withResolver.setSchemaResolver(context);
    assertSame(context, DatumContext.of(withResolver).getEffectiveSchemaResolver(stamped));

    // 2. with no context resolver, the schema stamp beats names-only
    GenericData empty = new GenericData();
    DatumContext emptyContext = DatumContext.of(empty);
    assertSame(stamp, emptyContext.getEffectiveSchemaResolver(stamped));
    assertSame(stamp, DatumContext.resolveEffectiveSchemaResolver(null, stamped));

    // 3. with neither, embedded-names-only: an AvroNamesRefResolver with
    // no external resolver (assigns no ids, resolves no external ids)
    SchemaResolver namesOnly = emptyContext.getEffectiveSchemaResolver(unstamped);
    assertInstanceOf(AvroNamesRefResolver.class, namesOnly);
    assertNull(namesOnly.getId(unstamped));
    assertInstanceOf(AvroNamesRefResolver.class,
        DatumContext.resolveEffectiveSchemaResolver(null, unstamped));
  }

  private static Schema anySchema(final String resolverName) {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AnyRec\",\"resolver\":\"" + resolverName
        + "\",\"fields\":[{\"name\":\"avsc\",\"type\":\"string\"},{\"name\":\"content\",\"type\":\"string\"}]}");
  }

  @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
      justification = "each stubResolver() call must return a distinct resolver instance -- telling"
          + " the named resolvers apart by identity is the behavior under test")
  @Test
  public void testNamedSelectionAmongContextResolvers() {
    SchemaResolver resolverA = stubResolver();
    SchemaResolver resolverB = stubResolver();
    GenericData data = new GenericData();
    data.registerSchemaResolver("A", resolverA);
    data.registerSchemaResolver("B", resolverB);

    // a schema carrying resolver=B resolves via B, resolver=A via A,
    // selected from this context's named set only
    DatumContext context = DatumContext.of(data);
    assertSame(resolverB, context.getEffectiveSchemaResolver(anySchema("B")));
    assertSame(resolverA, context.getEffectiveSchemaResolver(anySchema("A")));

    // a name the context does not know falls through to the context's
    // default resolver (the next step of the precedence)
    SchemaResolver fallback = stubResolver();
    data.setSchemaResolver(fallback);
    assertSame(fallback, context.getEffectiveSchemaResolver(anySchema("ZZZ")));
  }

  @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
      justification = "each stubResolver() call must return a distinct resolver instance -- telling"
          + " the two datum models' resolvers apart by identity is the behavior under test")
  @Test
  public void testGenericDataInstancesAreIndependent() {
    SchemaResolver resolver1 = stubResolver();
    SchemaResolver resolver2 = stubResolver();
    GenericData data1 = new GenericData();
    data1.setSchemaResolver(resolver1);
    data1.registerSchemaResolver("x", resolver1);
    GenericData data2 = new GenericData();
    data2.setSchemaResolver(resolver2);

    Schema plain = new Schema.Parser().parse(GRAPH);
    assertSame(resolver1, DatumContext.of(data1).getEffectiveSchemaResolver(plain));
    assertSame(resolver2, DatumContext.of(data2).getEffectiveSchemaResolver(plain));
    assertSame(resolver1, data1.getSchemaResolver("x"));
    assertNull(data2.getSchemaResolver("x"));

    // no static state: the shared singleton was never touched
    GenericData shared = GenericData.get();
    assertNull(shared.getSchemaResolver());
    assertNull(shared.getSchemaResolver("x"));
  }

  @Test
  public void testCopyDoesNotInheritStamp() {
    SchemaResolver resolver = stubResolver();
    Schema stamped = new Schema.Parser().setSchemaResolver(resolver).parse(GRAPH);

    // copying a record (Field copy constructors over the same field schema
    // instances, into a freshly created record) does not copy the stamp:
    // the new schema instance starts unstamped, while the shared field
    // schema instances keep their own stamps
    List<Schema.Field> stampedFields = stamped.getFields();
    List<Schema.Field> copiedFields = new ArrayList<>(stampedFields.size());
    for (Schema.Field field : stampedFields) {
      copiedFields.add(new Schema.Field(field, field.schema()));
    }
    Schema copy = Schema.createRecord(stamped.getName(), stamped.getDoc(), stamped.getNamespace(), false,
        copiedFields);
    assertNull(copy.getSchemaResolver());
    assertSame(resolver, copy.getField("inner").schema().getSchemaResolver());

    // programmatically built schemas carry no stamp either
    Schema built = SchemaBuilder.record("Built").fields().requiredString("s").endRecord();
    assertNull(built.getSchemaResolver());
    assertNull(built.getField("s").schema().getSchemaResolver());
  }

  @Test
  public void testParseJsonNodeWithResolverStampsGraph() throws IOException {
    // the JsonNode entry a conversion decoding an embedded schema uses:
    // parse a tree with a given resolver, producing stamped schemas
    SchemaResolver resolver = stubResolver();
    JsonNode node = Schema.MAPPER.readTree(GRAPH);
    Schema schema = new Schema.Parser().setSchemaResolver(resolver).parse(node);
    assertEquals(new Schema.Parser().parse(GRAPH), schema);
    assertSame(resolver, schema.getSchemaResolver());
    assertSame(resolver, schema.getField("inner").schema().getSchemaResolver());
    for (Schema graphNode : allNodes(schema)) {
      assertSame(resolver, graphNode.getSchemaResolver());
    }
  }
}
