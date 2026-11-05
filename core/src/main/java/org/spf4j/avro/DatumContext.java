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

import java.util.Objects;

import org.spf4j.avro.generic.GenericData;

/**
 * The IO context handed to the direct conversion hooks
 * ({@link Conversion#tryDirectDecode(org.spf4j.avro.io.Decoder, Schema, DatumContext)}
 * and
 * {@link Conversion#tryDirectEncode(Object, org.spf4j.avro.io.Encoder, Schema, DatumContext)})
 * by the datum readers and writers at each call site.
 *
 * <p>
 * This is the GenericData-carried context of the library's resolver design:
 * it carries the {@link GenericData} performing the IO operation (the
 * instance the conversion was registered on, whose other conversions and
 * settings are therefore reachable from the hook), and through it the
 * GenericData's {@link SchemaResolver} is delivered to conversions at use
 * time: see {@link #getEffectiveSchemaResolver(Schema)}. Conversions should
 * take what they need from the context per call instead of capturing
 * ambient (static or construction-time) state.
 * </p>
 *
 * <p>
 * Instances are immutable and are created by the datum reader/writer for
 * each hook invocation; a context never outlives the datum operation it
 * belongs to.
 * </p>
 */
public final class DatumContext {

  private final GenericData data;

  private DatumContext(final GenericData data) {
    this.data = Objects.requireNonNull(data, "data");
  }

  /** Create a context carrying the given datum model. */
  public static DatumContext of(final GenericData data) {
    return new DatumContext(data);
  }

  /**
   * The datum model performing the IO operation (the {@code GenericData},
   * {@code SpecificData} or {@code ReflectData} instance the conversion was
   * registered on).
   */
  public GenericData getData() {
    return data;
  }

  /**
   * The name of the schema property that selects a named schema resolver
   * registered on a datum model: a schema carrying
   * {@code "resolver": "<name>"} (written that way by the {@code "any"}
   * logical type's {@code addToSchema}) selects the resolver registered
   * under {@code <name>} on the IO context.
   */
  public static final String RESOLVER_PROP = "resolver";

  /**
   * The effective schema resolver for a conversion operating on
   * {@code schema} in this IO context. Equivalent to
   * {@link #resolveEffectiveSchemaResolver(GenericData, Schema)} applied to
   * this context's datum model.
   */
  public SchemaResolver getEffectiveSchemaResolver(final Schema schema) {
    return resolveEffectiveSchemaResolver(data, schema);
  }

  /**
   * Resolve the effective schema resolver for a conversion, implementing
   * the library's resolver precedence:
   *
   * <ol>
   * <li>The IO context's resolver: when {@code schema} carries a
   * {@code "resolver"} property (see {@link #RESOLVER_PROP}), the resolver
   * registered under that name on {@code data} (see
   * {@link GenericData#registerSchemaResolver(String, SchemaResolver)});
   * when the name is absent, or no resolver is registered under it on this
   * context, {@code data}'s default resolver
   * ({@link GenericData#getSchemaResolver()}). Names select within the
   * caller's context only, never within JVM-global state — there is no
   * global registry.</li>
   * <li>The schema-stamped resolver: the resolver the schema graph was
   * parsed with ({@link Schema#getSchemaResolver()}), which travels with
   * the schema.</li>
   * <li>Embedded-names-only resolution: an {@link AvroNamesRefResolver}
   * with no external resolver ({@link SchemaResolver#NONE}), which assigns
   * no ids and resolves no external ids — only the definitions embedded in
   * the payload/schema itself are available, through the datum machinery's
   * own parsing.</li>
   * </ol>
   *
   * @param data   the datum model performing the IO operation, or
   *               {@code null} when there is no IO context (the precedence
   *               then starts at the schema stamp)
   * @param schema the schema of the value being converted, or {@code null}
   * @return the effective resolver; never {@code null}
   */
  public static SchemaResolver resolveEffectiveSchemaResolver(final GenericData data, final Schema schema) {
    String resolverName = schema != null ? schema.getProp(RESOLVER_PROP) : null;
    if (data != null) {
      if (resolverName != null) {
        SchemaResolver named = data.getSchemaResolver(resolverName);
        if (named != null) {
          return named;
        }
      }
      SchemaResolver contextResolver = data.getSchemaResolver();
      if (contextResolver != null) {
        return contextResolver;
      }
    }
    if (schema != null) {
      SchemaResolver stamped = schema.getSchemaResolver();
      if (stamped != null) {
        return stamped;
      }
    }
    return new AvroNamesRefResolver(SchemaResolver.NONE);
  }

  @Override
  public String toString() {
    return "DatumContext{data=" + data + '}';
  }
}
