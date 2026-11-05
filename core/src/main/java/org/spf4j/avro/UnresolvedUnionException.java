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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/** Thrown when the expected contents of a union cannot be resolved. */
public class UnresolvedUnionException extends AvroRuntimeException {
  private Object unresolvedDatum;
  private Schema unionSchema;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
      justification = "the caller's object is adopted by reference deliberately: the datum/model layer shares"
          + "schemas and configuration objects instead of defensively copying them on construction and"
          + "configuration paths")
  public UnresolvedUnionException(Schema unionSchema, Object unresolvedDatum) {
    super("Not in union " + unionSchema + ": " + datumTypeDescription(unresolvedDatum));
    this.unionSchema = unionSchema;
    this.unresolvedDatum = unresolvedDatum;
  }

  public UnresolvedUnionException(Schema unionSchema, Schema.Field field, Object unresolvedDatum) {
    super(
        "Not in union " + unionSchema + ": " + datumTypeDescription(unresolvedDatum) + " (field=" + field.name() + ")");
    this.unionSchema = unionSchema;
    this.unresolvedDatum = unresolvedDatum;
  }

  public Object getUnresolvedDatum() {
    return unresolvedDatum;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
          + "and datum objects are shared carriers by design, not defensive-copy value objects")
  public Schema getUnionSchema() {
    return unionSchema;
  }

  private static String datumTypeDescription(Object datum) {
    return datum == null ? "null" : datum.getClass().getName();
  }
}
