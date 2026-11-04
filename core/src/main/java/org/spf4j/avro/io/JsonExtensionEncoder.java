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
package org.spf4j.avro.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.spf4j.avro.Schema;

/**
 * An extension interface that allows encoding decimal and other logical
 * type values in their natural (non-Avro-JSON-encoding) JSON form.
 * Implemented by {@link ExtendedJsonEncoder}.
 *
 * <p>
 * Datum writers consult this interface indirectly: a conversion's
 * {@code Conversion.tryDirectEncode} may delegate to these methods when
 * the encoder at hand implements this interface (the fork's
 * {@code Decimal2Converter} pattern). Ported from the fork's interface of
 * the same name.
 * </p>
 */
public interface JsonExtensionEncoder {

  /**
   * Write a pre-converted logical value (for example a temporal or
   * decimal Java object) directly into the JSON stream, advancing the
   * encoder grammar by {@code schema}.
   */
  void writeValue(final Object value, final Schema schema) throws IOException;

  /**
   * Write {@code decimal} as a raw JSON number, with its scale preserved
   * ({@code 1.10} is written as {@code 1.10}), instead of the standard
   * bytes-as-string form.
   */
  void writeDecimal(final BigDecimal decimal, final Schema schema) throws IOException;

  /** Write {@code value} as a raw JSON number instead of a long/string. */
  void writeBigInteger(final BigInteger value, final Schema schema) throws IOException;
}
