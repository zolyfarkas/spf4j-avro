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
package org.apache.avro;

/**
 * Test-only {@link Conversion} registered in
 * {@code META-INF/services/org.apache.avro.Conversion} so that
 * {@link TestConversionsServiceLoaderHook} can observe which class's
 * ServiceLoader discovery picks it up. The logical type name is unique to
 * this test so the extra registration cannot affect other tests (the seed's
 * {@code GenericData} also discovers it, harmlessly).
 */
public class ConversionsHookTestConversion extends Conversion<String> {

  public static final String LOGICAL_TYPE_NAME = "conversions-hook-test";

  @Override
  public Class<String> getConvertedType() {
    return String.class;
  }

  @Override
  public String getLogicalTypeName() {
    return LOGICAL_TYPE_NAME;
  }

  @Override
  public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return value.toString();
  }

  @Override
  public CharSequence toCharSequence(String value, Schema schema, LogicalType type) {
    return value;
  }
}
