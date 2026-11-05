/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.spf4j.avro.message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.spf4j.avro.Schema;
import org.spf4j.avro.SchemaParser;
import org.spf4j.avro.generic.GenericData;
import org.spf4j.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Generates <code>test_message.bin</code> - a <a href=
 * "https://avro.apache.org/docs/current/spec.html#single_object_encoding">single
 * object encoded</a> Avro message.
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
    justification = "test fixture paths built from constant names under the test resources folder"
        + " (the share.dir system property only relocates the fixture root); there is no untrusted input")
public class TestGenerateInteropSingleObjectEncoding {
  private static final String RESOURCES_FOLDER = System.getProperty("share.dir", "target/test-classes/share")
      + "/test/data/messageV1";
  private static final File SCHEMA_FILE = new File(RESOURCES_FOLDER + "/test_schema.avsc");
  private static final File MESSAGE_FILE = new File(RESOURCES_FOLDER + "/test_message.bin");
  private static Schema SCHEMA;
  private static GenericRecordBuilder BUILDER;

  @SuppressFBWarnings(value = "IOI_USE_OF_FILE_STREAM_CONSTRUCTORS",
      justification = "stream construction from a File is this API's documented shape (the container-file readers"
          + "and writers are file-based); the File constructors' exception behavior is part of the"
          + "contract")
  @BeforeAll
  public static void setup() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(SCHEMA_FILE)) {
      SCHEMA = new SchemaParser().parse(fileInputStream).mainSchema();
      BUILDER = new GenericRecordBuilder(SCHEMA);
    }
  }

  @SuppressFBWarnings(value = "IOI_USE_OF_FILE_STREAM_CONSTRUCTORS",
      justification = "stream construction from a File is this API's documented shape (the container-file readers"
          + "and writers are file-based); the File constructors' exception behavior is part of the"
          + "contract")
  @Test
  void generateData() throws IOException {
    MessageEncoder<GenericData.Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA);
    BUILDER.set("id", 42L).set("name", "Bill").set("tags", Arrays.asList("dog_lover", "cat_hater")).build();
    ByteBuffer buffer = encoder.encode(
        BUILDER.set("id", 42L).set("name", "Bill").set("tags", Arrays.asList("dog_lover", "cat_hater")).build());
    try (FileOutputStream out = new FileOutputStream(MESSAGE_FILE)) {
      out.write(buffer.array());
    }
  }
}
