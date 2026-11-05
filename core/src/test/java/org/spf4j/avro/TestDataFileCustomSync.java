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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import org.spf4j.avro.Schema.Type;
import org.spf4j.avro.file.DataFileWriter;
import org.spf4j.avro.generic.GenericDatumWriter;
import org.spf4j.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class TestDataFileCustomSync {
  private byte[] createDataFile(byte[] sync) throws IOException {
    Schema schema = Schema.create(Type.STRING);
    DataFileWriter<Utf8> w = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    w.create(schema, baos, sync);
    w.append(new Utf8("apple"));
    w.append(new Utf8("banana"));
    w.sync();
    w.append(new Utf8("celery"));
    w.append(new Utf8("date"));
    w.sync();
    w.append(new Utf8("endive"));
    w.append(new Utf8("fig"));
    w.close();
    return baos.toByteArray();
  }

  @SuppressFBWarnings(value = {"WEAK_MESSAGE_DIGEST_MD5", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"},
      justification = "the test reproduces the historical MD5-based sync-marker generation to build its fixture;"
          + "the digest is a file-format marker, not a security mechanism;"
          + "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS: this API's documented error model is unchecked:"
          + "exceptions are wrapped in AvroRuntimeException at the boundary so callers program against"
          + "the unchecked contract")
  private static byte[] generateSync() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((UUID.randomUUID() + "@" + time).getBytes(UTF_8));
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void invalidSync() throws IOException {
    assertThrows(IOException.class, () -> {
      // Invalid size (must be 16):
      byte[] sync = new byte[8];
      createDataFile(sync);
    });
  }

  @Test
  void randomSync() throws IOException {
    byte[] sync = generateSync();
    byte[] randSyncFile = createDataFile(null);
    byte[] customSyncFile = createDataFile(sync);
    assertFalse(Arrays.equals(randSyncFile, customSyncFile));
  }

  @Test
  void customSync() throws IOException {
    byte[] sync = generateSync();
    byte[] customSyncFile = createDataFile(sync);
    byte[] sameCustomSyncFile = createDataFile(sync);
    assertArrayEquals(customSyncFile, sameCustomSyncFile);
  }
}
