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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * Pins the scratch-buffer fallback of {@link BinaryEncoder#writeString}
 * (audit finding A3): strings that are too long for the ASCII fast path or
 * contain non-ASCII characters are UTF-8 encoded through a reusable
 * scratch buffer ({@code org.spf4j.avro.util.Utf8Encoder}) instead of a
 * per-string {@code String.getBytes} allocation. The bytes on the wire
 * must be identical to the getBytes-based encoding, including for
 * malformed input, and one encoder instance must stay correct across
 * buffer growth and reuse.
 */
public class TestBinaryEncoderWriteString {

  /** Reference encoding: zigzag-varint byte length + UTF-8 bytes. */
  private static byte[] expected(final String string) {
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    long length = bytes.length;
    long n = (length << 1) ^ (length >> 63);
    while ((n & ~0x7F) != 0) {
      bos.write((byte) ((n & 0x7F) | 0x80));
      n >>>= 7;
    }
    bos.write((byte) n);
    bos.write(bytes, 0, bytes.length);
    return bos.toByteArray();
  }

  private static byte[] write(final String string) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
    encoder.writeString(string);
    encoder.flush();
    return bos.toByteArray();
  }

  @Test
  public void testLongAsciiUsesFallback() throws IOException {
    String string = "a".repeat(200);
    assertArrayEquals(expected(string), write(string));
  }

  @Test
  public void testShortNonAsciiUsesFallback() throws IOException {
    assertArrayEquals(expected("héllo wörld"), write("héllo wörld"));
  }

  @Test
  public void testBufferGrowthWithSurrogatePairs() throws IOException {
    // 200 supplementary characters: 800 UTF-8 bytes, well beyond the
    // scratch buffer's initial capacity
    String string = "\uD83D\uDE00".repeat(200);
    assertArrayEquals(expected(string), write(string));
  }

  @Test
  public void testMalformedInputReplacedLikeGetBytes() throws IOException {
    String string = "abc\uD800def\uDC00";
    assertArrayEquals(expected(string), write(string));
  }

  @Test
  public void testFastPathBoundary() throws IOException {
    assertArrayEquals(expected("a".repeat(63)), write("a".repeat(63)));
    assertArrayEquals(expected("a".repeat(64)), write("a".repeat(64)));
    assertArrayEquals(expected(""), write(""));
  }

  @Test
  public void testEncoderReuseAcrossGrowth() throws IOException {
    String longString = "\u00e9".repeat(300);
    String shortString = "héllo";
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
    encoder.writeString(longString);
    encoder.writeString(shortString);
    encoder.writeString(longString);
    encoder.flush();
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bos.toByteArray(), null);
    assertEquals(longString, decoder.readString());
    assertEquals(shortString, decoder.readString());
    assertEquals(longString, decoder.readString());
  }
}
