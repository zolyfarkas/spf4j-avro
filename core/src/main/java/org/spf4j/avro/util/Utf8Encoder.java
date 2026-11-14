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
package org.spf4j.avro.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Reusable scratch that UTF-8 encodes a {@link String} into an owned byte
 * buffer, so that encoding a string does not allocate a byte array per
 * string the way {@link String#getBytes(java.nio.charset.Charset)} does.
 *
 * <p>
 * This is the instance-owned equivalent of the spf4j avro fork's
 * thread-local temp buffers ({@code Arrays.getBytesTmp}/{@code getCharsTmp}
 * in the fork's {@code org.apache.avro.util} package, backed by spf4j-core):
 * the fork's {@code BinaryEncoder.writeString} encoded through them to
 * avoid the per-string allocation; this port keeps no thread-local state,
 * so the scratch lives with its owner instead -- one instance per encoder,
 * encoders being single-threaded by contract.
 * </p>
 *
 * <p>
 * Malformed input (unpaired surrogates) and unmappable characters are
 * replaced, exactly as {@code String.getBytes(UTF_8)} replaces them, so the
 * produced bytes are identical to that method's. Not thread-safe.
 * </p>
 */
public final class Utf8Encoder {

  /**
   * The maximum size of array to allocate. Some VMs reserve some header
   * words in an array. (Same bound as the fork's ByteArrayBuilder.)
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private static final int INITIAL_CAPACITY = 128;

  private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE);

  private byte[] buffer = new byte[INITIAL_CAPACITY];

  private ByteBuffer bufferView = ByteBuffer.wrap(buffer);

  /**
   * Encodes {@code string} as UTF-8 into the scratch buffer.
   *
   * @param string the string to encode
   * @return the number of valid bytes in {@link #getBuffer()}
   */
  public int encode(final String string) {
    // maxBytesPerChar is a hard per-char output bound of the encoder, so
    // sizing the buffer to it makes overflow impossible; the buffer is
    // grown (never shrunk) and reused by subsequent calls.
    long maxBytes = (long) Math.ceil(string.length() * (double) encoder.maxBytesPerChar());
    ensureCapacity(maxBytes);
    encoder.reset();
    bufferView.clear();
    CoderResult result = encoder.encode(CharBuffer.wrap(string), bufferView, true);
    if (!result.isUnderflow()) {
      throw new IllegalStateException("UTF-8 encoding did not consume the input: " + result);
    }
    result = encoder.flush(bufferView);
    if (!result.isUnderflow()) {
      throw new IllegalStateException("UTF-8 encoding did not finish: " + result);
    }
    return bufferView.position();
  }

  /**
   * The scratch buffer holding the bytes of the most recent
   * {@link #encode(String)} call, of which the first {@code encode} result
   * bytes are valid.
   *
   * @return the internal scratch buffer, deliberately not a copy
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the returned array is this encoder's reusable scratch buffer, exposed deliberately"
          + " so callers can write its contents without copying; it is overwritten by the next"
          + " encode call and must not be retained")
  public byte[] getBuffer() {
    return buffer;
  }

  @Override
  public String toString() {
    return "Utf8Encoder[capacity=" + buffer.length + ']';
  }

  private void ensureCapacity(final long minCapacity) {
    if (minCapacity > buffer.length) {
      if (minCapacity > MAX_ARRAY_SIZE) {
        throw new OutOfMemoryError("Requested array size exceeds VM limit: " + minCapacity);
      }
      int newCapacity = (int) Math.max(minCapacity, Math.min(buffer.length * 2L, (long) MAX_ARRAY_SIZE));
      buffer = Arrays.copyOf(buffer, newCapacity);
      bufferView = ByteBuffer.wrap(buffer);
    }
  }
}
