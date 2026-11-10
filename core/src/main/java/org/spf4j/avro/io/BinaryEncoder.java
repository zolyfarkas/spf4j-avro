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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.spf4j.avro.util.Utf8;
import org.spf4j.avro.util.Utf8Encoder;

/**
 * An abstract {@link Encoder} for Avro's binary encoding.
 * <p/>
 * To construct and configure instances, use {@link EncoderFactory}
 *
 * @see EncoderFactory
 * @see BufferedBinaryEncoder
 * @see DirectBinaryEncoder
 * @see BlockingBinaryEncoder
 * @see Encoder
 * @see Decoder
 */
@SuppressFBWarnings(value = "ACEM_ABSTRACT_CLASS_EMPTY_METHODS",
    justification = "ACEM_ABSTRACT_CLASS_EMPTY_METHODS: the empty methods are the visitor/adapter extension"
        + "points: subclasses override only the callbacks they need; the defaults are deliberately"
        + "no-ops; ACEM_ABSTRACT_CLASS_EMPTY_METHODS: the empty methods are the visitor/adapter"
        + "extension points: subclasses override only the callbacks they need; the defaults are"
        + "deliberately no-ops; ACEM_ABSTRACT_CLASS_EMPTY_METHODS: the empty methods are the"
        + "visitor/adapter extension points: subclasses override only the callbacks they need; the"
        + "defaults are deliberately no-ops; ACEM_ABSTRACT_CLASS_EMPTY_METHODS: the empty methods are"
        + "the visitor/adapter extension points: subclasses override only the callbacks they need; the"
        + "defaults are deliberately no-ops")
public abstract class BinaryEncoder extends Encoder {

  /*
   * Buffer used for writing ASCII strings. A string is encoded as a long followed
   * by that many bytes of character data. A string of length 63 is the upper
   * limit for a 1 byte variable-length long value.
   */
  private final byte[] stringBuffer = new byte[63];

  /*
   * Scratch used to UTF-8 encode the strings that do not take the ASCII
   * fast path above. Instance-owned (encoders are single-threaded): the
   * spf4j avro fork used thread-local temp buffers for the same purpose.
   */
  private final Utf8Encoder stringEncoder = new Utf8Encoder();

  @Override
  public void writeNull() throws IOException {
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    this.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
  }

  @Override
  public void writeString(String string) throws IOException {
    /* empty string short-circuit */
    if (string.isEmpty()) {
      writeZero();
      return;
    }

    /*
     * Assume the String is ASCII. If the ASCII String fits into the existing
     * buffer, copy the characters into the buffer and write it to the underlying
     * Encoder. If the String is too long, or ends up not being ASCII, then
     * fall-back to the default JDK mechanism for handling String to byte array.
     */
    final int stringLength = string.length();
    if (stringLength <= stringBuffer.length) {
      boolean onlyAscii = true;
      for (int i = 0; onlyAscii && (i < stringLength); i++) {
        /*
         * The char data type is a single 16-bit Unicode character (UTF-16). ASCII, is a
         * 7-bit character encoding. Therefore, if the value is larger than 127, it
         * cannot be ASCII. If it is ASCII, it is safe to trim to byte.
         */
        final char c = string.charAt(i);
        if (c >= 0x80) {
          onlyAscii = false;
        } else {
          stringBuffer[i] = (byte) c;
        }
      }
      if (onlyAscii) {
        writeInt(stringLength);
        writeFixed(stringBuffer, 0, stringLength);
        return;
      }
    }

    /*
     * The string is too long for the ASCII buffer or contains non-ASCII
     * characters. Encode it as UTF-8 into the reusable scratch buffer
     * instead of String.getBytes, which instantiates a new byte array (and
     * copies the data into it) for every string. The scratch encoder
     * replaces malformed input exactly as String.getBytes does, so the
     * bytes written are identical.
     */
    int bytesLength = stringEncoder.encode(string);
    writeInt(bytesLength);
    writeFixed(stringEncoder.getBuffer(), 0, bytesLength);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    int len = bytes.limit() - bytes.position();
    if (0 == len) {
      writeZero();
    } else {
      writeInt(len);
      writeFixed(bytes);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    if (0 == len) {
      writeZero();
      return;
    }
    this.writeInt(len);
    this.writeFixed(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    this.writeInt(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    if (itemCount > 0) {
      this.writeLong(itemCount);
    }
  }

  @Override
  public void startItem() throws IOException {
  }

  @Override
  public void writeArrayEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeMapStart() throws IOException {
  }

  @Override
  public void writeMapEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    writeInt(unionIndex);
  }

  /** Write a zero byte to the underlying output. **/
  protected abstract void writeZero() throws IOException;

  /**
   * Returns the number of bytes currently buffered by this encoder. If this
   * Encoder does not buffer, this will always return zero.
   * <p/>
   * Call {@link #flush()} to empty the buffer to the underlying output.
   */
  public abstract int bytesBuffered();

}
