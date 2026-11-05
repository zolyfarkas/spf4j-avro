/**
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

import java.nio.ByteBuffer;

public class ByteBufferRecord {

  private ByteBuffer payload;
  private TypeEnum tp;

  public ByteBufferRecord() {
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "the returned object is this instance's live model/state, exposed deliberately: Avro's model"
          + "and datum objects are shared carriers by design, not defensive-copy value objects")
  public ByteBuffer getPayload() {
    return payload;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
      justification = "the caller's object is adopted by reference deliberately: the datum/model layer shares"
          + "schemas and configuration objects instead of defensively copying them on construction and"
          + "configuration paths")
  public void setPayload(ByteBuffer payload) {
    this.payload = payload;
  }

  public TypeEnum getTp() {
    return tp;
  }

  public void setTp(TypeEnum tp) {
    this.tp = tp;
  }

  @Override
  public boolean equals(Object ob) {
    if (this == ob)
      return true;
    if (!(ob instanceof ByteBufferRecord))
      return false;
    ByteBufferRecord that = (ByteBufferRecord) ob;
    if (this.getPayload() == null)
      return that.getPayload() == null;
    if (!this.getPayload().equals(that.getPayload()))
      return false;
    if (this.getTp() == null)
      return that.getTp() == null;
    return this.getTp() == that.getTp();
  }

  @Override
  public int hashCode() {
    return this.payload.hashCode();
  }
}
