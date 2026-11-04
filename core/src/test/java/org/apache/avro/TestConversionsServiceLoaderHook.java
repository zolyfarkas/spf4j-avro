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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

/**
 * Spec test for the fork's {@code Conversions}-level ServiceLoader hook.
 *
 * <p>
 * In the fork, {@link Conversions} builds its default conversion map
 * statically from its built-in registrations <em>plus</em>
 * {@code ServiceLoader.load(Conversion.class)}, and exposes the result as
 * {@code Conversions.defaultConversions()} — the map the fork's
 * {@code GenericData} starts from. A {@code Conversion} declared in
 * {@code META-INF/services/org.apache.avro.Conversion} (here:
 * {@link ConversionsHookTestConversion}) must therefore be visible through
 * {@code Conversions} itself.
 *
 * <p>
 * The seed has no such hook: its {@code Conversions} class has no
 * {@code defaultConversions()} and performs no ServiceLoader discovery
 * (only {@code GenericData}'s separate discovery exists), so this test
 * does not compile on the seed. It deliberately does not go through
 * {@code GenericData}, whose discovery is a different, already-present
 * path.
 */
public class TestConversionsServiceLoaderHook {

  @Test
  public void testConversionDiscoveredThroughConversionsClass() {
    Map<String, Conversion<?>> defaults = Conversions.defaultConversions();
    Conversion<?> discovered = defaults.get(ConversionsHookTestConversion.LOGICAL_TYPE_NAME);
    assertNotNull("Conversion registered via META-INF/services was not discovered by Conversions",
        discovered);
    assertTrue(discovered instanceof ConversionsHookTestConversion);
    // the map really is the defaults map: the built-ins are in it too
    assertTrue(defaults.containsKey("decimal"));
  }
}
