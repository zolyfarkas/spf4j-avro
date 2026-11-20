/*
 * Copyright 2018 SPF4J.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.spf4j.maven.plugin.avro.doc;

import org.spf4j.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Zoltan Farkas
 */
public class TestAvroDocRenderer {

  private static final String SCHEMA = "{"
          + "\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"org.example\","
          + "\"doc\":\"A **test** record with `code`.\","
          + "\"fields\":["
          + "{\"name\":\"status\",\"doc\":\"the status\","
          + "\"type\":{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"NEW\",\"DONE\"],"
          + "\"default\":\"NEW\",\"doc\":\"status enum\"},\"default\":\"NEW\"},"
          + "{\"name\":\"hash\",\"type\":{\"type\":\"fixed\",\"name\":\"Hash\",\"size\":16}},"
          + "{\"name\":\"count\",\"type\":[\"null\",\"long\"],\"default\":null,\"doc\":\"how many\"},"
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"abc\"}"
          + "]}";

  @Test
  public void testCollectAndRender() throws Exception {
    Schema schema = new Schema.Parser().parse(SCHEMA);
    Map<String, Schema> types = new LinkedHashMap<>();
    AvroDocRenderer.collect(schema, types);
    assertEquals(3, types.size());
    assertTrue(types.containsKey("org.example.TestRecord"));
    assertTrue(types.containsKey("org.example.Status"));
    assertTrue(types.containsKey("org.example.Hash"));
    assertEquals(4, AvroDocRenderer.countFields(types));
    StringBuilder out = new StringBuilder();
    AvroDocRenderer.render(out, "test title", types);
    String html = out.toString();
    assertTrue(html.contains("id=\"org.example.TestRecord\""));
    assertTrue(html.contains("href=\"#org.example.Status\""));
    assertTrue(html.contains("<span class=\"sym\">DONE</span>"));
    assertTrue(html.contains("Size: <code>16</code> bytes"));
    assertTrue(html.contains("&quot;abc&quot;"));
    assertTrue(html.contains("<strong>test</strong>"));
    assertTrue(html.contains("<code>code</code>"));
    assertTrue(html.contains("null"));
  }

  @Test
  public void testEscaping() {
    assertEquals("a&lt;b&gt;&amp;&quot;c&#39;d&quot;", AvroDocRenderer.esc("a<b>&\"c'd\""));
  }

  @Test
  public void testCodeSpanIsLiteral() {
    String html = AvroDocRenderer.docHtml("Use `**not bold** [x](https://example.com)` literally,"
            + " but **this** is bold and [this](https://example.com) is a link.");
    assertTrue(html.contains("<code>**not bold** [x](https://example.com)</code>"));
    assertTrue(html.contains("<strong>this</strong>"));
    assertTrue(html.contains("<a href=\"https://example.com\">this</a>"));
  }

  @Test
  public void testUnmatchedBacktickIsPlainText() {
    String html = AvroDocRenderer.docHtml("a `b c");
    assertFalse(html.contains("<code>"));
    assertTrue(html.contains("a `b c"));
  }

  @Test
  public void testParseUndefinedLogicalType() throws Exception {
    String json = "{\"type\":\"record\",\"name\":\"Any\",\"namespace\":\"org.example\","
            + "\"logicalType\":\"any\",\"doc\":\"Any type\","
            + "\"fields\":[{\"name\":\"content\",\"type\":\"bytes\"}]}";
    File file = File.createTempFile("any", ".avsc");
    Files.write(file.toPath(), json.getBytes(StandardCharsets.UTF_8));
    Schema schema = SchemaDocMojo.parseSchema(file);
    Map<String, Schema> types = new LinkedHashMap<>();
    AvroDocRenderer.collect(schema, types);
    assertTrue(types.containsKey("org.example.Any"));
    StringBuilder out = new StringBuilder();
    AvroDocRenderer.render(out, "t", types);
    assertTrue(out.toString().contains("org.example.Any"));
    Files.delete(file.toPath());
  }

}
