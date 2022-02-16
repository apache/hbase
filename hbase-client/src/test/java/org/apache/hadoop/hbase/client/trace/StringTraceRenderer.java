/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.trace;

import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Rudimentary tool for visualizing a hierarchy of spans. Given a collection of spans, indexes
 * them from parents to children and prints them out one per line, indented.
 */
@InterfaceAudience.Private
public class StringTraceRenderer {
  private static final Logger logger = LoggerFactory.getLogger(StringTraceRenderer.class);

  private final List<Node> graphs;

  public StringTraceRenderer(final Collection<SpanData> spans) {
    final Map<String, Node> spansById = indexSpansById(spans);
    populateChildren(spansById);
    graphs = findRoots(spansById);
  }

  private static Map<String, Node> indexSpansById(final Collection<SpanData> spans) {
    final Map<String, Node> spansById = new HashMap<>(spans.size());
    spans.forEach(span -> spansById.put(span.getSpanId(), new Node(span)));
    return spansById;
  }

  private static void populateChildren(final Map<String, Node> spansById) {
    spansById.forEach((spanId, node) -> {
      final SpanData spanData = node.spanData;
      final String parentSpanId = spanData.getParentSpanId();
      if (Objects.equals(parentSpanId, SpanId.getInvalid())) {
        return;
      }
      final Node parentNode = spansById.get(parentSpanId);
      if (parentNode == null) {
        logger.warn("Span {} has parent {} that is not found in index, {}", spanId, parentSpanId,
          spanData);
        return;
      }
      parentNode.children.put(spanId, node);
    });
  }

  private static List<Node> findRoots(final Map<String, Node> spansById) {
    return spansById.values()
      .stream()
      .filter(node -> Objects.equals(node.spanData.getParentSpanId(), SpanId.getInvalid()))
      .collect(Collectors.toList());
  }

  public void render(final Consumer<String> writer) {
    for (ListIterator<Node> iter = graphs.listIterator(); iter.hasNext(); ) {
      final int idx = iter.nextIndex();
      final Node node = iter.next();
      render(writer, node, 0, idx == 0);
    }
  }

  private static void render(
    final Consumer<String> writer,
    final Node node,
    final int indent,
    final boolean isFirst
  ) {
    writer.accept(render(node.spanData, indent, isFirst));
    final List<Node> children = new ArrayList<>(node.children.values());
    for (ListIterator<Node> iter = children.listIterator(); iter.hasNext(); ) {
      final int idx = iter.nextIndex();
      final Node child = iter.next();
      render(writer, child, indent + 2, idx == 0);
    }
  }

  private static String render(
    final SpanData spanData,
    final int indent,
    final boolean isFirst
  ) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append(' ');
    }

    return sb.append(isFirst ? "└─ " : "├─ ")
      .append(render(spanData))
      .toString();
  }

  private static String render(final SpanData spanData) {
    return new ToStringBuilder(spanData, ToStringStyle.NO_CLASS_NAME_STYLE)
      .append("spanId", spanData.getSpanId())
      .append("name", spanData.getName())
      .append("hasEnded", spanData.hasEnded())
      .toString();
  }

  private static class Node {
    final SpanData spanData;
    final LinkedHashMap<String, Node> children;

    Node(final SpanData spanData) {
      this.spanData = spanData;
      this.children = new LinkedHashMap<>();
    }
  }
}
