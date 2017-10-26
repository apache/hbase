/**
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
package org.apache.hadoop.hbase.trace;

import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Used to create the graph formed by spans.
 */
public class TraceTree {

  public static class SpansByParent {
    private final Set<Span> set;

    private final HashMap<SpanId, LinkedList<Span>> parentToSpans;

    SpansByParent(Collection<Span> spans) {
      set = new LinkedHashSet<Span>();
      parentToSpans = new HashMap<SpanId, LinkedList<Span>>();
      if(spans == null) {
        return;
      }
      for (Span span : spans) {
        set.add(span);
        for (SpanId parent : span.getParents()) {
          LinkedList<Span> list = parentToSpans.get(parent);
          if (list == null) {
            list = new LinkedList<Span>();
            parentToSpans.put(parent, list);
          }
          list.add(span);
        }
        if (span.getParents().length == 0) {
          LinkedList<Span> list = parentToSpans.get(Long.valueOf(0L));
          if (list == null) {
            list = new LinkedList<Span>();
            parentToSpans.put(new SpanId(Long.MIN_VALUE, Long.MIN_VALUE), list);
          }
          list.add(span);
        }
      }

    }

    public List<Span> find(SpanId parentId) {
      LinkedList<Span> spans = parentToSpans.get(parentId);
      if (spans == null) {
        return new LinkedList<Span>();
      }
      return spans;
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSet(set).iterator();
    }
  }

  public static class SpansByProcessId {
    private final Set<Span> set;

    SpansByProcessId(Collection<Span> spans) {
      set = new LinkedHashSet<Span>();
      if(spans == null) {
        return;
      }
      for (Span span : spans) {
        set.add(span);
      }
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSet(set).iterator();
    }
  }

  private final SpansByParent spansByParent;
  private final SpansByProcessId spansByProcessId;

  /**
   * Create a new TraceTree
   *
   * @param spans The collection of spans to use to create this TraceTree. Should
   *              have at least one root span.
   */
  public TraceTree(Collection<Span> spans) {
    this.spansByParent = new SpansByParent(spans);
    this.spansByProcessId = new SpansByProcessId(spans);
  }

  public SpansByParent getSpansByParent() {
    return spansByParent;
  }

  public SpansByProcessId getSpansByProcessId() {
    return spansByProcessId;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (Iterator<Span> iter = spansByParent.iterator(); iter.hasNext();) {
      Span span = iter.next();
      bld.append(prefix).append(span.toString());
      prefix = "\n";
    }
    return bld.toString();
  }
}
