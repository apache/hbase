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



import io.opentracing.Span;
import io.opentracing.mock.MockSpan;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Used to create the graph formed by spans.
 */
public class TraceTree {

  public static class SpansByParent {
    private static Comparator<MockSpan> COMPARATOR =
        new Comparator<MockSpan>() {
          @Override
          public int compare(MockSpan a, MockSpan b) {
            MockSpan.MockContext ctx1 = a.context();
            MockSpan.MockContext ctx2 = b.context();
            return (int)(ctx1.spanId() - ctx2.spanId());
          }
        };

    private final TreeSet<MockSpan> treeSet;

    private final HashMap<Long, LinkedList<MockSpan>> parentToSpans;

    SpansByParent(Collection<MockSpan> spans) {
      TreeSet<MockSpan> treeSet = new TreeSet<MockSpan>(COMPARATOR);
      parentToSpans = new HashMap<Long, LinkedList<MockSpan>>();
      for (MockSpan span : spans) {
        treeSet.add(span);
        long parentId = span.parentId();
        LinkedList<MockSpan> list = parentToSpans.get(parentId);
        if (list == null) {
          list = new LinkedList<MockSpan>();
          parentToSpans.put(parentId, list);
        }
        list.add(span);

        /*if (span.getParents().length == 0) {
          LinkedList<MockSpan> list = parentToSpans.get(SpanId.INVALID);
          if (list == null) {
            list = new LinkedList<MockSpan>();
            parentToSpans.put(SpanId.INVALID, list);
          }
          list.add(span);
        }*/
      }
      this.treeSet = treeSet;
    }

    public List<MockSpan> find(long parentId) {
      LinkedList<MockSpan> spans = parentToSpans.get(parentId);
      if (spans == null) {
        return new LinkedList<MockSpan>();
      }
      return spans;
    }

    public Iterator<MockSpan> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
    }
  }

  public static class SpansByProcessId {
    private static Comparator<MockSpan> COMPARATOR =
        new Comparator<MockSpan>() {
          @Override
          public int compare(MockSpan a, MockSpan b) {
            return (int)(a.context().spanId() - b.context().spanId());
          }
        };

    private final TreeSet<MockSpan> treeSet;

    SpansByProcessId(Collection<MockSpan> spans) {
      TreeSet<MockSpan> treeSet = new TreeSet<MockSpan>(COMPARATOR);
      for (MockSpan span : spans) {
        treeSet.add(span);
      }
      this.treeSet = treeSet;
    }

    public Iterator<MockSpan> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
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
  public TraceTree(Collection<MockSpan> spans) {
    if (spans == null) {
      spans = Collections.emptySet();
    }
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
    for (Iterator<MockSpan> iter = spansByParent.iterator(); iter.hasNext();) {
      Span span = iter.next();
      bld.append(prefix).append(span.toString());
      prefix = "\n";
    }
    return bld.toString();
  }
}
