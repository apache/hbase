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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABEL_QUALIFIER;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.ExpressionExpander;
import org.apache.hadoop.hbase.security.visibility.ExpressionParser;
import org.apache.hadoop.hbase.security.visibility.InvalidLabelException;
import org.apache.hadoop.hbase.security.visibility.ParseException;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An utility class that helps the mapper and reducers used with visibility to
 * scan the visibility_labels and helps in parsing and expanding the visibility
 * tags
 * 
 */
@InterfaceAudience.Private
public class LabelExpander {
  private Configuration conf;
  private ExpressionParser parser = new ExpressionParser();
  private ExpressionExpander expander = new ExpressionExpander();

  public LabelExpander(Configuration conf) {
    this.conf = conf;
  }

  private Map<String, Integer> labels;

  // TODO : The code repeats from that in Visibility Controller.. Refactoring
  // may be needed
  private List<Tag> createVisibilityTags(String visibilityLabelsExp) throws IOException,
      ParseException, InvalidLabelException {
    ExpressionNode node = null;
    node = parser.parse(visibilityLabelsExp);
    node = expander.expand(node);
    List<Tag> tags = new ArrayList<Tag>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<Integer> labelOrdinals = new ArrayList<Integer>();
    // We will be adding this tag before the visibility tags and the presence of
    // this
    // tag indicates we are supporting deletes with cell visibility
    tags.add(VisibilityUtils.VIS_SERIALIZATION_TAG);
    if (node.isSingleNode()) {
      getLabelOrdinals(node, labelOrdinals);
      writeLabelOrdinalsToStream(labelOrdinals, dos);
      tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
      baos.reset();
    } else {
      NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
      if (nlNode.getOperator() == Operator.OR) {
        for (ExpressionNode child : nlNode.getChildExps()) {
          getLabelOrdinals(child, labelOrdinals);
          writeLabelOrdinalsToStream(labelOrdinals, dos);
          tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
          baos.reset();
          labelOrdinals.clear();
        }
      } else {
        getLabelOrdinals(nlNode, labelOrdinals);
        writeLabelOrdinalsToStream(labelOrdinals, dos);
        tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
        baos.reset();
      }
    }
    return tags;
  }

  private void writeLabelOrdinalsToStream(List<Integer> labelOrdinals, DataOutputStream dos)
      throws IOException {
    Collections.sort(labelOrdinals);
    for (Integer labelOrdinal : labelOrdinals) {
      StreamUtils.writeRawVInt32(dos, labelOrdinal);
    }
  }

  private void getLabelOrdinals(ExpressionNode node, List<Integer> labelOrdinals)
      throws IOException, InvalidLabelException {
    if (node.isSingleNode()) {
      String identifier = null;
      int labelOrdinal = 0;
      if (node instanceof LeafExpressionNode) {
        identifier = ((LeafExpressionNode) node).getIdentifier();
        labelOrdinal = this.labels.get(identifier);
      } else {
        // This is a NOT node.
        LeafExpressionNode lNode = (LeafExpressionNode) ((NonLeafExpressionNode) node)
            .getChildExps().get(0);
        identifier = lNode.getIdentifier();
        labelOrdinal = this.labels.get(identifier);
        labelOrdinal = -1 * labelOrdinal; // Store NOT node as -ve ordinal.
      }
      if (labelOrdinal == 0) {
        throw new InvalidLabelException("Invalid visibility label " + identifier);
      }
      labelOrdinals.add(labelOrdinal);
    } else {
      List<ExpressionNode> childExps = ((NonLeafExpressionNode) node).getChildExps();
      for (ExpressionNode child : childExps) {
        getLabelOrdinals(child, labelOrdinals);
      }
    }
  }

  private void createLabels() throws IOException {
    // This scan should be done by user with global_admin previliges.. Ensure
    // that it works
    HTable visibilityLabelsTable = null;
    ResultScanner scanner = null;
    try {
      labels = new HashMap<String, Integer>();
      visibilityLabelsTable = new HTable(conf, LABELS_TABLE_NAME.getName());
      Scan scan = new Scan();
      scan.setAuthorizations(new Authorizations(VisibilityUtils.SYSTEM_LABEL));
      scan.addColumn(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
      scanner = visibilityLabelsTable.getScanner(scan);
      while (true) {
        Result next = scanner.next();
        if (next == null) {
          break;
        }
        byte[] row = next.getRow();
        byte[] value = next.getValue(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
        labels.put(Bytes.toString(value), Bytes.toInt(row));
      }
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } finally {
        if (visibilityLabelsTable != null) {
          visibilityLabelsTable.close();
        }
      }
    }
  }

  /**
   * Creates a kv from the cell visibility expr specified in the ImportTSV and uses it as the
   * visibility tag in the kv
   * @param rowKeyOffset
   * @param rowKeyLength
   * @param family
   * @param familyOffset
   * @param familyLength
   * @param qualifier
   * @param qualifierOffset
   * @param qualifierLength
   * @param ts
   * @param put
   * @param lineBytes
   * @param columnOffset
   * @param columnLength
   * @param cellVisibilityExpr
   * @return KeyValue from the cell visibility expr
   * @throws IOException
   * @throws BadTsvLineException
   * @throws ParseException 
   */
  public KeyValue createKVFromCellVisibilityExpr(int rowKeyOffset, int rowKeyLength, byte[] family,
      int familyOffset, int familyLength, byte[] qualifier, int qualifierOffset,
      int qualifierLength, long ts, Type put, byte[] lineBytes, int columnOffset, int columnLength,
      String cellVisibilityExpr) throws IOException, BadTsvLineException {
    if(this.labels == null  && cellVisibilityExpr != null) {
      createLabels();
    }
    KeyValue kv = null;
    if (cellVisibilityExpr != null) {
      // Apply the expansion and parsing here
      try {
        List<Tag> visibilityTags = createVisibilityTags(cellVisibilityExpr);
        kv = new KeyValue(lineBytes, rowKeyOffset, rowKeyLength, family, familyOffset,
            familyLength, qualifier, qualifierOffset, qualifierLength, ts, KeyValue.Type.Put,
            lineBytes, columnOffset, columnLength, visibilityTags);
      } catch (ParseException e) {
        throw new BadTsvLineException("Parse Exception " + e.getMessage());
      }
    } else {
      kv = new KeyValue(lineBytes, rowKeyOffset, rowKeyLength, family, familyOffset, familyLength,
          qualifier, qualifierOffset, qualifierLength, ts, KeyValue.Type.Put, lineBytes, columnOffset,
          columnLength);
    }
    return kv;
  }
}
