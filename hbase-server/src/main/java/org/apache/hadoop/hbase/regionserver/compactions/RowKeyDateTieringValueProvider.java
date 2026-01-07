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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a tiering value for compactions by extracting and parsing a date from the row key. This
 * implementation uses a configurable regex and date format to locate and parse a date substring
 * from the row key and returns the parsed epoch time in milliseconds. Configuration properties can
 * be set at globally or at table level.
 */
@InterfaceAudience.Private
public class RowKeyDateTieringValueProvider implements CustomTieredCompactor.TieringValueProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RowKeyDateTieringValueProvider.class);
  public static final String TIERING_KEY_DATE_PATTERN =
    "hbase.hstore.datatiering.tieringvalueprovider.regexpattern";
  public static final String TIERING_KEY_DATE_FORMAT =
    "hbase.hstore.datatiering.tieringvalueprovider.dateformat";
  public static final String TIERING_KEY_DATE_GROUP =
    "hbase.hstore.datatiering.tieringvalueprovider.regexextractgroup";
  private Pattern rowKeyPattern;
  private SimpleDateFormat dateFormat;
  private Integer rowKeyRegexExtractGroup;

  @Override
  public void init(Configuration conf) throws Exception {
    // Initialize regex pattern
    String regexPatternStr = conf.get(TIERING_KEY_DATE_PATTERN);
    if (regexPatternStr == null || regexPatternStr.isEmpty()) {
      throw new IllegalArgumentException(
        "Configuration property '" + TIERING_KEY_DATE_PATTERN + "' is required");
    }
    rowKeyPattern = Pattern.compile(regexPatternStr);

    // Initialize date format
    String dateFormatStr = conf.get(TIERING_KEY_DATE_FORMAT);
    if (dateFormatStr == null || dateFormatStr.isEmpty()) {
      throw new IllegalArgumentException(
        "Configuration property '" + TIERING_KEY_DATE_FORMAT + "' is required");
    }
    try {
      dateFormat = new SimpleDateFormat(dateFormatStr);
      dateFormat.setLenient(false);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid date format for Configuration property '"
        + TIERING_KEY_DATE_FORMAT + "': " + dateFormatStr, e);
    }

    // Initialize regex extract group
    String extractGroupStr = conf.get(TIERING_KEY_DATE_GROUP, "0");
    try {
      rowKeyRegexExtractGroup = Integer.parseInt(extractGroupStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        "Configuration property '" + TIERING_KEY_DATE_GROUP + "' must be a valid integer", e);
    }
    if (rowKeyRegexExtractGroup < 0) {
      throw new IllegalArgumentException(
        "Configuration property '" + TIERING_KEY_DATE_GROUP + "' must be non-negative");
    }
    // Validate extract group exists in pattern
    int groupCount = rowKeyPattern.matcher("").groupCount();
    if (rowKeyRegexExtractGroup > groupCount) {
      throw new IllegalArgumentException(
        "Extract group " + rowKeyRegexExtractGroup + " exceeds pattern group count " + groupCount);
    }

    LOG.info("Initialized RowKeyDateTieringValueProvider with regex='{}', dateFormat='{}' ",
      regexPatternStr, dateFormat);
  }

  @Override
  public long getTieringValue(ExtendedCell cell) {
    if (rowKeyPattern == null || dateFormat == null || rowKeyRegexExtractGroup == null) {
      throw new IllegalStateException("RowKeyDateTieringValueProvider not initialized properly");
    }
    byte[] rowArray = new byte[cell.getRowLength()];
    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowArray, 0, cell.getRowLength());
    String rowKeyStr;
    try {
      rowKeyStr = Bytes.toString(rowArray);
      // Validate UTF-8 encoding
      if (rowKeyStr.contains("\ufffd")) {
        LOG.debug("Failed to extract the date from row key due to invalid UTF-8 encoding");
        return Long.MAX_VALUE;
      }
    } catch (Exception e) {
      LOG.debug("Failed to convert row key to string", e);
      return Long.MAX_VALUE;
    }

    Matcher matcher = rowKeyPattern.matcher(rowKeyStr);
    if (!matcher.find()) {
      LOG.debug("Row key '{}' does not match the regex pattern", rowKeyStr);
      return Long.MAX_VALUE;
    }

    String extractedValue = null;
    try {
      extractedValue = matcher.group(rowKeyRegexExtractGroup);
      if (extractedValue == null || extractedValue.isEmpty()) {
        LOG.debug("No value extracted from row key '{}' using regex pattern", rowKeyStr);
        return Long.MAX_VALUE;
      }
      return dateFormat.parse(extractedValue).getTime();
    } catch (ParseException e) {
      LOG.debug("Error parsing date value '{}' extracted from row key '{}'", extractedValue,
        rowKeyStr, e);
    } catch (Exception e) {
      LOG.debug("Unexpected error while parsing date from row key '{}'", rowKeyStr, e);
    }
    return Long.MAX_VALUE;
  }

  public Pattern getRowKeyPattern() {
    return rowKeyPattern;
  }

  public SimpleDateFormat getDateFormat() {
    return dateFormat;
  }

  public Integer getRowKeyRegexExtractGroup() {
    return rowKeyRegexExtractGroup;
  }
}
