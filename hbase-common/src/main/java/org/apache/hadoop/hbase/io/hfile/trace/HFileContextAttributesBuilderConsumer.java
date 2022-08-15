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
package org.apache.hadoop.hbase.io.hfile.trace;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.CHECKSUM_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.COMPRESSION_ALGORITHM_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DATA_BLOCK_ENCODING_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ENCRYPTION_CIPHER_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.HFILE_NAME_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.READ_TYPE_KEY;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ReadType;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * <p>
 * Populate fields on an {@link AttributesBuilder} based on an {@link HFileContext}. Passed around
 * inside an active {@link Context}, indexed under {@link #CONTEXT_KEY}. The class is designed such
 * that calls to the {@link #accept(AttributesBuilder)} method are idempotent with regards to the
 * instance of this class.
 * </p>
 * <p>
 * The true and truly ridiculous class name should be something more like
 * {@code HFileContext_ContextAttributes_AttributesBuilder_Consumer}.
 * </p>
 */
@InterfaceAudience.Private
public class HFileContextAttributesBuilderConsumer implements Consumer<AttributesBuilder> {

  /**
   * Used to place extract attributes pertaining to the {@link HFileContext} that scopes the active
   * {@link Context}.
   */
  public static final ContextKey<Consumer<AttributesBuilder>> CONTEXT_KEY =
    ContextKey.named("db.hbase.io.hfile.context_attributes");

  private final HFileContext hFileContext;

  private boolean skipChecksum = false;
  private ReadType readType = null;

  public HFileContextAttributesBuilderConsumer(final HFileContext hFileContext) {
    this.hFileContext = Objects.requireNonNull(hFileContext);
  }

  /**
   * Specify that the {@link ChecksumType} should not be included in the attributes.
   */
  public HFileContextAttributesBuilderConsumer setSkipChecksum(final boolean skipChecksum) {
    this.skipChecksum = skipChecksum;
    return this;
  }

  /**
   * Specify the {@link ReadType} involced in this IO operation.
   */
  public HFileContextAttributesBuilderConsumer setReadType(final ReadType readType) {
    // TODO: this is not a part of the HFileBlock, its context of the operation. Should track this
    // detail elsewhere.
    this.readType = readType;
    return this;
  }

  @Override
  public void accept(AttributesBuilder builder) {
    if (hFileContext.getHFileName() != null) {
      builder.put(HFILE_NAME_KEY, hFileContext.getHFileName());
    }
    if (hFileContext.getCompression() != null) {
      builder.put(COMPRESSION_ALGORITHM_KEY, hFileContext.getCompression().getName());
    }
    if (hFileContext.getDataBlockEncoding() != null) {
      builder.put(DATA_BLOCK_ENCODING_KEY, hFileContext.getDataBlockEncoding().name());
    }
    if (
      hFileContext.getEncryptionContext() != null
        && hFileContext.getEncryptionContext().getCipher() != null
    ) {
      builder.put(ENCRYPTION_CIPHER_KEY, hFileContext.getEncryptionContext().getCipher().getName());
    }
    if (!skipChecksum && hFileContext.getChecksumType() != null) {
      builder.put(CHECKSUM_KEY, hFileContext.getChecksumType().getName());
    }
    if (readType != null) {
      builder.put(READ_TYPE_KEY, readType.name());
    }
  }
}
