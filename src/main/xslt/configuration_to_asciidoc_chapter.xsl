<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">


<!--
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

This stylesheet is used making an html version of hbase-default.adoc.
-->

<xsl:output method="text"/>

<!-- Normalize space -->
<xsl:template match="text()">
    <xsl:if test="normalize-space(.)">
      <xsl:value-of select="normalize-space(.)"/>
    </xsl:if>
</xsl:template>

<!-- Grab nodes of the <configuration> element -->
<xsl:template match="configuration">

<!-- Print the license at the top of the file -->
////
/**
 *
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
////
:doctype: book
:numbered:
:toc: left
:icons: font
:experimental:

[[hbase_default_configurations]]
=== HBase Default Configuration

The documentation below is generated using the default hbase configuration file, _hbase-default.xml_, as source.

  <xsl:for-each select="property">
    <xsl:if test="not(@skipInDoc)">
[[<xsl:apply-templates select="name"/>]]
`<xsl:apply-templates select="name"/>`::
+
.Description
<xsl:apply-templates select="description"/>
+
.Default
<xsl:choose>
  <xsl:when test="value != ''">`<xsl:apply-templates select="value"/>`

</xsl:when>
  <xsl:otherwise>none</xsl:otherwise>
</xsl:choose>
    </xsl:if>
  </xsl:for-each>

</xsl:template>
</xsl:stylesheet>
