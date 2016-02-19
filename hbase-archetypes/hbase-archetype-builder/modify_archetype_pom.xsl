<?xml version="1.0" encoding="UTF-8"?>
<xsl:transform version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:pom="http://maven.apache.org/POM/4.0.0"
    exclude-result-prefixes="pom">
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
-->
  <xsl:output indent="yes"/>

  <!-- copy all items from source to target with standard 'identity' template -->
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <!-- if properties element doesn't exist, insert it with sourceEncoding subelement -->
  <xsl:template match="pom:project[not(pom:properties)]">
    <xsl:copy>
      <xsl:apply-templates select="@*"/>
      <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
      </properties>
      <xsl:apply-templates select="node()"/>
    </xsl:copy>
  </xsl:template>

  <!-- if properties element exists without sourceEncoding subelement, insert it -->
  <xsl:template match="pom:properties[not(pom:project.build.sourceEncoding)]">
    <xsl:copy>
      <xsl:apply-templates select="@*"/>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
      <xsl:apply-templates select="node()"/>
    </xsl:copy>
  </xsl:template>

</xsl:transform>
