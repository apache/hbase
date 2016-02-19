<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

#hbase-archetypes

##Overview
The hbase-archetypes subproject of hbase provides an infrastructure for
creation and maintenance of Maven archetypes<sup id="a1">[1](#f1)</sup>
pertinent to HBase. Upon deployment to the archetype
catalog<sup id="a2">[2](#f2)</sup> of the central Maven
repository<sup id="a3">[3](#f3)</sup>, these archetypes may be used by
end-user developers to autogenerate completely configured Maven projects
(including fully-functioning sample code) through invocation of the
`archetype:generate` goal of the
maven-archetype-plugin<sup id="a4">[4](#f4)</sup>.

##Notes for contributors and committers to the HBase project

####The structure of hbase-archetypes
The hbase-archetypes project contains a separate subproject for each archetype.
The top level components of such a subproject comprise a complete, standalone
exemplar Maven project containing:

- a `src` directory with sample, fully-functioning code in the `./main` and
`./test` subdirectories,
- a `pom.xml` file defining all required dependencies, and
- any additional resources required by the exemplar project.

For example, the components of the hbase-client-project consist of (a) sample
code `./src/main/.../HelloHBase.java` and `./src/test/.../TestHelloHBase.java`,
(b) a `pom.xml` file establishing dependency upon hbase-client and test-scope
dependency upon hbase-testing-util, and (c) a `log4j.properties` resource file.

####How archetypes are created during the hbase install process
During the `mvn install` process, all standalone exemplar projects in the
`hbase-archetypes` subdirectory are first packaged/tested/installed, and then
the following steps are executed in `hbase-archetypes/hbase-archetype-builder`
(via the `pom.xml`, bash scripts, and xsl templates in that subdirectory):

1. For each exemplar project, resources are copied (via
maven-resources-plugin) and transformed (via xml-maven-plugin xslt
functionality) to the exemplar project's `./target/build-archetype`
subdirectory<sup id="a5">[5](#f5)</sup>.
2. The script `createArchetypes.sh` is executed to invoke the
maven-archetype-plugin's `create-from-project` goal within each exemplar
project's `./target/build-archetype` subdirectory. For each exemplar
project, this creates a corresponding Maven archetype in the
`./target/build-archetype/target/generate-sources/archetype` subdirectory.
(Note that this step always issues two platform-encoding warnings per
archetype, due to hard-wired behavior of the
maven-archetype-plugin<sup id="a6">[6](#f6)</sup>.)
3. The `pom.xml` file of each newly-created archetype is copied (via
maven-resources-plugin) and transformed (via xml-maven-plugin xslt
functionality)<sup id="a7">[7](#f7)</sup>.
4. The script `installArchetypes.sh` is executed to install each archetype
into the local Maven repository, ready for deployment to the central Maven
repository. (Note that installation of an archetype automatically includes
invocation of integration-testing prior to install, which performs a test
generation of a project from the archetype.)

####How to add a new archetype to the hbase-archetypes collection
1. Create a new subdirectory in `hbase-archetypes`, populated with a
completely configured Maven project, which will serve as the exemplar project
of the new archetype. (It may be most straightforward to simply copy the `src`
and `pom.xml` components from one of the existing exemplar projects, replace
the `src/main` and `src/test` code, and modify the `pom.xml` file's
`<dependencies>`, `<artifactId>`,` <name>`, and `<description>` elements.)
2. Modify the `hbase-archetype-builder/pom.xml` file: (a) add the new exemplar
project to the `<modules>` element, and (b) add appropriate `<execution>`
elements and `<transformationSet>` elements within the `<plugin>` elements
(using the existing entries from already-existing exemplar projects as a guide).
3. Add appropriate entries for the new exemplar project to the
`createArchetypes.sh` and `installArchetypes.sh` scripts in the
`hbase-archetype-builder` subdirectory (using the existing entries as a guide).

####How to do additional testing/inspection of an archetype in this collection
Although integration-testing (which is automatically performed for each
archetype during the install process) already performs test generation of a
project from an archetype, it may often be advisable to do further manual
testing of a newly built and installed archetype, particularly to examine and
test a project generated from the archetype (emulating the end-user experience
of utilizing the archetype). Upon completion of the install process outlined
above, all archetypes will have been installed in the local Maven repository
and can be tested locally by executing the following:
    `mvn archetype:generate -DarchetypeCatalog=local`
This displays a numbered list of all locally-installed archetypes for the user
to choose from for generation of a new Maven project.

##Footnotes:
<b id="f1">1</b> -- [Maven Archetype
](http://maven.apache.org/archetype/index.html) ("About" page).
-- [↩](#a1)

<b id="f2">2</b> -- [Maven Archetype Catalog
](http://repo1.maven.org/maven2/archetype-catalog.xml) (4MB+ xml file).
-- [↩](#a2)

<b id="f3">3</b> -- [Maven Central Repository](http://search.maven.org/)
(search engine).
-- [↩](#a3)

<b id="f4">4</b> -- [Maven Archetype Plugin - archetype:generate
](http://maven.apache.org/archetype/maven-archetype-plugin/generate-mojo.html).
-- [↩](#a4)

<b id="f5">5</b> -- Prior to archetype creation, each exemplar project's
    `pom.xml` is transformed as follows to make it into a standalone project:
    RESOURCE FILTERING (a) replaces `${project.version}` with the literal value
    of the current project.version and (b) replaces `${compileSource}` with the
    literal value of the version of Java that is being used for compilation;
    XSLT TRANSFORMATION (a) copies `<groupId>` and `<version>` subelements of
    `<parent>` to make them child elements of the root element, and (b) removes
    the `<parent>` and `<description>` elements.
    -- [↩](#a5)

<b id="f6">6</b> -- For an explanation of the platform-encoding warning issued
    during maven-archetype-plugin processing, see the first answer to [this
    stackoverflow posting](http://stackoverflow.com/a/24161287/4112172).
    -- [↩](#a6)

<b id="f7">7</b> -- Prior to archetype installation, each archetype's `pom.xml`
    is transformed as follows: a `<project.build.sourceEncoding>` subelement
    with value 'UTF-8' is added to the `<properties>` element. This prevents
    platform-encoding warnings from being issued when an end-user generates
    a project from the archetype.
    -- [↩](#a7)
