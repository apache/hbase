#!/usr/bin/env python3
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Check classifier and binary artifacts built with the root POM dependency versions.
# Run after Maven package assembly:single, without third-party Python packages.

import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import xml.etree.ElementTree as ET
import zipfile


root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
pom = ET.parse(root / "pom.xml").getroot()
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
properties = pom.find("m:properties", namespace)
versions = [properties.findtext("m:" + name + ".version", namespaces=namespace)
            for name in ("bootstrap", "jquery", "tablesorter", "vega", "vega-lite", "vega-embed")]


def check_licenses(license_text, notice_text):
    missing = [version for version in versions if version not in license_text + notice_text]
    assert not missing, ("Missing bundled license versions", missing)
    assert "Copyright OpenJS Foundation" in license_text
    assert "Copyright (c) 2007 Christian Bach" in license_text
    assert "Copyright (c) 2015-2023" in license_text
    assert "Bootstrap" in notice_text


classifiers = list((root / "hbase-server/target").glob("hbase-server-*-static-resources.jar"))
assert len(classifiers) == 1, classifiers
classifier = classifiers[0]
version = classifier.name[len("hbase-server-"):-len("-static-resources.jar")]
with zipfile.ZipFile(classifier) as archive:
    expected = {name: hashlib.sha256(archive.read(name)).hexdigest()
               for name in archive.namelist()
               if name.startswith("hbase-webapps/static/") and not name.endswith("/")}
    required = {"hbase-webapps/static/" + name for name in (
        "css/bootstrap.min.css", "js/bootstrap.bundle.min.js", "js/jquery.min.js",
        "js/jquery.tablesorter.min.js", "js/parser-date-iso8601.min.js",
        "js/vega.min.js", "js/vega-lite.min.js", "js/vega-embed.min.js")}
    assert required <= expected.keys(), required - expected.keys()
    assert all(name.startswith(("META-INF/", "hbase-webapps/static/"))
               for name in archive.namelist() if not name.endswith("/"))
    license_text = archive.read("META-INF/LICENSE").decode().lower()
    notice_text = archive.read("META-INF/NOTICE").decode().lower()
    assert all(word in license_text for word in ("jquery", "vega", "creative commons"))
    assert "bootstrap" in notice_text
    check_licenses(archive.read("META-INF/LICENSE").decode(),
                   archive.read("META-INF/NOTICE").decode())

binary = root / "hbase-assembly/target" / f"hbase-{version}-bin.tar.gz"
payload = {}
descriptors = {}
jars = {}
marker_files = []
licenses = {}
with tarfile.open(binary, "r|gz") as archive:
    for member in archive:
        if not member.isfile():
            continue
        name = member.name.split("/", 1)[1]
        assert "hbase29355-poison" not in name, name
        if "static-resources" in name and name.endswith(".jar"):
            marker_files.append(name)
        if name in ("LICENSE.txt", "NOTICE.txt"):
            licenses[name] = archive.extractfile(member).read().decode()
        if name.startswith("hbase-webapps/static/"):
            payload[name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
        for app in ("rest", "thrift"):
            if name == f"hbase-webapps/{app}/WEB-INF/web.xml":
                descriptors[app] = archive.extractfile(member).read()
        for module in ("server", "rest", "thrift"):
            if name == f"lib/hbase-{module}-{version}.jar":
                jars[module] = archive.extractfile(member).read()
assert payload == expected, (payload.keys(), expected.keys())
assert not marker_files, marker_files
assert set(jars) == {"server", "rest", "thrift"}, jars.keys()
assert set(descriptors) == {"rest", "thrift"}, descriptors.keys()
assert set(licenses) == {"LICENSE.txt", "NOTICE.txt"}, licenses.keys()
check_licenses(licenses["LICENSE.txt"], licenses["NOTICE.txt"])
servlet_counts = {}
for module, content in jars.items():
    with zipfile.ZipFile(io.BytesIO(content)) as jar:
        names = set(jar.namelist())
        assert not any(name.startswith("hbase-webapps/") for name in names), module
        if module in descriptors:
            classes = [element.text for element in ET.fromstring(descriptors[module]).iter()
                       if element.tag.rsplit("}", 1)[-1] == "servlet-class"
                       and element.text.startswith("org.apache.hadoop.hbase.generated.")]
            assert classes, module
            assert all(name.replace(".", "/") + ".class" in names for name in classes), classes
            servlet_counts[module] = len(classes)
result = {
    "binary_archive": str(binary),
    "archive_bytes": binary.stat().st_size,
    "static_files": len(payload),
    "all_static_hashes_match_classifier": True,
    "runtime_resource_classifier_jars": marker_files,
    "main_jars_exclude_webapps": sorted(jars),
    "compiled_jsp_servlets": servlet_counts,
    "classifier_has_bundled_work_license_and_notice": True,
    "classifier_and_binary_licenses_match_pom_versions": versions,
}
print(json.dumps(result, indent=2))
