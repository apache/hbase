// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
pipeline {
  agent {
    node {
      label 'hbase'
    }
  }
  triggers {
    cron('@daily')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '20'))
    timeout (time: 16, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  environment {
    HADOOP_VERSIONS = "2.10.2,3.2.4,3.3.5,3.3.6,3.4.0,3.4.1,3.4.2,3.4.3"
    BASEDIR = "${env.WORKSPACE}/component"
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  stages {
    stage('scm-checkout') {
      steps {
        dir('component') {
          checkout scm
        }
      }
    }
    // This is meant to mimic what a release manager will do to create RCs.
    // See http://hbase.apache.org/book.html#maven.release
    // TODO (HBASE-23870): replace this with invocation of the release tool
    stage ('packaging test') {
      steps {
        sh '''#!/bin/bash -e
          echo "Setting up directories"
          rm -rf "output-srctarball" && mkdir "output-srctarball"
          rm -rf "unpacked_src_tarball" && mkdir "unpacked_src_tarball"
          rm -rf ".m2-for-repo" && mkdir ".m2-for-repo"
          rm -rf ".m2-for-src" && mkdir ".m2-for-src"
        '''
        sh '''#!/bin/bash -e
          rm -rf "output-srctarball/machine" && mkdir "output-srctarball/machine"
          "${BASEDIR}/dev-support/gather_machine_environment.sh" "output-srctarball/machine"
          echo "got the following saved stats in 'output-srctarball/machine'"
          ls -lh "output-srctarball/machine"
        '''
        sh '''#!/bin/bash -e
          echo "Checking the steps for an RM to make a source artifact, then a binary artifact."
          docker build -t hbase-integration-test -f "${BASEDIR}/dev-support/docker/Dockerfile" .
          docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
            -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-17" --workdir=/hbase hbase-integration-test \
            "component/dev-support/integration-test/source-artifact.sh" \
            --intermediate-file-dir output-srctarball \
            --unpack-temp-dir unpacked_src_tarball \
            --maven-m2-initial .m2-for-repo \
            --maven-m2-src-build .m2-for-src \
            --clean-source-checkout \
            component
          if [ $? -eq 0 ]; then
            echo '(/) {color:green}+1 source release artifact{color}\n-- See build output for details.' >output-srctarball/commentfile
          else
            echo '(x) {color:red}-1 source release artifact{color}\n-- See build output for details.' >output-srctarball/commentfile
            exit 1
          fi
        '''
        echo "make sure we have proper hbase tarballs under hbase-assembly"
        sh '''#!/bin/bash -e
          if [ 2 -ne $(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz | grep -v hadoop3 | wc -l) ]; then
            echo '(x) {color:red}-1 testing binary artifact{color}\n-- source tarball did not produce the expected binaries.' >>output-srctarball/commentfile
            exit 1
          fi
          if [[ "${BRANCH_NAME}" == *"branch-2"* ]]; then
            if [ 2 -eq $(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-hadoop3-*-bin.tar.gz | wc -l) ]; then
               echo '(x) {color:red}-1 testing binary artifact{color}\n-- source tarball did not produce the expected hadoop3 binaries.' >>output-srctarball/commentfile
               exit 1
            fi
          fi
        '''
        stash name: 'hbase-install', includes: "unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz"
      } // steps
      post {
        always {
          script {
            def srcFile = "${env.WORKSPACE}/output-srctarball/hbase-src.tar.gz"
            if (fileExists(srcFile)) {
              echo "upload hbase-src.tar.gz to nightlies"
              sshPublisher(publishers: [
                sshPublisherDesc(configName: 'Nightlies',
                  transfers: [
                    sshTransfer(remoteDirectory: "hbase/${JOB_NAME}/${BUILD_NUMBER}",
                      sourceFiles: srcFile
                    )
                  ]
                )
              ])
              // remove the big src tarball, store the nightlies url in hbase-src.html
              sh '''#!/bin/bash -e
                SRC_TAR="${WORKSPACE}/output-srctarball/hbase-src.tar.gz"
                echo "Remove ${SRC_TAR} for saving space"
                rm -rf "${SRC_TAR}"
                python3 ${BASEDIR}/dev-support/gen_redirect_html.py "${ASF_NIGHTLIES_BASE}/output-srctarball" > "${WORKSPACE}/output-srctarball/hbase-src.html"
              '''
            }
          }
          archiveArtifacts artifacts: 'output-srctarball/*'
          archiveArtifacts artifacts: 'output-srctarball/**/*'
        }
      }
    } // packaging test
    stage ('integration test matrix') {
      matrix {
        agent {
          node {
            label 'hbase'
          }
        }
        axes {
          axis {
            name 'HADOOP_VERSION'
            // matrix does not support dynamic axis values, so here we need to keep align with the
            // above environment
            values "2.10.2","3.2.4","3.3.5","3.3.6","3.4.0","3.4.1","3.4.2","3.4.3"
          }
        }
        environment {
          BASEDIR = "${env.WORKSPACE}/component"
          OUTPUT_DIR = "output-integration-hadoop-${env.HADOOP_VERSION}"
        }
        when {
          expression {
            if (HADOOP_VERSION == '2.10.2') {
              // only branch-2/branch-2.x need to run against hadoop2, here we also includes
              // HBASE-XXXXX-branch-2 feature branch
              return env.BRANCH_NAME.contains('branch-2')
            }
            if (HADOOP_VERSION == '3.2.4') {
              // only branch-2.5 need to run against hadoop 3.2.4, here we also includes
              // HBASE-XXXXX-branch-2.5 feature branch
              return env.BRANCH_NAME.contains('branch-2.5')
            }
            return true
          }
        }
        stages {
          stage('scm-checkout') {
            steps {
              sh '''#!/bin/bash -e
                echo "Setting up directories"
                rm -rf "${OUTPUT_DIR}" && mkdir "${OUTPUT_DIR}"
                echo "(x) {color:red}-1 client integration test for ${HADOOP_VERSION}{color}\n-- Something went wrong with this stage, [check relevant console output|${BUILD_URL}/console]." >${OUTPUT_DIR}/commentfile
                rm -rf "unpacked_src_tarball"
                rm -rf "hbase-install" && mkdir "hbase-install"
                rm -rf "hbase-client" && mkdir "hbase-client"
                rm -rf "hadoop-install" && mkdir "hadoop-install"
                rm -rf "hbase-hadoop3-install"
                rm -rf "hbase-hadoop3-client"
                # remove old hadoop tarballs in workspace
                rm -rf hadoop-*.tar.gz
              '''
              dir('component') {
                checkout scm
              }
            } // steps
          } // scm-checkout
          stage('install hadoop') {
            steps {
              dir("downloads-hadoop") {
                sh '''#!/bin/bash -e
                  echo "Make sure we have a directory for downloading dependencies: $(pwd)"
                '''
                sh '''#!/bin/bash -e
                  echo "Ensure we have a copy of Hadoop ${HADOOP_VERSION}"
                  "${WORKSPACE}/component/dev-support/jenkins-scripts/cache-apache-project-artifact.sh" \
                    --working-dir "${WORKSPACE}/downloads-hadoop" \
                    --keys 'https://downloads.apache.org/hadoop/common/KEYS' \
                    --verify-tar-gz \
                    "${WORKSPACE}/hadoop-${HADOOP_VERSION}-bin.tar.gz" \
                    "hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
                  for stale in $(ls -1 "${WORKSPACE}"/hadoop-*.tar.gz | grep -v ${HADOOP_VERSION}); do
                    echo "Delete stale hadoop cache ${stale}"
                    rm -rf $stale
                  done
                  artifact=$(ls -1 "${WORKSPACE}"/hadoop-${HADOOP_VERSION}-bin.tar.gz | head -n 1)
                  tar --strip-components=1 -xzf "${artifact}" -C "${WORKSPACE}/hadoop-install"
                  if [[ ${HADOOP_VERSION} == 3.* ]]; then
                    # we need to patch some files otherwise minicluster will fail to start, see MAPREDUCE-7471
                    ${BASEDIR}/dev-support/integration-test/patch-hadoop3.sh "${WORKSPACE}/hadoop-install"
                  fi
                '''
              } // dir
            } // steps
          } // install hadoop
          stage('install hbase') {
            steps {
              unstash 'hbase-install'
              sh'''#!/bin/bash -e
                install_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz | grep -v client-bin | grep -v hadoop3)
                tar --strip-component=1 -xzf "${install_artifact}" -C "hbase-install"
                client_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-client-bin.tar.gz | grep -v hadoop3)
                tar --strip-component=1 -xzf "${client_artifact}" -C "hbase-client"
                if ls "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-hadoop3-*-bin.tar.gz &>/dev/null; then
                  echo "hadoop3 artifacts available, unpacking the hbase hadoop3 bin tarball into 'hbase-hadoop3-install' and the client hadoop3 tarball into 'hbase-hadoop3-client'"
                  mkdir hbase-hadoop3-install
                  mkdir hbase-hadoop3-client
                  hadoop3_install_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-hadoop3-*-bin.tar.gz | grep -v client-bin)
                  tar --strip-component=1 -xzf "${hadoop3_install_artifact}" -C "hbase-hadoop3-install"
                  hadoop3_client_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-hadoop3-*-client-bin.tar.gz)
                  tar --strip-component=1 -xzf "${hadoop3_client_artifact}" -C "hbase-hadoop3-client"
                fi
              '''
            } // steps
          }
          stage('integration test ') {
            steps {
              sh '''#!/bin/bash -e
                hbase_install_dir="hbase-install"
                hbase_client_dir="hbase-client"
                if [[ ${HADOOP_VERSION} == 3.* ]] && [[ -d "hbase-hadoop3-install" ]]; then
                  echo "run hadoop3 client integration test against hbase hadoop3 binaries"
                  hbase_install_dir="hbase-hadoop3-install"
                  hbase_client_dir="hbase-hadoop3-client"
                fi
                java_home="/usr/lib/jvm/java-17"
                if [[ ${HADOOP_VERSION} == 2.* ]]; then
                  java_home="/usr/lib/jvm/java-8"
                fi
                echo "Attempting to run an instance on top of Hadoop ${HADOOP_VERSION}."
                # Create working dir
                rm -rf "${OUTPUT_DIR}/non-shaded" && mkdir "${OUTPUT_DIR}/non-shaded"
                docker build -t hbase-integration-test -f "${BASEDIR}/dev-support/docker/Dockerfile" .
                docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
                  -u `id -u`:`id -g` -e JAVA_HOME="${java_home}" \
                  -e HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" \
                  --workdir=/hbase hbase-integration-test \
                  component/dev-support/integration-test/pseudo-distributed-test.sh \
                  --single-process \
                  --working-dir ${OUTPUT_DIR}/non-shaded \
                  --hbase-client-install ${hbase_client_dir} \
                  ${hbase_install_dir} \
                  hadoop-install/bin/hadoop \
                  hadoop-install/share/hadoop/yarn/timelineservice \
                  hadoop-install/share/hadoop/yarn/test/hadoop-yarn-server-tests-*-tests.jar \
                  hadoop-install/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
                  hadoop-install/bin/mapred \
                  >${OUTPUT_DIR}/hadoop.log 2>&1
                if [ $? -ne 0 ]; then
                  echo "(x) {color:red}-1 client integration test for ${HADOOP_VERSION}{color}\n--Failed when running client tests on top of Hadoop ${HADOOP_VERSION}. [see log for details|${BUILD_URL}/artifact/${OUTPUT_DIR}/hadoop.log]. (note that this means we didn't check the Hadoop ${HADOOP_VERSION} shaded client)" >${OUTPUT_DIR}/commentfile
                  exit 2
                fi
                echo "(/) {color:green}+1 client integration test for ${HADOOP_VERSION} {color}" >${OUTPUT_DIR}/commentfile
                if [[ ${HADOOP_VERSION} == 2.* ]] || [[ ${HADOOP_VERSION} == 3.2.* ]]; then
                  echo "skip running shaded hadoop client test for ${HADOOP_VERSION}"
                  exit 0
                fi
                # Create working dir
                rm -rf "${OUTPUT_DIR}/shaded" && mkdir "${OUTPUT_DIR}/shaded"
                echo "Attempting to run an instance on top of Hadoop ${HADOOP_VERSION}, relying on the Hadoop client artifacts for the example client program."
                docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
                  -u `id -u`:`id -g` -e JAVA_HOME="${java_home}" \
                  -e HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" \
                  --workdir=/hbase hbase-integration-test \
                  component/dev-support/integration-test/pseudo-distributed-test.sh \
                  --single-process \
                  --hadoop-client-classpath hadoop-install/share/hadoop/client/hadoop-client-api-*.jar:hadoop-install/share/hadoop/client/hadoop-client-runtime-*.jar \
                  --working-dir ${OUTPUT_DIR}/shaded \
                  --hbase-client-install ${hbase_client_dir} \
                  ${hbase_install_dir} \
                  hadoop-install/bin/hadoop \
                  hadoop-install/share/hadoop/yarn/timelineservice \
                  hadoop-install/share/hadoop/yarn/test/hadoop-yarn-server-tests-*-tests.jar \
                  hadoop-install/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
                  hadoop-install/bin/mapred \
                  >${OUTPUT_DIR}/hadoop-shaded.log 2>&1
                if [ $? -ne 0 ]; then
                  echo "(x) {color:red}-1 client integration testfor ${HADOOP_VERSION}{color}\n--Failed when running client tests on top of Hadoop ${HADOOP_VERSION} using Hadoop's shaded client. [see log for details|${BUILD_URL}/artifact/${OUTPUT_DIR}/hadoop-shaded.log]." >> ${OUTPUT_DIR}/commentfile
                  exit 2
                fi
                echo "(/) {color:green}+1 client integration test for ${HADOOP_VERSION} with shaded hadoop client{color}" >> ${OUTPUT_DIR}/commentfile
              '''
            } // steps
            post {
              always {
                stash name: "test-result-${env.HADOOP_VERSION}", includes: "${env.OUTPUT_DIR}/commentfile"
                archiveArtifacts artifacts: "${env.OUTPUT_DIR}/*"
                archiveArtifacts artifacts: "${env.OUTPUT_DIR}/**/*"
              } // always
            } // post
          } // integration test
        } // stages
      } // matrix
    } // integration test matrix
  } // stages
  post {
    always {
      script {
        def results = []
        results.add('output-srctarball/commentfile')
        for (hadoopVersion in getHadoopVersions(env.HADOOP_VERSIONS)) {
          try {
            unstash "test-result-${hadoopVersion}"
            results.add("output-integration-hadoop-${hadoopVersion}/commentfile")
          } catch (e) {
            echo "unstash ${hadoopVersion} failed, ignore"
          }
        }
        echo env.BRANCH_NAME
        echo env.BUILD_URL
        echo currentBuild.result
        echo currentBuild.durationString
        def comment = "Results for branch ${env.BRANCH_NAME}\n"
        comment += "\t[build ${currentBuild.displayName} on builds.a.o|${env.BUILD_URL}]: "
        if (currentBuild.result == null || currentBuild.result == "SUCCESS") {
          comment += "(/) *{color:green}+1 overall{color}*\n"
        } else {
          comment += "(x) *{color:red}-1 overall{color}*\n"
          // Ideally get the committer our of the change and @ mention them in the per-jira comment
        }
        comment += "----\ndetails (if available):\n\n"
        echo ""
        echo "[DEBUG] trying to aggregate step-wise results"
        comment += results.collect { fileExists(file: it) ? readFile(file: it) : "" }.join("\n\n")
        echo "[INFO] Comment:"
        echo comment
        echo ""
        echo "[DEBUG] checking to see if feature branch"
        def jiras = getJirasToComment(env.BRANCH_NAME, [])
        if (jiras.isEmpty()) {
          echo "[DEBUG] non-feature branch, checking change messages for jira keys."
          echo "[INFO] There are ${currentBuild.changeSets.size()} change sets."
          jiras = getJirasToCommentFromChangesets(currentBuild)
        }
        jiras.each { currentIssue ->
          jiraComment issueKey: currentIssue, body: comment
        }
      } // script
    } // always
  } // post
}

@NonCPS
List<String> getHadoopVersions(String versions) {
  return versions.split(',').collect { it.trim() }.findAll { it } as String[]
}

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper
@NonCPS
List<String> getJirasToCommentFromChangesets(RunWrapper thisBuild) {
  def seenJiras = []
  thisBuild.changeSets.each { cs ->
    cs.getItems().each { change ->
      CharSequence msg = change.msg
      echo "change: ${change}"
      echo "     ${msg}"
      echo "     ${change.commitId}"
      echo "     ${change.author}"
      echo ""
      seenJiras = getJirasToComment(msg, seenJiras)
    }
  }
  return seenJiras
}

@NonCPS
List<String> getJirasToComment(CharSequence source, List<String> seen) {
  source.eachMatch("HBASE-[0-9]+") { currentIssue ->
    echo "[DEBUG] found jira key: ${currentIssue}"
    if (currentIssue in seen) {
      echo "[DEBUG] already commented on ${currentIssue}."
    } else {
      echo "[INFO] commenting on ${currentIssue}."
      seen << currentIssue
    }
  }
  return seen
}

