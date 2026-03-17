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

// Jenkinsfile for Hadoop3 Backwards Compatibility Checks
// Uses matrix job to parallelize checks across different Hadoop3 versions

pipeline {
  agent {
    node {
      label 'hbase'
    }
  }
  triggers {
    pollSCM('H H */2 * *')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '20'))
    timeout (time: 8, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  environment {
    YETUS_RELEASE = '0.15.0'
    HADOOP_VERSIONS = "3.2.4,3.3.5,3.3.6,3.4.0,3.4.1,3.4.2"
  }
  parameters {
    booleanParam(name: 'USE_YETUS_PRERELEASE', defaultValue: false, description: '''Check to use the current HEAD of apache/yetus rather than our configured release.

    Should only be used manually when e.g. there is some non-work-aroundable issue in yetus we are checking a fix for.''')
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  stages {
    stage ('scm-checkout') {
      steps {
        dir('component') {
          checkout scm
        }
      }
    }
    stage ('thirdparty installs') {
      parallel {
        stage ('yetus install') {
          steps {
            dir('downloads-yetus') {
              sh '''#!/usr/bin/env bash
                echo "Make sure we have a directory for downloading dependencies: $(pwd)"
              '''
            }
            sh  '''#!/usr/bin/env bash
              set -e
              echo "Ensure we have a copy of Apache Yetus."
              if [[ true !=  "${USE_YETUS_PRERELEASE}" ]]; then
                YETUS_DIR="${WORKSPACE}/yetus-${YETUS_RELEASE}"
                echo "Checking for Yetus ${YETUS_RELEASE} in '${YETUS_DIR}'"
                if ! "${YETUS_DIR}/bin/test-patch" --version >/dev/null 2>&1 ; then
                  rm -rf "${YETUS_DIR}"
                  "${WORKSPACE}/component/dev-support/jenkins-scripts/cache-apache-project-artifact.sh" \
                      --working-dir "${WORKSPACE}/downloads-yetus" \
                      --keys 'https://downloads.apache.org/yetus/KEYS' \
                      --verify-tar-gz \
                      "${WORKSPACE}/yetus-${YETUS_RELEASE}-bin.tar.gz" \
                      "yetus/${YETUS_RELEASE}/apache-yetus-${YETUS_RELEASE}-bin.tar.gz"
                  mv "yetus-${YETUS_RELEASE}-bin.tar.gz" yetus.tar.gz
                else
                  echo "Reusing cached install of Apache Yetus version ${YETUS_RELEASE}."
                fi
              else
                YETUS_DIR="${WORKSPACE}/yetus-git"
                rm -rf "${YETUS_DIR}"
                echo "downloading from github"
                curl -L --fail https://api.github.com/repos/apache/yetus/tarball/HEAD -o yetus.tar.gz
              fi
              if [ ! -d "${YETUS_DIR}" ]; then
                echo "unpacking yetus into '${YETUS_DIR}'"
                mkdir -p "${YETUS_DIR}"
                gunzip -c yetus.tar.gz | tar xpf - -C "${YETUS_DIR}" --strip-components 1
              fi
            '''
            stash name: 'yetus', includes: "yetus-*/*,yetus-*/**/*"
          }
        }
      }
    }
    stage ('backwards compatibility checks') {
      matrix {
        axes {
          axis {
            name 'HADOOP3_VERSION'
            values '3.2.4', '3.3.5', '3.3.6', '3.4.0', '3.4.1', '3.4.2'
          }
        }
        agent {
          node {
            label 'hbase'
          }
        }
        when {
          expression {
            if (HADOOP3_VERSION == '3.2.4') {
              // only branch-2.5 need to run against hadoop 3.2.4, here we also includes
              // HBASE-XXXXX-branch-2.5 feature branch
              return env.BRANCH_NAME.contains('branch-2.5')
            }
            return true
          }
        }
        environment {
          PROJECT = 'hbase'
          BASEDIR = "${WORKSPACE}/component"
          PERSONALITY_FILE = "${BASEDIR}/dev-support/hbase-personality.sh"
          TESTS_FILTER = 'checkstyle,javac,javadoc,pylint,shellcheck,shelldocs,blanks,perlcritic,ruby-lint,rubocop'
          EXCLUDE_TESTS_URL = "${JENKINS_URL}/job/HBase-Find-Flaky-Tests/job/${BRANCH_NAME}/lastSuccessfulBuild/artifact/output/excludes"
          ASF_NIGHTLIES = 'https://nightlies.apache.org'
          ASF_NIGHTLIES_BASE_ORI = "${ASF_NIGHTLIES}/hbase/${JOB_NAME}/${BUILD_NUMBER}"
          ASF_NIGHTLIES_BASE = "${ASF_NIGHTLIES_BASE_ORI.replaceAll(' ', '%20')}"
          TESTS = 'compile,htmlout,javac,maven,mvninstall,shadedjars,unit'
          SET_JAVA_HOME = "/usr/lib/jvm/java-17"
          HADOOP_PROFILE = '3.0'
          TEST_PROFILE = 'runDevTests'
          SKIP_ERRORPRONE = true
          OUTPUT_DIR_RELATIVE = "output-jdk17-hadoop3-backwards-${HADOOP3_VERSION}"
          OUTPUT_DIR = "${WORKSPACE}/${OUTPUT_DIR_RELATIVE}"
          AUTHOR_IGNORE_LIST = 'hbase-website/app/pages/_docs/docs/_mdx/(multi-page)/building-and-developing/developer-guidelines.mdx,hbase-website/public/book.html'
          BLANKS_EOL_IGNORE_FILE = 'dev-support/blanks-eol-ignore.txt'
          BLANKS_TABS_IGNORE_FILE = 'dev-support/blanks-tabs-ignore.txt'
          // output from surefire; sadly the archive function in yetus only works on file names.
          ARCHIVE_PATTERN_LIST = 'TEST-*.xml,org.apache.h*.txt,*.dumpstream,*.dump'
        }
        stages {
          stage ('run checks') {
            steps {
              sh '''#!/usr/bin/env bash
                set -e
                rm -rf "${OUTPUT_DIR}" && mkdir "${OUTPUT_DIR}"
                rm -f "${OUTPUT_DIR}/commentfile"
              '''
              unstash 'yetus'
              dir('component') {
                checkout scm
              }
              sh '''#!/usr/bin/env bash
                set -e
                rm -rf "${OUTPUT_DIR}/machine" && mkdir "${OUTPUT_DIR}/machine"
                "${BASEDIR}/dev-support/gather_machine_environment.sh" "${OUTPUT_DIR_RELATIVE}/machine"
                echo "got the following saved stats in '${OUTPUT_DIR_RELATIVE}/machine'"
                ls -lh "${OUTPUT_DIR_RELATIVE}/machine"
              '''
              script {
                def ret = sh(
                  returnStatus: true,
                  script: '''#!/usr/bin/env bash
                    set -e
                    declare -i status=0
                    if "${BASEDIR}/dev-support/hbase_nightly_yetus.sh" ; then
                      echo "(/) {color:green}+1 jdk17 hadoop ${HADOOP3_VERSION} backward compatibility checks{color}" > "${OUTPUT_DIR}/commentfile"
                    else
                      echo "(x) {color:red}-1 jdk17 hadoop ${HADOOP3_VERSION} backward compatibility checks{color}" > "${OUTPUT_DIR}/commentfile"
                      status=1
                    fi
                    echo "-- For more information [see jdk17 report|${BUILD_URL}console]" >> "${OUTPUT_DIR}/commentfile"
                    exit "${status}"
                  '''
                )
                if (ret != 0) {
                  currentBuild.result = 'UNSTABLE'
                }
              }
            }
          }
        }
        post {
          always {
            script {
              stash name: "jdk17-hadoop3-backwards-result-${HADOOP3_VERSION}", includes: "${OUTPUT_DIR_RELATIVE}/commentfile"
              junit testResults: "${env.OUTPUT_DIR_RELATIVE}/**/target/**/TEST-*.xml", allowEmptyResults: true
              // zip surefire reports.
              sh '''#!/bin/bash -e
                if [ ! -f "${OUTPUT_DIR}/commentfile" ]; then
                  echo "(x) {color:red}-1 jdk17 hadoop ${HADOOP3_VERSION} backward compatibility checks{color}" >"${OUTPUT_DIR}/commentfile"
                  echo "-- Something went wrong running this stage, please [check relevant console output|${BUILD_URL}/console]." >> "${OUTPUT_DIR}/commentfile"
                fi
                if [ -d "${OUTPUT_DIR}/archiver" ]; then
                  count=$(find "${OUTPUT_DIR}/archiver" -type f | wc -l)
                  if [[ 0 -ne ${count} ]]; then
                    echo "zipping ${count} archived files"
                    zip -q -m -r "${OUTPUT_DIR}/test_logs.zip" "${OUTPUT_DIR}/archiver"
                  else
                    echo "No archived files, skipping compressing."
                  fi
                else
                  echo "No archiver directory, skipping compressing."
                fi
              '''
              def logFile = "${env.OUTPUT_DIR_RELATIVE}/test_logs.zip"
              if (fileExists(logFile)) {
                sshPublisher(publishers: [
                  sshPublisherDesc(configName: 'Nightlies',
                    transfers: [
                      sshTransfer(remoteDirectory: "hbase/${JOB_NAME}/${BUILD_NUMBER}",
                        sourceFiles: "${env.OUTPUT_DIR_RELATIVE}/test_logs.zip"
                      )
                    ]
                  )
                ])
                sh '''#!/bin/bash -e
                  echo "Remove ${OUTPUT_DIR}/test_logs.zip for saving space"
                  rm -rf "${OUTPUT_DIR}/test_logs.zip"
                  python3 ${BASEDIR}/dev-support/gen_redirect_html.py "${ASF_NIGHTLIES_BASE}/${OUTPUT_DIR_RELATIVE}" > "${OUTPUT_DIR}/test_logs.html"
                '''
              }
              archiveArtifacts artifacts: "${env.OUTPUT_DIR_RELATIVE}/*"
              archiveArtifacts artifacts: "${env.OUTPUT_DIR_RELATIVE}/**/*"
              publishHTML target: [
                allowMissing: true,
                keepAll: true,
                alwaysLinkToLastBuild: true,
                reportDir: "${env.OUTPUT_DIR_RELATIVE}",
                reportFiles: 'console-report.html',
                reportName: "JDK17 Nightly Build Report (Hadoop ${HADOOP3_VERSION} backwards compatibility)"
              ]
            } // script
          } // always
        } // post
      } // matrix
    } // stage ('backwards compatibility checks')
  } // stages
  post {
    always {
      script {
        sh "printenv"
        // wipe out all the output directories before unstashing
        sh'''
          echo "Clean up result directories"
          rm -rf output-jdk17-hadoop3-backwards-*
        '''
        def results = []
        for (hadoopVersion in getHadoopVersions(env.HADOOP_VERSIONS)) {
          try {
            unstash "jdk17-hadoop3-backwards-result-${hadoopVersion}"
            results.add("output-jdk17-hadoop3-backwards-${hadoopVersion}/commentfile")
          } catch (e) {
            echo "unstash ${hadoopVersion} failed, ignore"
          }
        }
        try {
          def comment = "Results for branch ${env.BRANCH_NAME}\n"
          comment += "\t[build ${currentBuild.displayName} on builds.a.o|${env.BUILD_URL}]: "
          if (currentBuild.result == null || currentBuild.result == "SUCCESS") {
            comment += "(/) *{color:green}+1 overall{color}*\n"
          } else {
            comment += "(x) *{color:red}-1 overall{color}*\n"
          }
          comment += "----\n"
          comment += "Backwards compatibility checks:\n"
          comment += results.collect { fileExists(file: it) ? readFile(file: it) : "" }.join("\n\n")

          echo "[INFO] Comment:"
          echo comment

          def jiras = getJirasToComment(env.BRANCH_NAME, [])
          if (jiras.isEmpty()) {
            echo "[DEBUG] non-feature branch, checking change messages for jira keys."
            jiras = getJirasToCommentFromChangesets(currentBuild)
          }
          jiras.each { currentIssue ->
            jiraComment issueKey: currentIssue, body: comment
          }
        } catch (Exception exception) {
          echo "Got exception: ${exception}"
          echo "    ${exception.getStackTrace()}"
        }
      }
    }
  }
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
