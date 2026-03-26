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
    pollSCM('@daily')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
    timeout (time: 1, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
    booleanParam(name: 'FORCE_FAIL', defaultValue: false, description: 'force a failure to test notifications.')
  }
  stages {
    stage ('build hbase website') {
      steps {
        dir('component') {
          checkout scm
        }
        sh '''#!/bin/bash -e
          if [ "${DEBUG}" = "true" ]; then
            set -x
          fi
          if [ "${FORCE_FAIL}" = "true" ]; then
            false
          fi
          user=$(whoami)
          docker build -t hbase-build-website -f "${WORKSPACE}/component/dev-support/docker/Dockerfile" .
          docker run --rm -v "${WORKSPACE}":/home/${user} -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
            -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-17" -e GIT_AUTHOR_NAME="HBase" \
            -e GIT_AUTHOR_EMAIL="dev@hbase.apache.org" -e GIT_COMMITTER_NAME="HBase" \
            -e GIT_COMMITTER_EMAIL="dev@hbase.apache.org" --workdir=/home/${user} hbase-build-website \
            "component/dev-support/generate-website/build-hbase-website.sh" \
            --working-dir /home/${user} component
        '''
        script {
          if (fileExists('website.patch.zip')) {
            sh'''#!/bin/bash -e
              patch=$(ls -1 *.patch | head -n 1)
              echo "Has patch ${patch}, stash and then publish"
            '''
            stash name: 'patch', includes: "*.patch"
            env.PUBLISH_WEBSITE = "true"
          } else {
            echo "No patch file, skip stashing and publishing"
            env.PUBLISH_WEBSITE = "false"
          }
        }
      }
    }
    stage('publish hbase website') {
      agent {
        node {
          label 'git-websites'
        }
      }
      when {
        expression {
          return env.PUBLISH_WEBSITE == 'true'
        }
      }
      steps {
        sh '''#!/bin/bash -e
          # wipe out stall repo and files
          rm -rf *.patch
          rm -rf hbase-site
        '''
        unstash 'patch'
        sh '''#!/bin/bash -e
          git clone --depth 1 --branch asf-site https://gitbox.apache.org/repos/asf/hbase-site.git
          patch=$(ls -1 *.patch | head -n 1)
          cd hbase-site;
          echo "applying ${patch}"
          git am ../${patch}
          echo "Publishing changes to remote repo..."
          if git push origin asf-site; then
            echo "changes pushed."
          else
            echo "Failed to push to asf-site. Website not updated."
            exit 1
          fi
          echo "Sending empty commit to work around INFRA-10751."
          git commit --allow-empty -m "INFRA-10751 Empty commit"
          # Push the empty commit
          if git push origin asf-site; then
            echo "empty commit pushed."
          else
            echo "Failed to push the empty commit to asf-site. Website may not update. Manually push an empty commit to fix this. (See INFRA-10751)"
            exit 1
          fi
          echo "Pushed the changes to branch asf-site. Refresh http://hbase.apache.org/ to see the changes within a few minutes."
        '''
      }
    }
  }
  post {
    always {
      // Has to be relative to WORKSPACE.
      archiveArtifacts artifacts: '*.patch.zip,hbase-*.txt'
    }
    failure {
      mail to: 'dev@hbase.apache.org', replyTo: 'dev@hbase.apache.org', subject: "Failure: HBase Generate Website", body: """
Build status: ${currentBuild.currentResult}

The HBase website has not been updated to incorporate recent HBase changes.

See ${env.BUILD_URL}console
"""
    }
    cleanup {
      deleteDir()
    }
  }
}
