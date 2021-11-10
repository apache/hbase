#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Image for use on Mac boxes to get a gpg agent socket available
# within transient release building ocntainers.
#
# build like:
#
# docker build --build-arg "UID=$UID" --build-arg "RM_USER=$USER" \
#     --tag org.apache.hbase/gpg-agent-proxy mac-sshd-gpg-agent
#
# run like:
#
# docker run --rm -p 62222:22 \
#     --mount "type=bind,src=${HOME}/.ssh/id_rsa.pub,dst=/home/${USER}/.ssh/authorized_keys,readonly" \
#     --mount "type=volume,src=gpgagent,dst=/home/${USER}/.gnupg/" \
#     org.apache.hbase/gpg-agent-proxy:latest
#
# test like:
#
# ssh -p 62222 -R "/home/${USER}/.gnupg/S.gpg-agent:$(gpgconf --list-dir agent-extra-socket)" \
#     -i "${HOME}/.ssh/id_rsa" -N -n localhost
#
# launch a docker container to do work that shares the mount for the gpg agent
# expressly does not need to be this same image, but needs to have defined the same user
#
# docker run --rm -it \
#     --mount "type=volume,src=gpgagent,dst=/home/${USER}/.gnupg/" \
#     --mount "type=bind,src=${HOME}/projects/hbase-releases/KEYS,dst=/home/${USER}/KEYS,readonly" \
#     --entrypoint /bin/bash --user "${USER}" --workdir "/home/${USER}/" \
#     org.apache.hbase/gpg-agent-proxy:latest
#
#
# Make sure to import the public keys
#
# gpg --no-autostart --import < ${HOME}/KEYS
# Optional?
# gpg --no-autostart --edit-key ${YOUR_KEY}
# trust
# 5
# y
# quit
#
# echo "foo" > foo
# gpg --no-autostart --armor --detach --sign foo
# gpg --no-autostart --verify foo.asc
#
# For more info see
# * gpg forwarding over ssh: https://wiki.gnupg.org/AgentForwarding
# * example docker for sshd: https://github.com/hotblac/nginx-ssh
# * why we have to bother with this: https://github.com/docker/for-mac/issues/483
#
# If the docker image changes then the host key used by sshd will change and you will get a
# nastygram when launching ssh about host identification changing. This is expected. you should
# remove the previous host key.
#
# Tested with
# * Docker Desktop 2.2.0.5
# * gpg 2.2.20
# * pinentry-mac 0.9.4
# * yubikey 5
#
FROM ubuntu:18.04

# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
#
# We only need gnupg2 here if we want the ability to test out the gpg-agent forwarding by sshing
# into the container rather than launching a new docker container.
RUN DEBIAN_FRONTEND=noninteractive apt-get -qq -y update \
  && DEBIAN_FRONTEND=noninteractive apt-get -qq -y install --no-install-recommends \
    openssh-server=1:7.6p1-* \
    gnupg2=2.2.4-* \
  && mkdir /run/sshd \
  && echo "StreamLocalBindUnlink yes" >> /etc/ssh/sshd_config \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
EXPOSE 22
# Set up our ssh user
ARG UID
ARG RM_USER
RUN groupadd sshgroup && \
    useradd --create-home --shell /bin/bash --groups sshgroup --uid $UID $RM_USER && \
    mkdir /home/$RM_USER/.ssh /home/$RM_USER/.gnupg && \
    chown -R $RM_USER:sshgroup /home/$RM_USER/ && \
    chmod -R 700 /home/$RM_USER/
# When we run we run sshd
ENTRYPOINT ["/usr/sbin/sshd", "-D"]
