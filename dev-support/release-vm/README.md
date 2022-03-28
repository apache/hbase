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

# HBase Release Env

This is a vagrant project that provides a virtual machine environment suitable
for running an Apache HBase release.

Requires:
* [VirtualBox](http://virtualbox.org)
* [Vagrant](http://virtualbox.org)
* The private portion of your signing key avilable in the local GPG agent
* The private portion of your Github authentication key available in either the local GPG agent or
  local SSH agent

## Usage

Unlock the local keyring before proceeding (this should prompt you for your GPG passphrase). For
example, assuming you have an authentication key configured in your keyring, this will do the
trick.

All terminal commands used below are assumed to be run with the current working directory as the
location containing the `Vagrantfile`.

The term "Host" is used to mean the environment that runs the Vagrant process. The term "Guest" is
used to mean the virtual machine managed by the Host.

### Ensure credentials work from the Host OS

The ssh- and gpg-agent forwarding configuration used here assumes that your credentials work
on the Host. Verify both are working before you proceed with the Guest. Additionally, using the
credentials requires you to unlock the respective keyring, the state of which is persisted by the
agent process or processes running on the Host.

See instructions in [`create-release`](../create-release/README.txt) regarding proper
configuration of ssh- and gpg-agents.

Assuming the git repo origin is on GitHub, the following command will ensure that your ssh
credentials are working. On the Host, run:

```sh
host:~$ ssh -T git@github.com
Hi <you>! You've successfully authenticated, but GitHub does not provide shell access.
```

Likewise, ensure you have an encryption key that can be used to sign a file. Again, on the Host,
run:

```sh
host:~$ gpg --detach --armor --sign Vagrantfile
host:~$ gpg --verify Vagrantfile.asc
gpg: assuming signed data in 'Vagrantfile'
...
host:~$ rm Vagrantfile.asc
```

### Make public keyring available to the VM

Export the public portion of your signing credentials where the Guest can access it. Vagrant
(+VirtualBox) shares the directory of the `Vagrantfile` with the Linux Guest via the `/vagrant`
mount point. Any files present in this working directory on the Host are available to the Guest.

From the Host, run:

```sh
host:~$ gpg --export <you>@apache.org > gpg.<you>.apache.pub
```

### Launch the Guest VM

Launch the Guest VM by running

```sh
host:~$ vagrant up
```

If anything about the Vagrant or VirtualBox environment have changed since you last used this VM,
it's best to `vagrant destroy -f` all local state and `vagrant up` a fresh instance.

### Verify the Guest VM

Connect to the Guest. This should forward your ssh- and gpg-agent session, as configured in the
`Vagrantfile`.

```sh
host:~$ vagrant ssh
```

Now that you're in the Guest VM, be sure that all `gpg` command you issue include the
`--no-autostart` flag. This ensures that the `gpg` process in the Guest communicates with the
agent running on the Host OS rather than launching its own process on the Guest OS.

From the Guest, verify that ssh-agent forwarding is working, using the same test performed on the
Host,

```sh
guest:~$ ssh -T git@github.com
Hi <you>! You've successfully authenticated, but GitHub does not provide shell access.
```

From the Guest, import your exported public identity and verify the gpg-agent passthrough is
working correctly.

```sh
guest:~$ gpg --no-autostart --import /vagrant/gpg.<you>.apache.pub
...
gpg: Total number processed: 1
gpg:               imported: 1
guest:~$ gpg --no-autostart --detach --armor --sign repos/hbase/pom.xml
guest:~$ gpg --no-autostart --verify repos/hbase/pom.xml.asc
gpg: assuming signed data in 'repos/hbase/pom.xml'
...
guest:~$ rm repos/hbase/pom.xml.asc
```

### Build a Release Candidate

Finally, you can initiate the release build. Follow the instructions in
[`create-release`](../create-release/README.txt), i.e.,

```sh
guest:~$ mkdir ~/build-2.3.1-rc0
guest:~$ cd repos/hbase
guest:~/repos/hbase$ ./dev-support/create-release/do-release-docker.sh -d ~/build-2.3.1-rc0/ ...
```
