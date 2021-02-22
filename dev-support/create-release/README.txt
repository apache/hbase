Creates an HBase release candidate.

The scripts in this directory came originally from spark
(https://github.com/apache/spark/tree/master/dev/create-release). They were
then modified to suit the hbase context. These scripts supercede the old
_../make_rc.sh_ script for making release candidates because what is here is
more comprehensive doing more steps of the RM process as well as running in a
container so the RM build environment can be a constant.

It:

 * Tags release
 * Sets version to the release version
 * Sets version to next SNAPSHOT version.
 * Builds, signs, and hashes all artifacts.
 * Pushes release tgzs to the dev dir in a apache dist.
 * Pushes to repository.apache.org staging.

The entry point is the do-release-docker.sh script. It requires a local
docker; for example, on mac os x, a Docker for Desktop installed and running.

(To run a build w/o invoking docker (not recommended!), use _do_release.sh_.)

The scripts will query interactively for needed parameters and passphrases.
For explanation of the parameters, execute:

 $ release-build.sh --help

The scripts run in dry-run mode by default where only local builds are
performed and nothing is uploaded to the ASF repos. Pass the '-f' flag
to remove dry-run mode.

Before starting the RC build, run a reconciliation of what is in JIRA with
what is in the commit log. Make sure they align and that anomalies are
explained up in JIRA. See http://hbase.apache.org/book.html#maven.release
for how.

Regardless of where your release build will run (locally, locally in docker,
on a remote machine, etc) you will need a local gpg-agent with access to your
secret keys. Before starting an RC build, make sure your local gpg-agent has
configs to properly handle your credentials, especially if you want to avoid
typing the passphrase to your secret key: e.g. if you are going to run
and step away (the RC creation takes ~5 hours), best to increase the TTL on
caching the unlocked secret by setting the following into local your
~/.gnupg/gpg-agent.conf file:
  # in seconds, e.g. a day
  default-cache-ttl 86400
  max-cache-ttl 86400

A quick way to tell gpg to clear out state, re-read the gpg-agent.conf file
and start a new gpg-agent is via the following command phrase:

 $ gpgconf --kill all && gpg-connect-agent /bye

You can verify options took hold with '$ gpg --list-options gpg-agent'.

Similarly, run ssh-agent with your ssh key added if building with docker.

Running a build on GCE is easy enough. Here are some notes if of use.
Create an instance. 4CPU/15G/20G disk seems to work well enough.
Once up, run the below to make your machine fit for RC building:

# Presuming debian-compatible OS, do these steps on the VM.
# Your VM username should be your ASF id, because it will show up in build artifacts.
# Follow the docker install guide: https://docs.docker.com/engine/install/debian/
$ sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common
$ curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
$ sudo add-apt-repository -y \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   stable"
$ sudo apt-get update
$ sudo apt-get install -y docker-ce docker-ce-cli containerd.io
# Follow the post installation steps: https://docs.docker.com/engine/install/linux-postinstall/
$ sudo usermod -aG docker $USER
# LOGOUT and then LOGIN again so $USERID shows as part of docker group
# Test here by running docker's hello world as your build user
$ docker run hello-world

# Follow the GPG guide for forwarding your gpg-agent from your local machine to the VM
#   https://wiki.gnupg.org/AgentForwarding
# On the VM find out the location of the gpg agent socket and extra socket
$ gpgconf --list-dir agent-socket
/run/user/1000/gnupg/S.gpg-agent
$ gpgconf --list-dir agent-extra-socket
/run/user/1000/gnupg/S.gpg-agent.extra
# On the VM configure sshd to remove stale sockets
$ sudo bash -c 'echo "StreamLocalBindUnlink yes" >> /etc/ssh/sshd_config'
$ sudo systemctl restart ssh
# logout of the VM

# Do these steps on your local machine.
# make sure gpg-agent is running
$ gpg-connect-agent /bye
# Export your public key and copy it to the VM.
# Assuming 'example.gce.host' maps to your VM's external IP (or use the IP)
$ gpg --export example@apache.org > ~/gpg.example.apache.pub
$ scp ~/gpg.example.apache.pub example.gce.host:
# ssh into the VM while forwarding the remote gpg socket locations found above to your local
#   gpg-agent's extra socket (this will restrict what commands the remote node is allowed to have
#   your agent handle. Note that the gpg guide above can help you set this up in your ssh config
#   rather than typing it in ssh like this every time.
$ ssh -i ~/.ssh/my_id \
    -R "/run/user/1000/gnupg/S.gpg-agent:$(gpgconf --list-dir agent-extra-socket)" \
    -R "/run/user/1000/gnupg/S.gpg-agent.extra:$(gpgconf --list-dir agent-extra-socket)" \
    example.gce.host

# now in an SSH session on the VM with the socket forwarding
# import your public key and test signing with the forwarding to your local agent.
$ gpg --no-autostart --import gpg.example.apache.pub
$ echo "foo" > foo.txt
$ gpg --no-autostart --detach --armor --sign foo.txt
$ gpg --no-autostart --verify foo.txt.asc

# install git and clone the main project on the build machine
$ sudo apt-get install -y git
$ git clone https://github.com/apache/hbase.git
# finally set up an output folder and launch a dry run.
$ mkdir ~/build
$ cd hbase
$ ./dev-support/create-release/do-release-docker.sh -d ~/build
