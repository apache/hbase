Entrance script is _do-release-docker.sh_. Requires a local docker;
for example, on mac os x, Docker for Desktop installed and running.

Before starting the RC build, run a reconciliation of what is in
JIRA with what is in the commit log. Make sure they align and that
anomalies are explained up in JIRA.

See http://hbase.apache.org/book.html#maven.release

Running a build on GCE is easy enough. Here are some notes if of use.
Create an instance. 4CPU/15G/10G disk seems to work well enough.
Once up, run the below to make your machine fit for RC building:


# Presuming debian-compatible OS
$ sudo apt-get install -y git openjdk-8-jdk maven gnupg gnupg-agent
# Install docker
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
$ sudo usermod -a -G docker $USERID
# LOGOUT and then LOGIN again so $USERID shows as part of docker groupl
# Copy up private key for $USERID export from laptop and import on gce.
$ gpg --import stack.duboce.net.asc
$ export GPG_TTY=$(tty) # https://github.com/keybase/keybase-issues/issues/2798
$ eval $(gpg-agent --disable-scdaemon --daemon --no-grab  --allow-preset-passphrase --default-cache-ttl=86400 --max-cache-ttl=86400)
$ git clone https://github.com/apache/hbase.git
$ cd hbase
$ mkdir ~/build
$ ./dev-resources/create-release/do-release-docker.sh -d ~/build
# etc.
