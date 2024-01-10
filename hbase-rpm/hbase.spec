# taken from hbase.spec in https://github.com/apache/bigtop/
# greatly modified to simplify and fix dependencies to work in the hubspot environment

%define hadoop_major_version 3.2
%define hbase_major_version 2.4
%define etc_hbase_conf %{_sysconfdir}/hbase/conf
%define etc_hbase_conf_dist %{etc_hbase_conf}.dist
%define hbase_home /usr/lib/hbase
%define bin_hbase %{hbase_home}/bin
%define lib_hbase %{hbase_home}/lib
%define conf_hbase %{hbase_home}/conf
%define logs_hbase %{hbase_home}/logs
%define pids_hbase %{hbase_home}/pids
%define man_dir %{_mandir}
%define hbase_username hbase
%define hadoop_home /usr/lib/hadoop
%define zookeeper_home /usr/lib/zookeeper

# FIXME: brp-repack-jars uses unzip to expand jar files
# Unfortunately guice-2.0.jar pulled by ivy contains some files and directories without any read permission
# and make whole process to fail.
# So for now brp-repack-jars is being deactivated until this is fixed.
# See BIGTOP-294
%define __os_install_post \
    %{_rpmconfigdir}/brp-compress ; \
    %{_rpmconfigdir}/brp-strip-static-archive %{__strip} ; \
    %{_rpmconfigdir}/brp-strip-comment-note %{__strip} %{__objdump} ; \
    /usr/lib/rpm/brp-python-bytecompile ; \
    %{nil}

%define doc_hbase %{_docdir}/hbase-%{hbase_version}
%global initd_dir %{_sysconfdir}/rc.d/init.d
%define alternatives_cmd alternatives

# Disable debuginfo package
%define debug_package %{nil}

# HubSpot: use zstd because it decompresses much faster
%define _binary_payload w19.zstdio
%define _source_payload w19.zstdio

Name: hbase
Version: %{hbase_version}
Release: %{release}
BuildArch: noarch
Summary: HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data. This project's goal is the hosting of very large tables -- billions of rows X millions of columns -- atop clusters of commodity hardware.
URL: http://hbase.apache.org/
Group: Systems/Daemons
Buildroot: %{_topdir}/INSTALL/hbase-%{maven_version}
License: ASL 2.0
Source0: %{input_tar}
Source1: install_hbase.sh

Requires: coreutils, /usr/sbin/useradd, /sbin/chkconfig, /sbin/service
Requires: hadoop >= %{hadoop_major_version}

AutoReq: no

%description
HBase is an open-source, distributed, column-oriented store modeled after Google' Bigtable: A Distributed Storage System for Structured Data by Chang et al. Just as Bigtable leverages the distributed data storage provided by the Google File System, HBase provides Bigtable-like capabilities on top of Hadoop. HBase includes:

    * Convenient base classes for backing Hadoop MapReduce jobs with HBase tables
    * Query predicate push down via server side scan and get filters
    * Optimizations for real time queries
    * A high performance Thrift gateway
    * A REST-ful Web service gateway that supports XML, Protobuf, and binary data encoding options
    * Cascading source and sink modules
    * Extensible jruby-based (JIRB) shell
    * Support for exporting metrics via the Hadoop metrics subsystem to files or Ganglia; or via JMX

%prep
%setup -n hbase-%{maven_version}

%install
%__rm -rf $RPM_BUILD_ROOT
bash %{SOURCE1} \
	--input-tar=%{SOURCE0} \
    --doc-dir=%{doc_hbase} \
    --conf-dir=%{etc_hbase_conf_dist} \
	--prefix=$RPM_BUILD_ROOT

%__install -d -m 0755 $RPM_BUILD_ROOT/%{initd_dir}/

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/log/hbase
ln -s %{_localstatedir}/log/hbase %{buildroot}/%{logs_hbase}

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/run/hbase
ln -s %{_localstatedir}/run/hbase %{buildroot}/%{pids_hbase}

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/lib/hbase

%__install -d -m 0755 $RPM_BUILD_ROOT/usr/bin

# Pull hadoop from its packages
rm -f $RPM_BUILD_ROOT/%{lib_hbase}/{hadoop,slf4j-log4j12-}*.jar

ln -f -s %{hadoop_home}/client/hadoop-annotations.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-auth.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-common.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-hdfs-client.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-mapreduce-client-common.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-mapreduce-client-core.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-mapreduce-client-jobclient.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-yarn-api.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-yarn-client.jar $RPM_BUILD_ROOT/%{lib_hbase}
ln -f -s %{hadoop_home}/client/hadoop-yarn-common.jar $RPM_BUILD_ROOT/%{lib_hbase}

%pre
getent group hbase 2>/dev/null >/dev/null || /usr/sbin/groupadd -r hbase
getent passwd hbase 2>&1 > /dev/null || /usr/sbin/useradd -c "HBase" -s /sbin/nologin -g hbase -r -d /var/lib/hbase hbase 2> /dev/null || :

%post
%{alternatives_cmd} --install %{etc_hbase_conf} %{name}-conf %{etc_hbase_conf_dist} 30

%files
%defattr(-,hbase,hbase)
%{logs_hbase}
%{pids_hbase}
%dir %{_localstatedir}/log/hbase
%dir %{_localstatedir}/run/hbase
%dir %{_localstatedir}/lib/hbase

%defattr(-,root,root)
%{hbase_home}
%{hbase_home}/hbase-*.jar
/usr/bin/hbase
%config(noreplace) %{etc_hbase_conf_dist}

# files from doc package
%defattr(-,root,root)
%doc %{doc_hbase}/
