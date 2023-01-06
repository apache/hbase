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
#%define webapps_hbase %{hbase_home}/hbase-webapps
%define man_dir %{_mandir}
%define hbase_username hbase
%define hbase_services master regionserver thrift thrift2 rest
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

Name: hbase
Version: %{hbase_version}
Release: %{release}
BuildArch: noarch
Summary: HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data. This project's goal is the hosting of very large tables -- billions of rows X millions of columns -- atop clusters of commodity hardware. 
URL: http://hbase.apache.org/
Group: Systems/Daemons
Buildroot: %{_topdir}/INSTALL/hbase-%{version}
License: ASL 2.0
Source0: %{input_tar}
Source1: install_hbase.sh
Source2: hbase.svc
Source3: init.d.tmpl
Source4: hbase.default
Source5: hbase.nofiles.conf
Source6: regionserver-init.d.tpl

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

%package master
Summary: The Hadoop HBase master Server.
Group: System/Daemons
Requires: %{name} >= %{hbase_major_version}
Requires(pre): %{name} >= %{hbase_major_version}
# Required for init scripts
Requires: /lib/lsb/init-functions

%description master
HMaster is the "coordinator server" for a HBase. There is only one HMaster for a single HBase deployment.

%package regionserver
Summary: The Hadoop HBase RegionServer server.
Group: System/Daemons
Requires: %{name} >= %{hbase_major_version}
Requires(pre): %{name} >= %{hbase_major_version}
Requires: /lib/lsb/init-functions

%description regionserver
HRegionServer makes a set of HRegions available to clients. It checks in with the HMaster. There are many HRegionServers in a single HBase deployment.

%package thrift
Summary: The Hadoop HBase Thrift Interface
Group: System/Daemons
Requires: %{name} >= %{hbase_major_version}
Requires(pre): %{name} >= %{hbase_major_version}
# Required for init scripts
Requires: /lib/lsb/init-functions

%description thrift
ThriftServer - this class starts up a Thrift server which implements the Hbase API specified in the Hbase.thrift IDL file.
"Thrift is a software framework for scalable cross-language services development. It combines a powerful software stack with a code generation engine to build services that work efficiently and seamlessly between C++, Java, Python, PHP, and Ruby. Thrift was developed at Facebook, and we are now releasing it as open source." For additional information, see http://developers.facebook.com/thrift/. Facebook has announced their intent to migrate Thrift into Apache Incubator.

%package thrift2
Summary: The Hadoop HBase Thrift2 Interface
Group: System/Daemons
Requires: %{name} >= %{hbase_major_version}
Requires(pre): %{name} >= %{hbase_major_version}
# Required for init scripts
Requires: /lib/lsb/init-functions

%description thrift2
Thrift2 Server to supersede original Thrift Server.
Still under development. https://issues.apache.org/jira/browse/HBASE-8818

%package rest
Summary: The Apache HBase REST gateway
Group: System/Daemons
Requires: %{name} >= %{hbase_major_version}
Requires(pre): %{name} >= %{hbase_major_version}
# Required for init scripts
Requires: /lib/lsb/init-functions

%description rest
The Apache HBase REST gateway

%prep
%setup -n hbase-%{hbase_version}

%install
%__rm -rf $RPM_BUILD_ROOT
bash %{SOURCE1} \
	--mvn-target-dir=%{mvn_target_dir} \
    --doc-dir=%{doc_hbase} \
    --conf-dir=%{etc_hbase_conf_dist} \
	--prefix=$RPM_BUILD_ROOT

%__install -d -m 0755 $RPM_BUILD_ROOT/%{initd_dir}/

%__install -d -m 0755 $RPM_BUILD_ROOT/etc/default/
%__install -m 0644 %{SOURCE4} $RPM_BUILD_ROOT/etc/default/hbase

%__install -d -m 0755 $RPM_BUILD_ROOT/etc/security/limits.d
%__install -m 0644 %{SOURCE5} $RPM_BUILD_ROOT/etc/security/limits.d/hbase.nofiles.conf

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/log/hbase
ln -s %{_localstatedir}/log/hbase %{buildroot}/%{logs_hbase}

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/run/hbase
ln -s %{_localstatedir}/run/hbase %{buildroot}/%{pids_hbase}

%__install -d  -m 0755  %{buildroot}/%{_localstatedir}/lib/hbase

for service in %{hbase_services}
do
    init_file=$RPM_BUILD_ROOT/%{initd_dir}/hbase-${service}
    if [[ "$service" = "regionserver" ]] ; then
        # Region servers start from a different template that allows
        # them to run multiple concurrent instances of the daemon
        %__cp %{SOURCE6} $init_file
        %__sed -i -e "s|@INIT_DEFAULT_START@|3 4 5|" $init_file
        %__sed -i -e "s|@INIT_DEFAULT_STOP@|0 1 2 6|" $init_file
        %__sed -i -e "s|@CHKCONFIG@|345 87 13|" $init_file
        %__sed -i -e "s|@HBASE_DAEMON@|${service}|" $init_file
    else
        %__sed -e "s|@HBASE_DAEMON@|${service}|" %{SOURCE2} > ${RPM_SOURCE_DIR}/hbase-${service}.node
        bash %{SOURCE3} ${RPM_SOURCE_DIR}/hbase-${service}.node rpm $init_file
    fi

    chmod 755 $init_file
done

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
%config(noreplace) %{_sysconfdir}/default/hbase
%config(noreplace) /etc/security/limits.d/hbase.nofiles.conf
%{hbase_home}
%{hbase_home}/hbase-*.jar
#%{webapps_hbase}
/usr/bin/hbase
%config(noreplace) %{etc_hbase_conf_dist}

# files from doc package
%defattr(-,root,root)
%doc %{doc_hbase}/

%define service_macro() \
%files %1 \
%attr(0755,root,root)/%{initd_dir}/%{name}-%1 \
%post %1 \
chkconfig --add %{name}-%1

%service_macro master
%service_macro thrift
%service_macro thrift2
%service_macro regionserver
%service_macro rest
