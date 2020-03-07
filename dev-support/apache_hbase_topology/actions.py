#!/usr/bin/env python
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

"""The actions module for the apache_hbase topology. The behavior to be carried out by the
build_cluster and start_cluster clusterdock scripts are to be defined through the build and
start functions, respectively.
"""

import logging
import tarfile
from ConfigParser import ConfigParser
from os import EX_OK, listdir, makedirs, remove # pylint: disable=ungrouped-imports
                                                # Follow convention of grouping from module imports
                                                # after normal imports.
from os.path import exists, join
from shutil import move
from socket import getfqdn
from sys import stdout
from uuid import uuid4

# pylint: disable=import-error
# clusterdock topologies get access to the clusterdock package at run time, but their reference
# to clusterdock modules will confuse pylint, so we have to disable it.

import requests
from docker import Client

from clusterdock import Constants
from clusterdock.cluster import Cluster, Node, NodeGroup
from clusterdock.docker_utils import (build_image, get_clusterdock_container_id,
                                      get_host_port_binding, is_image_available_locally, pull_image)
from clusterdock.utils import strip_components_from_tar, XmlConfiguration

# We disable a couple of Pylint conventions because it assumes that module level variables must be
# named as if they're constants (which isn't the case here).
logger = logging.getLogger(__name__) # pylint: disable=invalid-name
logger.setLevel(logging.INFO)

client = Client() # pylint: disable=invalid-name

DEFAULT_APACHE_NAMESPACE = Constants.DEFAULT.apache_namespace # pylint: disable=no-member

def _copy_container_folder_to_host(container_id, source_folder, destination_folder,
                                   host_folder=None):
    if not exists(destination_folder):
        makedirs(destination_folder)
    stream, _ = client.get_archive(container_id, source_folder)
    tar_filename = join(destination_folder, 'container_folder.tar')
    with open(tar_filename, 'wb') as file_descriptor:
        file_descriptor.write(stream.read())
    tar = tarfile.open(name=tar_filename)
    tar.extractall(path=destination_folder, members=strip_components_from_tar(tar))
    tar.close()
    remove(tar_filename)
    logger.info("Extracted container folder %s to %s.", source_folder,
                host_folder if host_folder else destination_folder)

def _create_configs_from_file(filename, cluster_config_dir, wildcards):
    configurations = ConfigParser(allow_no_value=True)
    configurations.read(filename)

    for config_file in configurations.sections():
        logger.info("Updating %s...", config_file)
        # For XML configuration files, run things through XmlConfiguration.
        if config_file.endswith('.xml'):
            XmlConfiguration(
                {item[0]: item[1].format(**wildcards)
                 for item in configurations.items(config_file)}
            ).write_to_file(join(cluster_config_dir, config_file))
        # For everything else, recognize whether a line in the configuration should simply be
        # appended to the bottom of a file or processed in some way. The presence of +++ will
        # lead to the evaluation of the following string through the end of the line.
        else:
            lines = []
            for item in configurations.items(config_file):
                if item[0].startswith('+++'):
                    command = item[0].lstrip('+ ').format(**wildcards)

                    # Yes, we use eval here. This is potentially dangerous, but intention.
                    lines.append(str(eval(command))) # pylint: disable=eval-used
                elif item[0] == "body":
                    lines.append(item[1].format(**wildcards))
                else:
                    lines.append(item[0].format(**wildcards))
            with open(join(cluster_config_dir, config_file), 'w') as conf:
                conf.write("".join(["{0}\n".format(line) for line in lines]))

# Keep track of some common web UI ports that we'll expose to users later (e.g. to allow a user
# to reach the HDFS NameNode web UI over the internet).
HBASE_REST_SERVER_PORT = 8080
NAMENODE_WEB_UI_PORT = 50070
RESOURCEMANAGER_WEB_UI_PORT = 8088

# When starting or building cluster, CLUSTERDOCK_VOLUME will be the root directory for persistent
# files (note that this location will itself be in a Docker container's filesystem).
CLUSTERDOCK_VOLUME = '/tmp/clusterdock'

def start(args):
    """This function will be executed when ./bin/start_cluster apache_hbase is invoked."""

    # pylint: disable=too-many-locals
    # Pylint doesn't want more than 15 local variables in a function; this one has 17. This is about
    # as low as I want to go because, while I can cheat and stuff unrelated things in a dictionary,
    # that won't improve readability.

    uuid = str(uuid4())
    container_cluster_config_dir = join(CLUSTERDOCK_VOLUME, uuid, 'config')
    makedirs(container_cluster_config_dir)

    for mount in client.inspect_container(get_clusterdock_container_id())['Mounts']:
        if mount['Destination'] == CLUSTERDOCK_VOLUME:
            host_cluster_config_dir = join(mount['Source'], uuid, 'config')
            break
    else:
        raise Exception("Could not find source of {0} mount.".format(CLUSTERDOCK_VOLUME))

    # CLUSTERDOCK_VOLUME/uuid/config in the clusterdock container corresponds to
    # host_cluster_config_dir on the Docker host.
    logger.debug("Creating directory for cluster configuration files in %s...",
                 host_cluster_config_dir)

    # Generate the image name to use from the command line arguments passed in.
    image = '/'.join(
        [item
         for item in [args.registry_url, args.namespace or DEFAULT_APACHE_NAMESPACE,
                      "clusterdock:{os}_java-{java}_hadoop-{hadoop}_hbase-{hbase}".format(
                          os=args.operating_system, java=args.java_version,
                          hadoop=args.hadoop_version, hbase=args.hbase_version
                      )]
         if item]
    )
    if args.always_pull or not is_image_available_locally(image):
        pull_image(image)

    # Before starting the cluster, we create a throwaway container from which we copy
    # configuration files back to the host. We also use this container to run an HBase
    # command that returns the port of the HBase master web UI. Since we aren't running init here,
    # we also have to manually pass in JAVA_HOME as an environmental variable.
    get_hbase_web_ui_port_command = ('/hbase/bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool '
                                     'hbase.master.info.port')
    container_id = client.create_container(image=image, command=get_hbase_web_ui_port_command,
                                           environment={'JAVA_HOME': '/java'})['Id']
    logger.debug("Created temporary container (id: %s) from which to copy configuration files.",
                 container_id)

    # Actually do the copying of Hadoop configs...
    _copy_container_folder_to_host(container_id, '/hadoop/etc/hadoop',
                                   join(container_cluster_config_dir, 'hadoop'),
                                   join(host_cluster_config_dir, 'hadoop'))

    # ... and repeat for HBase configs.
    _copy_container_folder_to_host(container_id, '/hbase/conf',
                                   join(container_cluster_config_dir, 'hbase'),
                                   join(host_cluster_config_dir, 'hbase'))

    logger.info("The /hbase/lib folder on containers in the cluster will be volume mounted "
                "into %s...", join(host_cluster_config_dir, 'hbase-lib'))
    _copy_container_folder_to_host(container_id, '/hbase/lib',
                                   join(container_cluster_config_dir, 'hbase-lib'),
                                   join(host_cluster_config_dir, 'hbase-lib'))

    # Every node in the cluster will have a shared volume mount from the host for Hadoop and HBase
    # configuration files as well as the HBase lib folder.
    shared_volumes = [{join(host_cluster_config_dir, 'hadoop'): '/hadoop/etc/hadoop'},
                      {join(host_cluster_config_dir, 'hbase'): '/hbase/conf'},
                      {join(host_cluster_config_dir, 'hbase-lib'): '/hbase/lib'}]

    # Get the HBase master web UI port, stripping the newline the Docker REST API gives us.
    client.start(container=container_id)
    if client.wait(container=container_id) == EX_OK:
        hbase_master_web_ui_port = client.logs(container=container_id).rstrip()
        client.remove_container(container=container_id, force=True)
    else:
        raise Exception('Failed to remove HBase configuration container.')

    # Create the Node objects. These hold the state of our container nodes and will be started
    # at Cluster instantiation time.
    primary_node = Node(hostname=args.primary_node[0], network=args.network,
                        image=image, ports=[NAMENODE_WEB_UI_PORT,
                                            hbase_master_web_ui_port,
                                            RESOURCEMANAGER_WEB_UI_PORT,
                                            HBASE_REST_SERVER_PORT],
                        volumes=shared_volumes)
    secondary_nodes = []
    for hostname in args.secondary_nodes:
        # A list of service directories will be used to name folders on the host and, appended
        # with an index, in the container, as well (e.g. /data1/node-1/dfs:/dfs1).
        service_directories = ['dfs', 'yarn']

        # Every Node will have shared_volumes to let one set of configs on the host be propagated
        # to every container. If --data-directories is specified, this will be appended to allow
        # containers to use multiple disks on the host.
        volumes = shared_volumes[:]
        if args.data_directories:
            data_directories = args.data_directories.split(',')
            volumes += [{join(data_directory, uuid, hostname, service_directory):
                             "/{0}{1}".format(service_directory, i)}
                        for i, data_directory in enumerate(data_directories, start=1)
                        for service_directory in service_directories]
        secondary_nodes.append(Node(hostname=hostname,
                                    network=args.network,
                                    image=image,
                                    volumes=volumes))

    Cluster(topology='apache_hbase',
            node_groups=[NodeGroup(name='primary', nodes=[primary_node]),
                         NodeGroup(name='secondary', nodes=secondary_nodes)],
            network_name=args.network).start()

    # When creating configs, pass in a dictionary of wildcards into create_configurations_from_file
    # to transform placeholders in the configurations.cfg file into real values.
    _create_configs_from_file(filename=args.configurations,
                              cluster_config_dir=container_cluster_config_dir,
                              wildcards={"primary_node": args.primary_node,
                                         "secondary_nodes": args.secondary_nodes,
                                         "all_nodes": args.primary_node + args.secondary_nodes,
                                         "network": args.network})

    # After creating configurations from the configurations.cfg file, update hdfs-site.xml and
    # yarn-site.xml to use the data directories passed on the command line.
    if args.data_directories:
        _update_config_for_data_dirs(
            container_cluster_config_dir=container_cluster_config_dir,
            data_directories=data_directories
        )

    if not args.dont_start_services:
        _start_services(primary_node, hbase_master_web_ui_port=hbase_master_web_ui_port)

def _update_config_for_data_dirs(container_cluster_config_dir, data_directories):
    logger.info('Updating dfs.datanode.data.dir in hdfs-site.xml...')
    hdfs_site_xml_filename = join(container_cluster_config_dir, 'hadoop', 'hdfs-site.xml')
    hdfs_site_xml = XmlConfiguration(
        properties={'dfs.datanode.data.dir':
                    ','.join(["/dfs{0}".format(i)
                              for i, _ in enumerate(data_directories, start=1)])},
        source_file=hdfs_site_xml_filename
    )
    hdfs_site_xml.write_to_file(filename=hdfs_site_xml_filename)

    logger.info('Updating yarn.nodemanager.local-dirs in yarn-site.xml...')
    yarn_site_xml_filename = join(container_cluster_config_dir, 'hadoop', 'yarn-site.xml')
    yarn_site_xml = XmlConfiguration(
        properties={'yarn.nodemanager.local-dirs':
                    ','.join(["/yarn{0}".format(i)
                              for i, _ in enumerate(data_directories, start=1)])},
        source_file=yarn_site_xml_filename
    )
    yarn_site_xml.write_to_file(filename=yarn_site_xml_filename)

def _start_services(primary_node, **kwargs):
    logger.info("Formatting namenode on %s...", primary_node.fqdn)
    primary_node.ssh('hdfs namenode -format')

    logger.info("Starting HDFS...")
    primary_node.ssh('/hadoop/sbin/start-dfs.sh')

    logger.info("Starting YARN...")
    primary_node.ssh('/hadoop/sbin/start-yarn.sh')

    logger.info('Starting HBase...')
    primary_node.ssh('/hbase/bin/start-hbase.sh')
    primary_node.ssh('/hbase/bin/hbase-daemon.sh start rest')

    logger.info("NameNode and HBase master are located on %s. SSH over and have fun!",
                primary_node.hostname)

    logger.info("The HDFS NameNode web UI can be reached at http://%s:%s",
                getfqdn(), get_host_port_binding(primary_node.container_id,
                                                 NAMENODE_WEB_UI_PORT))

    logger.info("The YARN ResourceManager web UI can be reached at http://%s:%s",
                getfqdn(), get_host_port_binding(primary_node.container_id,
                                                 RESOURCEMANAGER_WEB_UI_PORT))

    logger.info("The HBase master web UI can be reached at http://%s:%s",
                getfqdn(), get_host_port_binding(primary_node.container_id,
                                                 kwargs.get('hbase_master_web_ui_port')))

    logger.info("The HBase REST server can be reached at http://%s:%s",
                getfqdn(), get_host_port_binding(primary_node.container_id,
                                                 HBASE_REST_SERVER_PORT))

def build(args):
    """This function will be executed when ./bin/build_cluster apache_hbase is invoked."""

    # pylint: disable=too-many-locals
    # See start function above for rationale for disabling this warning.

    container_build_dir = join(CLUSTERDOCK_VOLUME, str(uuid4()))
    makedirs(container_build_dir)

    # If --hbase-git-commit is specified, we build HBase from source.
    if args.hbase_git_commit:
        build_hbase_commands = [
            "git clone https://github.com/apache/hbase.git {0}".format(container_build_dir),
            "git -C {0} checkout {1}".format(container_build_dir, args.hbase_git_commit),
            "mvn --batch-mode clean install -DskipTests assembly:single -f {0}/pom.xml".format(
                container_build_dir
            )
        ]

        maven_image = Constants.docker_images.maven # pylint: disable=no-member
        if not is_image_available_locally(maven_image):
            pull_image(maven_image)

        container_configs = {
            'command': 'bash -c "{0}"'.format(' && '.join(build_hbase_commands)),
            'image': maven_image,
            'host_config': client.create_host_config(volumes_from=get_clusterdock_container_id())
        }

        maven_container_id = client.create_container(**container_configs)['Id']
        client.start(container=maven_container_id)
        for line in client.logs(container=maven_container_id, stream=True):
            stdout.write(line)
            stdout.flush()

        # Mimic docker run --rm by blocking on docker wait and then removing the container
        # if it encountered no errors.
        if client.wait(container=maven_container_id) == EX_OK:
            client.remove_container(container=maven_container_id, force=True)
        else:
            raise Exception('Error encountered while building HBase.')

        assembly_target_dir = join(container_build_dir, 'hbase-assembly', 'target')
        for a_file in listdir(assembly_target_dir):
            if a_file.endswith('bin.tar.gz'):
                args.hbase_tarball = join(assembly_target_dir, a_file)
                break

    # Download all the binary tarballs into our temporary directory so that we can add them
    # into the Docker image we're building.
    filenames = []
    for tarball_location in [args.java_tarball, args.hadoop_tarball, args.hbase_tarball]:
        tarball_filename = tarball_location.rsplit('/', 1)[-1]
        filenames.append(tarball_filename)

        # Download tarballs given as URLs.
        if container_build_dir not in tarball_location:
            get_request = requests.get(tarball_location, stream=True, cookies=(
                {'oraclelicense': 'accept-securebackup-cookie'}
                if tarball_location == args.java_tarball
                else None
            ))
            # Raise Exception if download failed.
            get_request.raise_for_status()
            logger.info("Downloading %s...", tarball_filename)
            with open(join(container_build_dir, tarball_filename), 'wb') as file_descriptor:
                for chunk in get_request.iter_content(1024):
                    file_descriptor.write(chunk)
        else:
            move(tarball_location, container_build_dir)

    dockerfile_contents = r"""
    FROM {nodebase_image}
    COPY {java_tarball} /tarballs/
    RUN mkdir /java && tar -xf /tarballs/{java_tarball} -C /java --strip-components=1
    RUN echo "JAVA_HOME=/java" >> /etc/environment

    COPY {hadoop_tarball} /tarballs/
    RUN mkdir /hadoop && tar -xf /tarballs/{hadoop_tarball} -C /hadoop --strip-components=1
    COPY {hbase_tarball} /tarballs/
    RUN mkdir /hbase && tar -xf /tarballs/{hbase_tarball} -C /hbase --strip-components=1

    # Remove tarballs folder.
    RUN rm -rf /tarballs

    # Set PATH explicitly.
    RUN echo "PATH=/java/bin:/hadoop/bin:/hbase/bin/:$(echo $PATH)" >> /etc/environment

    # Add hbase user and group before copying root's SSH keys over.
    RUN groupadd hbase \
        && useradd -g hbase hbase \
        && cp -R /root/.ssh ~hbase \
        && chown -R hbase:hbase ~hbase/.ssh

    # Disable requiretty in /etc/sudoers as required by HBase chaos monkey.
    RUN sed -i 's/Defaults\s*requiretty/#&/' /etc/sudoers
    """.format(nodebase_image='/'.join([item
                                        for item in [args.registry_url,
                                                     args.namespace or DEFAULT_APACHE_NAMESPACE,
                                                     "clusterdock:{os}_nodebase".format(
                                                         os=args.operating_system
                                                     )]
                                        if item]),
               java_tarball=filenames[0], hadoop_tarball=filenames[1], hbase_tarball=filenames[2])

    logger.info("Created Dockerfile: %s", dockerfile_contents)

    with open(join(container_build_dir, 'Dockerfile'), 'w') as dockerfile:
        dockerfile.write(dockerfile_contents)

    image = '/'.join(
        [item
         for item in [args.registry_url, args.namespace or DEFAULT_APACHE_NAMESPACE,
                      "clusterdock:{os}_java-{java}_hadoop-{hadoop}_hbase-{hbase}".format(
                          os=args.operating_system, java=args.java_version,
                          hadoop=args.hadoop_version, hbase=args.hbase_version
                      )]
         if item])

    logger.info("Building image %s...", image)
    build_image(dockerfile=join(container_build_dir, 'Dockerfile'), tag=image)

    logger.info("Removing build temporary directory...")
    return [image]
