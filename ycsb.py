import pprint
import tempfile

import os
import paramiko
from multiprocessing import Pool
import logging
import tarfile

from paramiko import PKey

logger = logging.getLogger('ycsb')
import glob


class YCSB:
    def __init__(self, execution_id, config, credentials):
        self.execution_id = execution_id
        self.config = config
        self.credentials = credentials

    def single_initialise_client(self, client_uri, ycsb_tar_file):
        ssh_ = self.credentials["ssh"]
        ssh_key, ssh_username, ssh_password = ssh_["key"], ssh_["username"], ssh_["password"]
        client = self.ssh_client(client_uri, ssh_key, ssh_password, ssh_username)
        try:
            remote_dir = "/tmp/{execution_id}".format(execution_id=self.execution_id)
            stdin, stdout, stderr = client.exec_command("mkdir -p {remote_dir}/ycsb".format(remote_dir=remote_dir))
            mkdir_err = self.drain_channel(stderr)
            if len(mkdir_err):
                raise Exception("Could not create remote directory {}, error: {}".format(remote_dir, mkdir_err))
            ftp_client = client.open_sftp()
            ftp_client.put(ycsb_tar_file, "{remote_dir}/ycsb.tar".format(remote_dir=remote_dir))
            ftp_client.close()
            logger.info("Extracting ycsb to {remote_dir}/ycsb".format(remote_dir=remote_dir))
            command = "tar -xvf {remote_dir}/ycsb.tar -C {remote_dir}/ycsb".format(remote_dir=remote_dir)
            stdin, stdout, stderr = client.exec_command(command)
            self.drain_channel(stdout, True)
            self.drain_channel(stderr, True)
        finally:
            client.close()

    def single_load_data(self, client_uri, cluster_uri):
        ssh_ = self.credentials["ssh"]
        ssh_key, ssh_username, ssh_password = ssh_["key"], ssh_["username"], ssh_["password"]
        data_ = self.config["data"]
        client = self.ssh_client(client_uri, ssh_key, ssh_password, ssh_username)
        try:
            command = "cd /tmp/{execution_id}/ycsb;" \
                      "bash -l -c \"./bin/ycsb load grakn -P workloads/workloada " \
                      "-p grakn.endpoint={cluster_uri} -p grakn.keyspace=ks_{execution_id} " \
                      "-p recordcount={recordcount} -p fieldcount={fieldcount} -p fieldlength={fieldlength}\"".format(
                execution_id=self.execution_id, cluster_uri=cluster_uri, recordcount=data_["records"],
                fieldcount=data_["fieldcount"], fieldlength=data_["fieldlength"])
            stdin, stdout, stderr = client.exec_command(command)
            error = self.drain_channel(stderr)
            if len(error):
                raise Exception("Error while executing benchmark with command {}, error: \n{}".format(command, error))
            with open(os.path.join(self.config['reportpath'], "load.log"), 'w') as x_file:
                for line in stdout:
                    x_file.write('{}'.format(line))
            logger.info("Executed benchmark, saved in {}", )

        finally:
            client.close()

    def run_load_data(self, client_uris, cluster_uri):
        with Pool(processes=len(client_uris)) as pool:
            results = pool.starmap(self.single_load_data,
                                   [(uri, cluster_uri) for uri in
                                    client_uris])

    def initialise_cluster(self, cluster_size):
        url = self.config["cluster"]["url"]
        if url:
            logger.info("Pointing benchmarks to %s", url)
            return url
        else:
            raise Exception("Cluster endpoint is not provided but instantiating a new cluster not implemented")

    def initialise_clients(self, ycsb_tar_file):
        client_ = self.config["client"]
        client_url_list = client_["url_list"]
        if client_url_list:
            logger.info("Initialising clients at %s", ",".join(client_url_list))
        else:
            n = client_["number"]
            raise Exception("Clients are not provided but instantiating a new cluster not implemented")
        with Pool(processes=len(client_url_list)) as pool:
            pool.starmap(self.single_initialise_client, [(url, ycsb_tar_file) for url in client_url_list])
        logger.info("Clients instantiated successfully")
        return client_url_list

    def run(self):
        ycsb_tar_file = self.config["ycsb_repo"]["tar"]
        if not ycsb_tar_file:
            ycsb_tar_file = self.create_tar(os.path.expanduser(self.config["ycsb_repo"]["path"]))
        logger.info("Running YCSB tests with configuration \n -> %s", pprint.pformat(self.config))
        logger.info("Using YCSB distribution file located at \s", ycsb_tar_file)
        # "As client nodes, the c3.xlarge class of instances (7.5GB RAM, 4 CPU cores)
        # were used to drive the test activity."
        client_urls = self.initialise_clients(ycsb_tar_file)
        cluster_config = self.config["cluster"]
        for cluster_size in cluster_config["size_list"]:
            # See: Benchmarking Top NoSQL Databases as reference
            # "The tests ran on Amazon Web Services EC2 instances, using the latest instance generation.
            # The tests used the i2.xlarge class of instances
            # (30.5 GB RAM, 4 CPU cores, and a single volume of 800 GB of SSD local storage) for the database nodes"
            cluster_url = self.initialise_cluster(cluster_size)
            # Reference paper: non durable writes
            # We run 3 YCSB workloads, A (Write: 50%, Read: 50%), C (Write: 0%, Read: 100%) and H (Write: 100%, Read: 0%)
            self.run_load_data(client_urls, cluster_url)

    @staticmethod
    def create_tar(path_to_local_ycsb):
        jar_ = path_to_local_ycsb + "/grakn/target/grakn-binding-*.jar"
        grakn_compiled = glob.glob(jar_)
        if not len(grakn_compiled) > 0:
            raise Exception("Could not find a build of the grakn binding in " + jar_)
        dir = tempfile.mkdtemp()
        tar_ = dir + "/ycsb.tar"
        with tarfile.open(tar_, "w:gz") as tar:
            tar.add(path_to_local_ycsb, arcname=".")
        return tar_

    @staticmethod
    def ssh_client(client_uri, ssh_key, ssh_password, ssh_username):
        client = paramiko.SSHClient()
        if ssh_key:
            client.get_host_keys().add(client_uri, 'ssh-rsa', PKey(data=ssh_key))
        else:
            logger.info("Key not provided, loading system host keys")
            client.load_system_host_keys()
        client.connect(client_uri, username=ssh_username, password=ssh_password)
        return client

    @staticmethod
    def drain_channel(stdout, print_lines=False):
        output = ""
        for line in stdout:
            output += line
            if print_lines:
                print(line, end=" ")
        return output
