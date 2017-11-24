import json
import pprint
import tempfile

import os
import paramiko
from multiprocessing import Pool
import logging
import tarfile

logger = logging.getLogger('ycsb')
import glob


class YCSB:
    def __init__(self, execution_id, config, credentials):
        self.execution_id = execution_id
        self.config = config
        self.credentials = credentials

    def single_initialise_client(self, client_uri, ycsb_tar_file):
        logger.info("Initialising client at %s", client_uri)
        ssh_ = self.credentials.get("ssh") if self.credentials else None
        if ssh_:
            ssh_key, ssh_username, ssh_password = ssh_.get("key"), ssh_.get("username"), ssh_.get("password")
        else:
            ssh_key, ssh_username, ssh_password = None, None, None
        client = None
        try:
            client = self.ssh_client(client_uri, ssh_key, ssh_password, ssh_username)
            remote_dir = "/tmp/{execution_id}".format(execution_id=self.execution_id)
            stdin, stdout, stderr = client.exec_command("mkdir -p {remote_dir}/ycsb".format(remote_dir=remote_dir))
            mkdir_err = self.drain_channel(stderr)
            if len(mkdir_err):
                raise Exception("Could not create remote directory {}, error: {}".format(remote_dir, mkdir_err))
            remote_tar_file = self.config["ycsb_repo"]["ycsb_remote_tar_path"]
            if not remote_tar_file:
                _, file = os.path.split(ycsb_tar_file)
                remote_tar_file = "{remote_dir}/{file}".format(remote_dir=remote_dir, file=file)
                logger.info(
                    ("Copying YCSB distribution to {client_uri}:" + remote_tar_file + " (might take a while)").format(
                        client_uri=client_uri))
                ftp_client = client.open_sftp()
                ftp_client.put(ycsb_tar_file, remote_tar_file)
                ftp_client.close()
            logger.info("Extracting ycsb to from {client_uri}:{remote_tar_file} to {client_uri}:{remote_dir}/ycsb"
                        .format(client_uri=client_uri, remote_dir=remote_dir, remote_tar_file=remote_tar_file))
            command = "tar -xf " + remote_tar_file + " -C {remote_dir}/ycsb --strip-components 1".format(
                remote_dir=remote_dir)
            logger.info(command)
            stdin, stdout, stderr = client.exec_command(command)
            self.drain_channel(stdout, False)
            error = self.drain_channel(stderr, False)
            logger.debug(error)
            logger.info("Client {client_uri} ready!".format(client_uri=client_uri))
        except Exception as e:
            logger.error("Error while initialising client {}: {}".format(client_uri, e))
            raise e
        finally:
            if client:
                client.close()

    def single_query_data(self, client_uri, cluster_uri, workload):
        logger.info("Queying data from %s to %s with workload %s", client_uri, cluster_uri, workload)
        ssh_ = self.credentials["ssh"]
        ssh_key, ssh_username, ssh_password = ssh_.get("key"), ssh_.get("username"), ssh_.get("password")
        client = self.ssh_client(client_uri, ssh_key, ssh_password, ssh_username)
        results = {}
        data_ = self.config["data"]
        try:
            command = "cd /tmp/{execution_id}/ycsb; " \
                      "bash -l -c \"./bin/ycsb run grakn " \
                      "-P workloads/{workload} -s " \
                      "-threads {threads} " \
                      "-p grakn.endpoint={cluster_uri} " \
                      "-p grakn.keyspace=ks_{execution_id} " \
                      "-p recordcount={recordcount} " \
                      "-p operationcount={operationcount} " \
                      "-p fieldcount={fieldcount} " \
                      "-p fieldlength={fieldlength} " \
                      "-p hdrhistogram.fileoutput=true " \
                      "-p hdrhistogram.output.path=/tmp/hist.log " \
                      "2>&1 | tee /tmp/graknbench.log \"".format(
                execution_id=self.execution_id, cluster_uri=cluster_uri, recordcount=data_["records"],
                threads=self.config["threads"]["run"], fieldcount=data_["fieldcount"], workload=workload,
                fieldlength=data_["fieldlength"], operationcount=data_["operations"])
            self.execute_and_monitor_command(client, client_uri, command, results)
            logger.info("Query from %s to %s terminated", client_uri, cluster_uri)
        finally:
            client.close()
        return results

    def single_load_data(self, client_uri, cluster_uri, workload):
        logger.info("Loading data from %s to %s", client_uri, cluster_uri)
        ssh_ = self.credentials["ssh"]
        ssh_key, ssh_username, ssh_password = ssh_.get("key"), ssh_.get("username"), ssh_.get("password")
        client = self.ssh_client(client_uri, ssh_key, ssh_password, ssh_username)
        results = {}
        data_ = self.config["data"]
        try:
            command = "cd /tmp/{execution_id}/ycsb; " \
                      "bash -l -c \"./bin/ycsb load grakn " \
                      "-P workloads/{workload} -s " \
                      "-threads {threads} " \
                      "-p grakn.endpoint={cluster_uri} " \
                      "-p grakn.keyspace=ks_{execution_id} " \
                      "-p recordcount={recordcount} " \
                      "-p operationcount={operationcount} " \
                      "-p fieldcount={fieldcount} " \
                      "-p fieldlength={fieldlength} " \
                      "-p hdrhistogram.fileoutput=true " \
                      "-p hdrhistogram.output.path=/tmp/hist.log " \
                      "2>&1 | tee /tmp/graknbench.log \"".format(
                execution_id=self.execution_id, cluster_uri=cluster_uri, recordcount=data_["records"],
                threads=self.config["threads"]["load"], fieldcount=data_["fieldcount"], workload=workload,
                fieldlength=data_["fieldlength"], operationcount=data_["operations"])
            self.execute_and_monitor_command(client, client_uri, command, results)
            logger.info("Load from %s to %s terminated", client_uri, cluster_uri)
        finally:
            client.close()
        return results

    def execute_and_monitor_command(self, client, client_uri, command, results):
        logger.debug("Command from %s: %s", client_uri, command)
        stdin, stdout, stderr = client.exec_command(command)
        with open(os.path.join(self.config['reportpath'], self.execution_id + "_benchmark.log"), 'a') as local_log_file:
            for line in iter(lambda: stdout.readline(2048), ""):
                if "est completion in" in line or "Return=" in line:
                    logger.info(client_uri + ": " + line.replace("\n", ""))
                if "[OVERALL]" in line:
                    fields = line.split("[OVERALL]")[1].split(",")
                    results[fields[-2].strip()] = fields[-1].strip()
                local_log_file.write(line)
        error = self.drain_channel(stderr)
        if len(error):
            raise Exception("Error while executing benchmark with command {}, error: \n{}".format(command, error))

    def run_query_data(self, client_urls, cluster_url, workload):
        pass

    def run_on_all_clients(self, client_uris, cluster_uri, name, workload, f):
        with Pool(processes=len(client_uris)) as pool:
            results = pool.starmap(f, [(uri, cluster_uri, workload) for uri in client_uris])
        js = json.dumps(results)
        with open(os.path.join(self.config['reportpath'], self.execution_id + "_{}_{}.json".format(name, workload)),
                  'w') as fp:
            fp.write(js)

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
        if not client_url_list:
            n = client_["number"]
            raise Exception("Clients are not provided but instantiating a new cluster not implemented")
        with Pool(processes=len(client_url_list)) as pool:
            pool.starmap(self.single_initialise_client, [(url, ycsb_tar_file) for url in client_url_list])
        logger.info("Clients instantiated successfully")
        return client_url_list

    def run(self):
        logger.info("Running execution %s", self.execution_id)
        ycsb_tar_file = None
        if not self.config["ycsb_repo"].get("ycsb_remote_tar_path"):
            ycsb_tar_file = self.config["ycsb_repo"].get("ycsb_tar_path")
            if not ycsb_tar_file:
                ycsb_tar_file = self.create_tar(os.path.expanduser(self.config["ycsb_repo"]["ycsb_path"]))
        logger.info("Running YCSB tests with configuration \n%s", pprint.pformat(self.config))
        logger.info("Using YCSB distribution file located at %s", ycsb_tar_file)
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
            if self.config["data"]["load"]:
                # We load only once. If you run workloads with inserts, that increases the size!
                # So run workloade for last if you use workloade
                self.run_on_all_clients(client_urls, cluster_url, "load", "workloada", self.single_load_data)
            for w in self.config["data"]["workloads"]:
                # We run 3 YCSB workloads, A (Write: 50%, Read: 50%), C (Write: 0%, Read: 100%) and H (Write: 100%, Read: 0%)
                logger.info("======= Running workload {} =======".format(w))
                self.run_on_all_clients(client_urls, cluster_url, "query", w, self.single_query_data)

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
        # if ssh_key:
        #     client.get_host_keys().add(client_uri, 'ssh-rsa', paramiko.RSAKey.from_private_key_file(ssh_key))
        # else:
        # logger.info("Key not provided, loading system host keys")
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
