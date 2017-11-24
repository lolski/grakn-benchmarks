import argparse
import coloredlogs, logging
import time

import os

import errno
import yaml

import ycsb

coloredlogs.install()
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("paramiko.transport").setLevel(logging.WARNING)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def main():
    parser = argparse.ArgumentParser()
    # Test type
    parser.add_argument('--ycsb', type=bool, default=True, help='Run YCSB benchmarks')
    parser.add_argument('--snb', type=bool, default=True, help='Run SNB benchmarks')
    # Configuration
    parser.add_argument('--configpath', type=str, default="config/ycsb_local.yml",
                        help='Path to the yml file containing the experiment configuration')
    parser.add_argument('--credentialspath', type=str, default="config/credentials_example.yml",
                        help='Path to the yml file containing the credentials')
    parser.add_argument('--reportpath', type=str, default="reports",
                        help='Path to the directory where reports are stored')
    parser.add_argument('--ycsb_path', type=str,
                        help='Path to ycsb code')
    parser.add_argument('--ycsb_tar_path', type=str,
                        help='Path to ycsb tar distribution (local). It overrides ycsb_path.')
    parser.add_argument('--ycsb_remote_tar_path', type=str,
                        help='Path to ycsb tar distribution (remote, it has to be on all the clients). It overrides ycsb_path.')
    args = parser.parse_args()

    config = None
    with open(args.configpath, 'r') as stream:
        try:
            config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit(-1)

    mkdir_p(args.reportpath)
    config['reportpath'] = args.reportpath
    if args.ycsb and (args.ycsb_path or args.ycsb_tar_path or args.ycsb_remote_tar_path):
        config['ycsb_repo'] = {'ycsb_path': args.ycsb_path, 'ycsb_tar_path': args.ycsb_tar_path, 'ycsb_remote_tar_path': args.ycsb_remote_tar_path}
    credentials = None
    with open(args.credentialspath, 'r') as stream:
        try:
            credentials = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit(-1)

    executionid = time.strftime("%d%m%Y_%H%M%S")
    ycsb_benchmark = ycsb.YCSB(executionid, config, credentials)
    ycsb_benchmark.run()


if __name__ == "__main__":
    main()
