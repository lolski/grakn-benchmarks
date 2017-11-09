import argparse
import logging
import time

import os

import errno
import yaml

import ycsb

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


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
