# What is this?

It's a Python script that simplifies running YCSB benchmarks on GRAKN.

This will be extended to 1) run other types of benchmarks 2) automatically resize the cluster and
execute the tests so to get results on clusters of different sizes

# Installing Prerequisites

Install `pip`, `pyenv`, and `virtualenv`:
```
sudo easy_install pip
brew install pyenv
pip install --upgrade virtualenv
```

Install Python 3.6.3, use it and initialize a `virtualenv` environment in the repo:
```
cd /path/to/grakn-benchmarks-repo
pyenv install -l
pyenv install 3.6.3
eval "$(pyenv init -)"
virtualenv --system-site-packages -p python3 .
source bin/activate
```

Install all dependencies:
```
pip install -r requirements.txt
```

# How do I run it?

```
python run.py --configpath config/ycsb_oracle.yml --credentialspath config/.credentials_oracle.yml --ycsb_remote_tar_path /tmp/22112017_151331/ycsb-0.14.0-SNAPSHOT.tar.gz
```

It can automatically find and try public keys located at `~/.ssh/`. Otherwise, you will need a config file similar to `config/credentials_example.yml` which should look like this:
```
ssh:
  username: domenico
  password: somepassword
  key: "/Users/domenico/.ssh/id_rsa"
```

## Important!

The YCSB distribution needs to be configured for Grakn.
Clone this branch:

```
https://github.com/graknlabs/YCSB/tree/grakn
```

If the repo is in directory `/Users/domenico/Dev/git/YCSB` then you can run the script with the option

```
--ycsb_path  /Users/domenico/Dev/git/YCSB
```

This will take care of compiling and packaging.

If you have a distribution ready in `/Users/domenico/Dev/git/YCSB/distribution/target/ycsb-0.14.0-SNAPSHOT.tar.gz` use

```
--ycsb_tar_path  /Users/domenico/Dev/git/YCSB/distribution/target/ycsb-0.14.0-SNAPSHOT.tar.gz
```