# ib-topo

A tool to categorize hosts by ToRsets on a SHARP-enabled cluster to facilitate topology aware placement of jobs.

## Install

Prepare a venv:

```bash
python3 -m venv ib-topo
source ib-topo/bin/activate
```

```bash
pip install .
```

## Usage

```bash
ib-topo <path-to-host-file> <username> <path-to-ssh-private-key> <path-to-sharp-cmd> <path-to-output-dir>
```

### Outputs

This will create a number of files in the <output> directory:

- guids.txt: A file with the InfiniBand device GUIDs from every host
- topology.txt: A file with the InfiniBand fabric topology output from `sharp_cmd`
- torset-NN_hosts.txt: A set of files with the hosts belonging to each torset.

## Dev

### Test

```bash
python3 -m pytest
```
