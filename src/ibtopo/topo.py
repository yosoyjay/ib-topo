import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

import fabric
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import networkx as nx

logging.basicConfig(level=logging.INFO)


def run_command(command):
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        if process.returncode == 0:
            return out
        else:
            return err
    except Exception as e:
        return str(e)


def run_remote_cmd(host, username, cmd):
    try:
        with fabric.Connection(host, user=username) as conn:
            result = conn.run(cmd, hide=True)
            return {
                'stdout': result.stdout.strip(),
                'stderr': result.stderr.strip(),
                'return_code': result.return_code
            }
    except Exception as e:
        raise Exception(f"Error running command on host {host}: {str(e)}")


@dataclass
class TopologyConfig:
    hosts_file: Path
    output_dir: Path
    sharp_cmd_path: Path
    sharp_smx_ucx_interface: str
    username: str
    pkey_path: Path


class IBTopology:
    # Output files directory
    output_dir: Path
    # Output file for guids from ibstat across cluster
    guids_file: Path
    guid_to_host_ip: dict = {}
    # guid_map[guid] = host
    hosts_file: Path
    # Hosts in cluster read from hosts file
    hosts: list = []
    # Output topology file from sharp_cmd
    topo_file: Path
    # List of nodes extracted from topology file
    nodes: list = []
    # Map hosts to torsets
    host_ip_to_torset: dict = {}
    # Entire graph of topology
    graph: nx.Graph
    # Map torsets to hosts
    torsets = {}

    def __init__(self, output_dir: Path, hosts_file: Path, sharp_cmd_path: Path, sharp_smx_ucx_interface: str='mlx5_ib0:1'):
        self.hosts_file = hosts_file
        self.sharp_cmd_path = sharp_cmd_path
        self.sharp_smx_ucx_interface = sharp_smx_ucx_interface
        self.output_dir = output_dir
        self.guids_file = output_dir / 'guids.txt'
        self.topo_file = output_dir / 'topology.txt'

        self.hosts = self._read_hosts_file()

    def _read_hosts_file(self):
        with open(self.hosts_file, 'r') as f:
            hosts = [host.strip() for host in f.readlines()]

        return hosts

    def _fetch_guid(self, host, username, private_key):
        cmd = "ibstat | grep 'Port GUID' | cut -d ':' -f 2"
        return run_remote_cmd(host, username, cmd)

    def fetch_guids(self, username, private_key):
        guids = {}
        for host in self.hosts:
            result = self._fetch_guid(host, username, private_key)
            if result['return_code'] == 0:
                # Querying GUIDs from ibstat will have pattern 0x0099999999999999, but Sharp will return 0x99999999999999
                # - So we need to remove the leading 00 after 0x
                guid = result['stdout'].replace('0x00', '0x')
                guids[guid] = host
            else:
                logging.error(f"Error fetching GUID for host {host}")
        return guids

    def write_guids_to_file(self, guids_file):
        with open(guids_file, 'w') as f:
            for guid in self.guid_to_host_ip.keys():
                f.write(f"{guid}\n")

    def create_topo_file(self):
        cmd = f"SHARP_SMX_UCX_INTERFACE={self.sharp_smx_ucx_interface}; {self.sharp_cmd_path} topology --ib-dev {self.sharp_smx_ucx_interface} --guids_file {self.guids_file} --topology_file {self.topo_file}"
        run_command(cmd)
        logging.info(f"Topology file generated at {self.topo_file}")

    def _populate_nodes(self):
        nodes = []
        with open(self.topo_file, 'r') as f:
            for line in f:
                if 'Nodes=' not in line:
                    continue
                # 'SwitchName=ibsw2 Nodes=0x155dfffd341acb,0x155dfffd341b0b'
                # Want GUIDs for nodes
                nodes.append(line.strip().split(' ')[1].split('=')[1])
        return nodes

    def _populate_graph(self):
        graph = nx.Graph()
        with open(self.topo_file, 'r') as f:
            for line in f.readlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.split()
                switch_name = parts[0].split("=")[1]
                if len(parts) > 1:
                    connections = parts[1].split("=")
                    if connections[0] == "Switches":
                        connected_switches = connections[1].split(",")
                        for conn_switch in connected_switches:
                            graph.add_edge(switch_name, conn_switch)
                    elif connections[0] == "Nodes":
                        connected_nodes = connections[1].split(",")
                        for node in connected_nodes:
                            graph.add_node(node, type='node')
                            graph.add_edge(switch_name, node, type='switch-to-node')
        return graph

    def identify_torsets(self):
        # torset_node_map equivalent
        host_ip_to_torset = {}
        for ix, node in enumerate(self.nodes):
            node_guids = node.strip().split(",")
#            torset_index = len(host_ip_to_torset)
            for guid in node_guids:
                host_ip = self.guid_to_host_ip[guid]
                if host_ip in host_ip_to_torset:
                    continue
                host_ip_to_torset[host_ip] = f"torset-{ix:02}"
        return host_ip_to_torset

    def group_hosts_by_torset(self):
        torsets = {}
        for host_ip, torset in self.host_ip_to_torset.items():
            if torset not in torsets:
                torsets[torset] = [host_ip]
            else:
                torsets[torset].append(host_ip)
        return torsets

    def write_hosts_by_torset(self):
        for torset, hosts in self.torsets.items():
            with open(f"{torset}_hosts.txt", 'w') as f:
                for host in hosts:
                    f.write(f"{host}\n")

    def draw_topology(self):
        pos = nx.spring_layout(self.graph, k=0.5, iterations=100)
        nx.draw(self.graph, pos, with_labels=True)
        plt.savefig(self.output_dir / 'topology.png')


def main(topo_config: TopologyConfig):
    hosts_path = topo_config.hosts_file
    sharp_if = topo_config.sharp_smx_ucx_interface
    sharp_cmd = topo_config.sharp_cmd_path
    username = topo_config.username
    pkey_path = topo_config.pkey_path
    output_dir = topo_config.output_dir

    ib_topology = IBTopology(output_dir, hosts_path, sharp_if, sharp_cmd)
    ib_topology.guid_to_host_ip = ib_topology.fetch_guids(username, pkey_path)
    logging.info("Finished collecting Infiniband device GUIDs from hosts")
    ib_topology.write_guids_to_file(ib_topology.guids_file)
    logging.info(f"GUIDs written to {ib_topology.guids_file}")
    ib_topology.create_topo_file()
    logging.info(f"Topology file generated at {ib_topology.topo_file}")
    ib_topology.nodes = ib_topology._populate_nodes()
    ib_topology.graph = ib_topology._populate_graph()
    logging.info("Populated graph from topology file")
    ib_topology.host_ip_to_torset = ib_topology.identify_torsets()
    logging.info("Identified torsets for hosts")
    ib_topology.torsets = ib_topology.group_hosts_by_torset()
    ib_topology.write_hosts_by_torset()
    logging.info("Hosts grouped by torset and written to files in {ib_topology.output_dir}")
    ib_topology.draw_topology()
    logging.info("Topology graph saved to {ib_topology.output_dir / 'topology.png'}")

    logging.info("{len(ib_topology.host_ip_to_torset)} nodes identified")
    logging.info("{len(ib_topology.torsets)} torsets identified")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts', type=str, help='Path to host file')
    parser.add_argument('--username', type=str, help='Username to connect to hosts')
    parser.add_argument('--pkey_path', type=str, help='Path to user private key')
    parser.add_argument('--sharp_cmd_path', type=str, help='Path to sharp_cmd')
    parser.add_argument('--output_dir', type=str, help='Output directory for generated files')
    parser.add_argument('sharp_smx_ucx_interface', type=str, default='mlx5_ib0:1', help='Sharp SMX UCX Interface (default: mlx5_ib0:1)')

    args = parser.parse_args()
    torset_config = TopologyConfig(
        hosts_file=Path(args.hosts),
        output_dir=Path(args.output_dir),
        sharp_cmd_path=Path(args.sharp_cmd_path),
        sharp_smx_ucx_interface=args.sharp_smx_ucx_interface,
        username=args.username,
        pkey_path=Path(args.pkey_path)
    )

    main(torset_config)