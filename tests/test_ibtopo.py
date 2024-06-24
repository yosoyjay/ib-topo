from pathlib import Path

import pytest

from ibtopo import IBTopology

OUTPUT_DIR = Path('tests/data')
HOSTS_FILE = 'tests/data/hosts.txt'
GUIDS_FILE = 'tests/data/guids.txt'
NEW_GUIDS_FILE = 'tests/data/mocked_guids.txt'
TOPO_FILE = 'tests/data/topology.txt'
NEW_TOPO_FILE = 'tests/data/topo_new.txt'
MOCKED_GUID_TO_HOST_IP = {
    '0x155dfffd341acb': '10.0.0.1',
    '0x155dfffd341afb': '10.0.0.2',
    '0x155dfffd34193b': '10.0.0.3',
    '0x155dfffd341b0b': '10.0.0.4',
    '0x155dfffd34168b': '10.0.0.5',
    '0x155dfffd3416bb': '10.0.0.6',
    '0x155dfffd341b23': '10.0.0.7',
    '0x155dfffd34136b': '10.0.0.8',
    '0x155dfffd34110b': '10.0.0.9',
    '0x155dfffd341b1b': '10.0.0.10',
    '0x155dfffd341adb': '10.0.0.11',
    '0x155dfffd341b03': '10.0.0.12',
    '0x155dfffd341a03': '10.0.0.13',
    '0x155dfffd341aeb': '10.0.0.14',
    '0x155dfffd341abb': '10.0.0.15',
    '0x155dfffd341ad3': '10.0.0.16'
}


def test_ibtopology_init():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    assert str(ibtopo.hosts_file) == str('tests/data/hosts.txt')
    assert str(ibtopo.guids_file) == str('tests/data/guids.txt')
    assert str(ibtopo.topo_file) == str('tests/data/topology.txt')
    assert ibtopo.sharp_cmd_path == 'sharp_cmd'
    assert ibtopo.sharp_smx_ucx_interface == 'mlx5_ib0:1'

    assert len(ibtopo.hosts) == 16


def test_read_host_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    hosts = ibtopo._read_hosts_file()
    assert type(hosts) is list
    assert len(hosts) == 16


@pytest.mark.skip(reason="Requires testing on InfiniBand cluster")
def test_fetch_guids(username, private_key):
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    guids = ibtopo.fetch_guids(username, private_key)
    assert type(guids) is dict


def test_write_guids_to_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.guids_file = NEW_GUIDS_FILE
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.write_guids_to_file(ibtopo.guids_file)

    with open(ibtopo.guids_file, 'r') as test_file, open(GUIDS_FILE, 'r') as expected_file:
        test_data = test_file.read().strip()
        expected_data = expected_file.read().strip()

        assert test_data == expected_data


@pytest.mark.skip(reason="Requires testing on InfiniBand cluster")
def test_create_topo_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.topo_file = NEW_TOPO_FILE
    ibtopo.create_topo_file()

    with open(ibtopo.topo_file, 'r') as test_file, open(TOPO_FILE, 'r') as expected_file:
        test_data = test_file.read()
        expected_data = expected_file.read()

        assert test_data == expected_data


def test_populate_nodes():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.nodes = ibtopo._populate_nodes()

    # 4 switches have 2 hosts.  The rest have 1 host
    assert len(ibtopo.nodes) == 12


def test_populate_graph():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.nodes = ibtopo._populate_nodes()
    ibtopo.graph = ibtopo._populate_graph()

    # Includes switches and hosts
    assert len(ibtopo.graph.nodes) == 31
    assert len(ibtopo.graph.edges) == 30


def test_identify_torsets():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.nodes = ibtopo._populate_nodes()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    torset_nodes_map = ibtopo.identify_torsets()

    assert len(torset_nodes_map) == 16


def test_group_hosts_by_torset():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.nodes = ibtopo._populate_nodes()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.host_ip_to_torset = ibtopo.identify_torsets()

    torsets = ibtopo.group_hosts_by_torset()
    assert len(torsets) == 12


def test_draw_topology():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, 'sharp_cmd')
    ibtopo.nodes = ibtopo._populate_nodes()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.host_ip_to_torset = ibtopo.identify_torsets()
    ibtopo.torsets = ibtopo.group_hosts_by_torset()
    ibtopo.draw_topology()
