from pathlib import Path

import pytest

from ibtopo import IBTopology

OUTPUT_DIR = Path('tests/data')
HOSTS_FILE = Path('tests/data/hosts.txt')
GUIDS_FILE = Path('tests/data/guids.txt')
SHARP_CMD = 'sharp_cmd'
NEW_GUIDS_FILE = 'tests/data/new_guids.txt'
TOPO_FILE = 'tests/data/topology.txt'
NEW_TOPO_FILE = 'tests/data/new_topology.txt'
MOCKED_GUID_TO_HOST_IP = {
    '0x155dfffd341acb': '10.193.0.13',
    '0x155dfffd341acc': '10.193.0.13',
    '0x155dfffd341acd': '10.193.0.13',
    '0x155dfffd341ace': '10.193.0.13',
    '0x155dfffd341acf': '10.193.0.13',
    '0x155dfffd341ad0': '10.193.0.13',
    '0x155dfffd341ad1': '10.193.0.13',
    '0x155dfffd341ad2': '10.193.0.13',
    '0x155dfffd341afb': '10.193.0.19',
    '0x155dfffd341afc': '10.193.0.19',
    '0x155dfffd341afd': '10.193.0.19',
    '0x155dfffd341afe': '10.193.0.19',
    '0x155dfffd341aff': '10.193.0.19',
    '0x155dfffd341b00': '10.193.0.19',
    '0x155dfffd341b01': '10.193.0.19',
    '0x155dfffd341b02': '10.193.0.19',
    '0x155dfffd34193b': '10.193.0.10',
    '0x155dfffd34193c': '10.193.0.10',
    '0x155dfffd34193d': '10.193.0.10',
    '0x155dfffd34193e': '10.193.0.10',
    '0x155dfffd34193f': '10.193.0.10',
    '0x155dfffd341940': '10.193.0.10',
    '0x155dfffd341941': '10.193.0.10',
    '0x155dfffd341942': '10.193.0.10',
    '0x155dfffd341b0b': '10.193.0.9',
    '0x155dfffd341b0c': '10.193.0.9',
    '0x155dfffd341b0d': '10.193.0.9',
    '0x155dfffd341b0e': '10.193.0.9',
    '0x155dfffd341b0f': '10.193.0.9',
    '0x155dfffd341b10': '10.193.0.9',
    '0x155dfffd341b11': '10.193.0.9',
    '0x155dfffd341b12': '10.193.0.9',
    '0x155dfffd34168b': '10.193.0.4',
    '0x155dfffd34168c': '10.193.0.4',
    '0x155dfffd34168d': '10.193.0.4',
    '0x155dfffd34168e': '10.193.0.4',
    '0x155dfffd34168f': '10.193.0.4',
    '0x155dfffd341690': '10.193.0.4',
    '0x155dfffd341691': '10.193.0.4',
    '0x155dfffd341692': '10.193.0.4',
    '0x155dfffd3416bb': '10.193.0.7',
    '0x155dfffd3416bc': '10.193.0.7',
    '0x155dfffd3416bd': '10.193.0.7',
    '0x155dfffd3416be': '10.193.0.7',
    '0x155dfffd3416bf': '10.193.0.7',
    '0x155dfffd3416c0': '10.193.0.7',
    '0x155dfffd3416c1': '10.193.0.7',
    '0x155dfffd3416c2': '10.193.0.7',
    '0x155dfffd341b23': '10.193.0.5',
    '0x155dfffd341b24': '10.193.0.5',
    '0x155dfffd341b25': '10.193.0.5',
    '0x155dfffd341b26': '10.193.0.5',
    '0x155dfffd341b27': '10.193.0.5',
    '0x155dfffd341b28': '10.193.0.5',
    '0x155dfffd341b29': '10.193.0.5',
    '0x155dfffd341b2a': '10.193.0.5',
    '0x155dfffd34136b': '10.193.0.18',
    '0x155dfffd34136c': '10.193.0.18',
    '0x155dfffd34136d': '10.193.0.18',
    '0x155dfffd34136e': '10.193.0.18',
    '0x155dfffd34136f': '10.193.0.18',
    '0x155dfffd341370': '10.193.0.18',
    '0x155dfffd341371': '10.193.0.18',
    '0x155dfffd341372': '10.193.0.18',
    '0x155dfffd34110b': '10.193.0.11',
    '0x155dfffd34110c': '10.193.0.11',
    '0x155dfffd34110d': '10.193.0.11',
    '0x155dfffd34110e': '10.193.0.11',
    '0x155dfffd34110f': '10.193.0.11',
    '0x155dfffd341110': '10.193.0.11',
    '0x155dfffd341111': '10.193.0.11',
    '0x155dfffd341112': '10.193.0.11',
    '0x155dfffd341b1b': '10.193.0.17',
    '0x155dfffd341b1c': '10.193.0.17',
    '0x155dfffd341b1d': '10.193.0.17',
    '0x155dfffd341b1e': '10.193.0.17',
    '0x155dfffd341b1f': '10.193.0.17',
    '0x155dfffd341b20': '10.193.0.17',
    '0x155dfffd341b21': '10.193.0.17',
    '0x155dfffd341b22': '10.193.0.17',
    '0x155dfffd341adb': '10.193.0.8',
    '0x155dfffd341adc': '10.193.0.8',
    '0x155dfffd341add': '10.193.0.8',
    '0x155dfffd341ade': '10.193.0.8',
    '0x155dfffd341adf': '10.193.0.8',
    '0x155dfffd341ae0': '10.193.0.8',
    '0x155dfffd341ae1': '10.193.0.8',
    '0x155dfffd341ae2': '10.193.0.8',
    '0x155dfffd341b03': '10.193.0.16',
    '0x155dfffd341b04': '10.193.0.16',
    '0x155dfffd341b05': '10.193.0.16',
    '0x155dfffd341b06': '10.193.0.16',
    '0x155dfffd341b07': '10.193.0.16',
    '0x155dfffd341b08': '10.193.0.16',
    '0x155dfffd341b09': '10.193.0.16',
    '0x155dfffd341b0a': '10.193.0.16',
    '0x155dfffd341a03': '10.193.0.15',
    '0x155dfffd341a04': '10.193.0.15',
    '0x155dfffd341a05': '10.193.0.15',
    '0x155dfffd341a06': '10.193.0.15',
    '0x155dfffd341a07': '10.193.0.15',
    '0x155dfffd341a08': '10.193.0.15',
    '0x155dfffd341a09': '10.193.0.15',
    '0x155dfffd341a0a': '10.193.0.15',
    '0x155dfffd341aeb': '10.193.0.12',
    '0x155dfffd341aec': '10.193.0.12',
    '0x155dfffd341aed': '10.193.0.12',
    '0x155dfffd341aee': '10.193.0.12',
    '0x155dfffd341aef': '10.193.0.12',
    '0x155dfffd341af0': '10.193.0.12',
    '0x155dfffd341af1': '10.193.0.12',
    '0x155dfffd341af2': '10.193.0.12',
    '0x155dfffd341abb': '10.193.0.6',
    '0x155dfffd341abc': '10.193.0.6',
    '0x155dfffd341abd': '10.193.0.6',
    '0x155dfffd341abe': '10.193.0.6',
    '0x155dfffd341abf': '10.193.0.6',
    '0x155dfffd341ac0': '10.193.0.6',
    '0x155dfffd341ac1': '10.193.0.6',
    '0x155dfffd341ac2': '10.193.0.6',
    '0x155dfffd341ad3': '10.193.0.14',
    '0x155dfffd341ad4': '10.193.0.14',
    '0x155dfffd341ad5': '10.193.0.14',
    '0x155dfffd341ad6': '10.193.0.14',
    '0x155dfffd341ad7': '10.193.0.14',
    '0x155dfffd341ad8': '10.193.0.14',
    '0x155dfffd341ad9': '10.193.0.14',
    '0x155dfffd341ada': '10.193.0.14',
}


def test_ibtopology_init():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    assert str(ibtopo.hosts_file) == str(HOSTS_FILE)
    assert str(ibtopo.guids_file) == str(GUIDS_FILE)
    assert str(ibtopo.topo_file) == str(TOPO_FILE)
    assert ibtopo.sharp_cmd_path == SHARP_CMD
    assert ibtopo.sharp_smx_ucx_interface == 'mlx5_ib0:1'

    assert len(ibtopo.hosts) == 16


def test_read_host_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    hosts = ibtopo._read_hosts_file()
    assert type(hosts) is list
    assert len(hosts) == 16


@pytest.mark.skip(reason="Requires testing on InfiniBand cluster")
def test_fetch_guids(username, private_key):
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    guids = ibtopo.fetch_guids(username, private_key)
    assert type(guids) is dict


def test_write_guids_to_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.guids_file = NEW_GUIDS_FILE
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.write_guids_to_file(ibtopo.guids_file)

    with open(ibtopo.guids_file, 'r') as test_file, open(GUIDS_FILE, 'r') as expected_file:
        test_data = test_file.read().strip()
        expected_data = expected_file.read().strip()

        assert test_data == expected_data


@pytest.mark.skip(reason="Requires testing on InfiniBand cluster")
def test_create_topo_file():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.topo_file = NEW_TOPO_FILE
    ibtopo.create_topo_file()

    with open(ibtopo.topo_file, 'r') as test_file, open(TOPO_FILE, 'r') as expected_file:
        test_data = test_file.read()
        expected_data = expected_file.read()

        assert test_data == expected_data


def test_populate_device_guids_per_switch():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()

    # 128 devices (GUIDs) total, 32 switches have 2 devices -> 96 switches
    assert len(ibtopo.device_guids_per_switch) == 96


def test_populate_graph():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()
    ibtopo.graph = ibtopo._populate_graph()

    # Includes switches and hosts
    assert len(ibtopo.graph.nodes) == 241
    assert len(ibtopo.graph.edges) == 240


def test_identify_torsets():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    torset_nodes_map = ibtopo.identify_torsets()

    assert len(torset_nodes_map) == 16


def test_group_hosts_by_torset():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.host_ip_to_torset = ibtopo.identify_torsets()

    torsets = ibtopo.group_hosts_by_torset()
    assert len(torsets) == 12


def test_write_hosts_by_torset():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.host_ip_to_torset = ibtopo.identify_torsets()
    ibtopo.torsets = ibtopo.group_hosts_by_torset()
    ibtopo.write_hosts_by_torset()

    for torset, _ in ibtopo.torsets.items():
        with open(f"tests/data/{torset}_hosts.txt", 'r') as test_file, open(f"tests/data/ref_{torset}_hosts.txt", 'r') as expected_file:
            test_data = test_file.read()
            expected_data = expected_file.read()

            assert test_data == expected_data


def test_draw_topology():
    ibtopo = IBTopology(OUTPUT_DIR, HOSTS_FILE, SHARP_CMD)
    ibtopo.device_guids_per_switch = ibtopo._populate_device_guids_per_switch()
    ibtopo.graph = ibtopo._populate_graph()
    ibtopo.guid_to_host_ip = MOCKED_GUID_TO_HOST_IP
    ibtopo.host_ip_to_torset = ibtopo.identify_torsets()
    ibtopo.torsets = ibtopo.group_hosts_by_torset()
    ibtopo.draw_topology()
