from pathlib import Path

import pytest

created_files = ['tests/data/torset-{i:02d}_hosts.txt'.format(i=i) for i in range(12)]
created_files = created_files + ['tests/data/new_guids.txt']


def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 0:
        for file in created_files:
            Path(file).unlink()
        print("All tests passed!")
