"""Configuration for pytest."""
import os


def pytest_generate_tests(metafunc):
    """Generate test for each file in specified path.

    Args:
        metafunc: pytest object
    """
    if "json_file" in metafunc.fixturenames:
        path = "bi/transformation/tests/json/"
        files = [path + x for x in os.listdir(path)]
        metafunc.parametrize("json_file", files)
