import os

import pytest

from todo_sync_test import TEST_DB_DIR


@pytest.fixture(autouse=True)
def clean_test_db_dir() -> None:
    for file in os.listdir(TEST_DB_DIR):
        path = os.path.join(TEST_DB_DIR, file)
        if os.path.isfile(path) and path.endswith(".db"):
            os.remove(path)
