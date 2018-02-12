"""Test commands from mongozen.util."""

import os
import shutil

import pytest
import pandas as pd
from birch import Birch
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from mongozen.util import export_collection


TEST_CFG = Birch('mztest', supported_formats='json')
TEMP_DIR = os.path.expanduser('~/.tempmongozentest')


@pytest.fixture(scope="session", autouse=True)
def do_something(request):
    # Will be executed before the first test
    os.makedirs(TEMP_DIR, exist_ok=True)

    yield

    # Will be executed after the last test
    shutil.rmtree(TEMP_DIR)


def test_export():
    client = MongoClient(TEST_CFG['TESTDBURI'])
    assert isinstance(client, MongoClient)
    db = client[TEST_CFG['DB']]
    assert isinstance(db, Database)
    collection = db[TEST_CFG['COLLECTION']]
    assert isinstance(collection, Collection)
    assert collection.count() == 2

    export_path = os.path.join(TEMP_DIR, 'tempfile123.csv')
    export_collection(
        collection=collection,
        output_fpath=export_path,
        fields=['user_id', 'name', 'height'],
        ftype='csv',
        verbose=True,
        auto=True,
    )

    df = pd.read_csv(export_path)
    assert len(df) == 2
    df1 = df[df['user_id'] == 1]
    df1.iloc[0]['name'] == 'Jack'
    df1.iloc[0]['height'] == 180
    df2 = df[df['user_id'] == 2]
    df2.iloc[0]['name'] == 'Jill'
    df2.iloc[0]['height'] == 185
