import json
import csv
import sqlite3
import trans_sql
import time
from main import main


def test_q2():
    results2 = main()
    assert len(results2) == 2146
