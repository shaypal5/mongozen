
from unittest import TestCase

from mongozen.matchop import Matchop


class TestMatchopAnd(TestCase):
    """Check that & operator between a Matchop and any dict works correctly."""

    def test_matchop_and_1(self):
        """Tests Matchop and operator."""
        print("Test 1")
        res = Matchop({'a': {'$gt': 4}}) & {'a': {'$gt': 5}}
        expected = {'a': {'$gt': 5}}
        self.assertEqual(res, expected)

    def test_matchop_and_2(self):
        """Tests Matchop and operator."""
        print("Test 2")
        res = Matchop({'a': {'$gt': 4}}) & {'a': {'$gte': 5}}
        expected = {'a': {'$gte': 5}}
        self.assertEqual(res, expected)

    def test_matchop_and_3(self):
        """Tests Matchop and operator."""
        print("Test 3")
        res = Matchop({'a': {'$gt': 5}}) & {'a': {'$gte': 5}}
        expected = {'a': {'$gt': 5}}
        self.assertEqual(res, expected)

    def test_matchop_and_4(self):
        """Tests Matchop and operator."""
        print("Test 4")
        res = Matchop({'a': {'$gt': 5}}) & {'a': {'$lte': 8}}
        expected = {'a': {'$gt': 5, '$lte': 8}}
        self.assertEqual(res, expected)

    def test_matchop_and_5(self):
        """Tests Matchop and operator."""
        print("Test 5")
        res = Matchop({'a': {'$gt': 5}}) & {'a': {'$lte': 8}}
        expected = {'a': {'$gt': 5, '$lte': 8}}
        self.assertEqual(res, expected)

    def test_matchop_and_6(self):
        """Tests Matchop and operator."""
        print("Test 6")
        res = Matchop({'a': {'$lt': 9}}) & {'a': {'$lte': 8}}
        expected = {'a': {'$lte': 8}}
        self.assertEqual(res, expected)

    def test_matchop_and_7(self):
        """Tests Matchop and operator."""
        print("Test 7")
        res = Matchop({'a': {'$eq': 9}}) & {'a': {'$lte': 8}}
        expected = {'a': {'$eq': 9, '$lte': 8}}
        self.assertEqual(res, expected)
