import pandas
import unittest

from custom_libs.SQLUtils import SQLUtils
from custom_libs.SQLUtils import SQLEngine

class SQLEngineTest(unittest.TestCase):

    def setUp(self):
        self.con = SQLEngine("sql_practice")

    def testGet(self):
        self.members = self.con.execute("SELECT * FROM cd.members LIMIT 10")
        assertEqual(len(self.members), 10)

    def tearDown(self):
        self.con.dispose()


class SQLUtilsTest(unittest.TestCase):

    def setUp(self):
        self.utils = SQLUtils("sql_practice")

    def testExec(self):
        self.members = self.execute("SELECT * FROM cd.members WHERE firstname = 'Tim'")
        assertEqual(len(self.members), 2)

    def testGet(self):
        self.members = self.get(table_name="cd.members", cols=None, limit_value=10, where_col="firstname", where_value="Tim", where_equal="=")
        assertEqual(len(self.members), 2)


if __name__ == '__main__':
    unittest.main()

