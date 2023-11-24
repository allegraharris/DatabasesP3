import unittest
import sys
from io import StringIO
from unittest.mock import patch
from main import readInput, eval_query, print_table, databases, filter

print("Set up")
class TestDBMS(unittest.TestCase):

    def setUp(self):
       # Redirect stdout to capture print statements
       self.held_output = StringIO()

    def tearDown(self):
       # Restore stdout
       self.held_output.close()

    @patch('sys.stdout', new_callable=StringIO)
    def test_create_table(self, mock_stout):
        global sql_query 
        sql_query = "CREATE TABLE test_table (col1 INT, col2 STRING, primary key (col1));"
        filter()
        print_table(eval_query())
        output = mock_stout.getvalue().strip()
        self.assertEqual(output, "Query OK, 0 rows affected")
    

    @patch('sys.stdout', new_callable=StringIO)
    def test_show_table(self, mock_stdout):
        global sql_query
        sql_query = "SHOW TABLES;"
        filter()
        eval_query()
        output = mock_stdout.getvalue().strip()
        self.assertEqual(output, "Tables\n-------\ntest_table")

if __name__ == '__main__':
    unittest.main()