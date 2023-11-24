import unittest
import sys
from io import StringIO
from unittest.mock import patch
from main import readInput, eval_query, print_table, databases

class TestDBMS(unittest.TestCase):

    def setUp(self):
       # Redirect stdout to capture print statements
       
       self.held_output = StringIO()
       self.original_stdout = sys.stdout
       sys.stdout = self.held_output

    def tearDown(self):
       # Restore stdout
       sys.stdout = self.original_stdout

    def run_query(self, query):
       with patch('builtins.input', return_value=query):
           eval_query()

    def test_create_table(self):
       print("\nRunning test_create_table...")
       query = "CREATE TABLE test_table (col1 INT, col2 STRING, primary_key (col1));"
       # Pass the query to the readInput function
       #read_input_result = readInput(query)
       self.run_query(query)
       output = self.held_output.getvalue().strip()
       self.assertEqual(output, "Query OK, 0 rows affected")
       print("Test test_create_table passed.")
    

    def test_show_table(self):
       print("\nRunning test_show_table...")
       query = "SHOW TABLES;"
       #read_input_result = readInput(query)
       self.run_query(query)
       output = self.held_output.getvalue().strip()
       self.assertEqual(output, "Tables\n-------\ntest_table")
       print("Test test_show_table passed.")
