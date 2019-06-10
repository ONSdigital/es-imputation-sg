import unittest.mock as mock
import sys, os
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import calculate_means_method 

class TestImputation:

  @mock.patch('calculate_means_method.boto3')
  def test_lambda_handler(self, mocked):
    response = calculate_means_method.lambda_handler("", None)
