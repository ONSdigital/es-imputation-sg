import unittest.mock as mock
import sys, os
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import calculate_means_method as cmm

class TestImputation:

  @mock.patch.object('calculate_means_method', 'boto3')
  def test_lambda_handler():
    cmm.lambda_handler("", None)
