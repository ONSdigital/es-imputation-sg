import boto3
import sys
import os
import imputation_calculate_movement
import unittest.mock as mock
import unittest
from moto import mock_s3
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__)+"/.."))

#
# class TestClass(object):
#     @mock_s3
#     def test_s3(self):
#         conn = boto3.resource('s3', region_name='eu-west-2')
#         conn.create_bucket(Bucket='mybucket')
#
#         s3 = boto3.client('s3', region_name='us-east-1')
#         s3.put_object(Bucket='mybucket', Key="test", Body="correct return!")
#
#         body = conn.Object('mybucket', 'test').get()['Body'].read().decode()
#
#         assert body == 'correct return!'
#
#     def test_input(self):
#         with mock.patch.dict(imputation_calculate_movement.os.environ, {
#             'current_period': '201809',
#             'previous_period': '201806',
#             'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel Q607_constructional_fill'
#         }):
#             assert imputation_calculate_movement.lambda_handler(None, None) is not None


class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_sorted_current_contains_values(self):
        response = imputation_calculate_movement.lambda_handler(input, None)

# If input_dataframe.count() < 1 - {throw new expection}