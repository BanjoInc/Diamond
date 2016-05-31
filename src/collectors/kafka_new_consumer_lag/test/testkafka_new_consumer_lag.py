#!/usr/bin/python
# coding=utf-8
##########################################################################

from test import CollectorTestCase
from test import get_collector_config
from mock import patch, Mock

from diamond.collector import Collector
from kafka_new_consumer_lag import KafkaNewConsumerLagCollector


##########################################################################


class TestKafkaNewConsumerLagCollector(CollectorTestCase):
    def setUp(self):
        self.collector = KafkaNewConsumerLagCollector()

    def test_import(self):
        self.assertTrue(KafkaNewConsumerLagCollector)

    @patch.object(Collector, 'publish')
    def test_should_publish_kafka_consumer_lag_stats(self, publish_mock):
        output_mock = Mock(
            return_value=(self.getFixture('consumer_lag_check').getvalue(), '')
        )
        collector_mock = patch.object(
            KafkaNewConsumerLagCollector,
            'run_command',
            output_mock
        )
        patch_collector = patch.object(
            KafkaNewConsumerLagCollector,
            'get_consumers',
            Mock(return_value=['stage_nginx_access'])
        )

        patch_collector.start()
        collector_mock.start()
        self.collector.collect()
        collector_mock.stop()
        patch_collector.stop()

        metrics = {
            'stage_nginx_access.nginx_access.0': 1,
            'stage_nginx_access.nginx_access.1': 10,
            'stage_nginx_access.nginx_access.2': 52
        }

        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['path'])
        self.assertPublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_should_publish_kafka_consumer_lag_stats(self, publish_mock):
        self.collector.config.update({
            'cluster_name': 'dev/test-01'
        })
        output_mock = Mock(
            return_value=(self.getFixture('consumer_lag_check').getvalue(), '')
        )
        collector_mock = patch.object(
            KafkaNewConsumerLagCollector,
            'run_command',
            output_mock
        )
        patch_collector = patch.object(
            KafkaNewConsumerLagCollector,
            'get_consumers',
            Mock(return_value=['stage_nginx_access'])
        )

        patch_collector.start()
        collector_mock.start()
        self.collector.collect()
        collector_mock.stop()
        patch_collector.stop()

        metrics = {
            'dev_test_01.stage_nginx_access.nginx_access.0': 1,
            'dev_test_01.stage_nginx_access.nginx_access.1': 10,
            'dev_test_01.stage_nginx_access.nginx_access.2': 52
        }
        self.assertPublishedMany(publish_mock, metrics)
