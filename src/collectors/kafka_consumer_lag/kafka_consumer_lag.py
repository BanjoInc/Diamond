# coding=utf-8

"""
The KafkaConsumerLagCollector collects consumer lag metrics
using ConsumerOffsetChecker.

#### Dependencies

 * bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker for Kafka 0.9 below
 * bin/kafka-consumer-groups.sh for Kafka 0.9 above
"""

import diamond.collector

from distutils.version import LooseVersion


class ConsumerMetric(object):
    def __init__(self, cluster_name, metrics):
        super(ConsumerMetric, self).__init__()
        self.cluster_name = cluster_name
        self.consumer_group, \
        self.topic, \
        self.partition, \
        self.consumer_offset, \
        self.broker_offset, \
        self.consumer_lag, \
        _ = metrics

    def get_consumer_lag_metric_name(self):
        if self.cluster_name:
            prefix_keys = [self.cluster_name, self.consumer_group, self.topic, self.partition]
        else:
            prefix_keys = [self.consumer_group, self.topic, self.partition]
        return '.'.join(prefix_keys)

    def get_consumer_lag(self):
        return int(self.consumer_lag)


class KafkaConsumerLagCollector(diamond.collector.ProcessCollector):
    def get_default_config_help(self):
        collector = super(KafkaConsumerLagCollector, self)
        config_help = collector.get_default_config_help()
        config_help.update({
            'bin': 'The path to kafka-run-class.sh binary for Kafka 0.8.x '
                   'and kafka-consumer-groups.sh binary for Kafka 0.9.x',
            'topics': 'Comma-separated list of consumer topics.',
            'zookeeper': 'ZooKeeper connect string.',
            'consumer_groups': 'Consumer groups',
            'kafka_version': 'Kafka version (Default: 0.8.2.0)'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(KafkaConsumerLagCollector, self).get_default_config()
        config.update({
            'path': 'kafka.ConsumerLag',
            'bin': '/opt/kafka/bin/kafka-run-class.sh',
            'zookeeper': 'localhost:2181',
            'kafka_version': '0.8.2.0'
        })
        return config

    def get_total_lag_metric_name(self, cluster_name, consumer_group, topic):
        """
        Return formatted total consumer lag metric name
        :return:
        """
        if cluster_name:
            prefix_keys = [cluster_name, consumer_group, topic, 'total']
        else:
            prefix_keys = [consumer_group, topic, 'total']
        return '.'.join(prefix_keys)

    def collect(self):
        """
        Collect Kafka consumer lag metrics
        :return:
        """
        zookeeper = self.config.get('zookeeper')
        if isinstance(zookeeper, list):
            zookeeper = ','.join(zookeeper)

        cluster_name = '-'.join(zookeeper.split('/')[1:]).replace('-', '_')
        is_0_9_above = LooseVersion(self.config.get('kafka_version')) >= LooseVersion('0.9.0.0')

        consumer_groups = self.get_consumer_groups(zookeeper, is_0_9_above)

        if is_0_9_above:
            separator = ', '
        else:
            separator = ' '

        for consumer_group in consumer_groups:
            try:
                cmd = self.get_command(consumer_group, zookeeper, is_0_9_above)

                raw_output = self.run_command(cmd)
                if raw_output is None:
                    return

                total_lag = 0
                topic = None
                for i, output in enumerate(raw_output[0].split('\n')):
                    if i == 0:
                        continue

                    items = output.strip().split(separator)
                    metrics = [item for item in items if item]

                    if not metrics:
                        continue

                    metric = ConsumerMetric(cluster_name, metrics)
                    total_lag += metric.get_consumer_lag()
                    consumer_lag_metric_name = metric.get_consumer_lag_metric_name()
                    self.publish(consumer_lag_metric_name, metric.get_consumer_lag())
                    if topic is None:
                        topic = metric.topic

                total_lag_metric_name = self.get_total_lag_metric_name(cluster_name, consumer_group, topic)
                self.publish(total_lag_metric_name, total_lag)
            except Exception as e:
                self.log.error(e)

    def get_command(self, consumer_group, zookeeper, is_0_9_above):
        """
        :param consumer_group:
        :param zookeeper:
        :param is_0_9_above:
        :return: Command for 0.8.x or 0.9.x based on versions
        """
        if is_0_9_above:
            cmd = [
                '--group',
                consumer_group,
                '--zookeeper',
                zookeeper,
                '--describe'
            ]
        else:
            cmd = [
                'kafka.tools.ConsumerOffsetChecker',
                '--group',
                consumer_group,
                '--zookeeper',
                zookeeper
            ]
        return cmd

    def get_consumer_groups(self, zookeeper, is_0_9_above):
        """
        Get consumer groups from config or kafka (0.9 above)
        :return: Consumer groups
        """
        consumer_groups = self.config.get('consumer_groups')
        if is_0_9_above and consumer_groups is None:
            consumer_groups = []
            cmd = [
                '--zookeeper',
                zookeeper,
                '--list'
            ]
            raw_output = self.run_command(cmd)
            if raw_output is not None:
                for output in raw_output[0].split('\n'):
                    if not output:
                        continue
                    consumer_groups.append(output)

        if not isinstance(consumer_groups, list):
            consumer_groups = [consumer_groups]
        return consumer_groups
