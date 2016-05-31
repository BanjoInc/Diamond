# coding=utf-8

"""
The KafkaNewConsumerLagCollector collects consumer lag metrics
using kafka-consumer-groups.sh. Compatible with 0.9 above

#### Dependencies

 * bin/kafka-consumer-groups.sh

"""

import diamond.collector


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
        prefix_keys = [self.consumer_group, self.topic, self.partition]
        if self.cluster_name:
            prefix_keys.insert(0, self.cluster_name)
        return '.'.join(prefix_keys)

    def get_consumer_lag(self):
        return int(self.consumer_lag)


class KafkaNewConsumerLagCollector(diamond.collector.ProcessCollector):
    def get_default_config_help(self):
        collector = super(KafkaNewConsumerLagCollector, self)
        config_help = collector.get_default_config_help()
        config_help.update({
            'bin': 'The path to kafka-consumer-groups.sh binary',
            'bootstrap_server': 'A comma separated kafka bootstrap servers',
            'cluster_name': 'Cluster name (Optional)'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(KafkaNewConsumerLagCollector, self).get_default_config()
        config.update({
            'path': 'kafka.ConsumerLag',
            'bin': '/opt/kafka/bin/kafka-consumer-groups.sh',
            'bootstrap_server': 'localhost:9092'
        })
        return config

    def collect(self):
        """
        Collect Kafka consumer lag metrics
        :return:
        """
        bootstrap_server = ','.join(self.config.get('bootstrap_server'))
        cluster_name = self.config.get('cluster_name', '').replace('/', '_').replace('-', '_')

        if not isinstance(bootstrap_server, list):
            bootstrap_server = [bootstrap_server]

        consumers = self.get_consumers(bootstrap_server)
        for consumer in consumers:
            try:
                cmd = [
                    '--bootstrap-server',
                    bootstrap_server,
                    '--new-consumer',
                    '--describe',
                    '--group',
                    consumer
                ]

                raw_output = self.run_command(cmd)
                if raw_output is None:
                    continue

                for i, output in enumerate(raw_output[0].split('\n')):
                    if i == 0:
                        continue

                    items = output.strip().split(', ')
                    metrics = [item for item in items if item]

                    if not metrics:
                        continue

                    metric = ConsumerMetric(cluster_name, metrics)
                    consumer_lag_metric_name = metric.get_consumer_lag_metric_name()
                    self.publish(consumer_lag_metric_name, metric.get_consumer_lag())
            except Exception as e:
                self.log.error(e)

    def get_consumers(self, bootstrap_server):
        """
        :param bootstrap_server:
        :return: All consumers
        """
        cmd = [
            '--bootstrap-server',
            bootstrap_server,
            '--list',
            '--new-consumer'
        ]
        consumers = []
        raw_output = self.run_command(cmd)
        if raw_output is None:
            return consumers
        for output in raw_output[0].split('\n'):
            consumers.append(output)
        return consumers
