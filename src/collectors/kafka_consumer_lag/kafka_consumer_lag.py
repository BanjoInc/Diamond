# coding=utf-8

"""
The KafkaConsumerLagCollector collects consumer lag metrics
using ConsumerOffsetChecker.

#### Dependencies

 * bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

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
        if self.cluster_name:
            prefix_keys = [self.cluster_name, self.consumer_group, self.topic, self.partition]
        else:
            prefix_keys = [self.consumer_group, self.topic, self.partition]
        return '.'.join(prefix_keys)

    def get_total_lag_metric_name(self):
        if self.cluster_name:
            prefix_keys = [self.cluster_name, self.consumer_group, self.topic, 'total']
        else:
            prefix_keys = [self.consumer_group, self.topic, 'total']
        return '.'.join(prefix_keys)

    def get_consumer_lag(self):
        return int(self.consumer_lag)


class KafkaConsumerLagCollector(diamond.collector.ProcessCollector):

    def get_default_config_help(self):
        collector = super(KafkaConsumerLagCollector, self)
        config_help = collector.get_default_config_help()
        config_help.update({
            'bin': 'The path to kafka-run-class.sh binary',
            'topics': 'Comma-separated list of consumer topics.',
            'zookeeper': 'ZooKeeper connect string.',
            'consumer_groups': 'Consumer groups'
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
            'zookeeper': 'localhost:2181'
        })
        return config

    def get_topics(self, zookeeper):
        """
        :param zookeeper:
        :return: All Kafka topics
        """
        topics = self.config.get('topics')
        if not topics:
            topics = []
            cmd = [
                'kafka.admin.TopicCommand',
                '--list',
                '--zookeeper',
                zookeeper
                ]

            raw_output = self.run_command(cmd)
            if raw_output is None:
                return []
            for output in raw_output[0].split('\n'):
                if not output.strip() or '__consumer_offsets' in output or 'marked for deletion' in output:
                    continue

                topics.append(output)
        elif isinstance(topics, list):
            topics = [topics]
        return topics

    def collect(self):
        """
        Collect Kafka consumer lag metrics
        :return:
        """
        zookeeper = ','.join(self.config.get('zookeeper'))
        consumer_groups = self.config.get('consumer_groups')
        cluster_name = '-'.join(zookeeper.split('/')[1:]).replace('-', '_')

        if not isinstance(consumer_groups, list):
            consumer_groups = [consumer_groups]

        topics = self.get_topics(zookeeper)
        for topic in topics:
            consumer_metrics = []
            for consumer_group in consumer_groups:
                try:
                    cmd = [
                        'kafka.tools.ConsumerOffsetChecker',
                        '--group',
                        consumer_group,
                        '--topic',
                        topic,
                        '--zookeeper',
                        zookeeper
                        ]

                    raw_output = self.run_command(cmd)
                    if raw_output is None:
                        return

                    for i, output in enumerate(raw_output[0].split('\n')):
                        if i == 0:
                            continue

                        items = output.strip().split(' ')
                        metrics = [item for item in items if item]

                        if not metrics:
                            continue

                        metric = ConsumerMetric(cluster_name, metrics)
                        consumer_lag_metric_name = metric.get_consumer_lag_metric_name()
                        self.publish(consumer_lag_metric_name, metric.get_consumer_lag())

                        consumer_metrics.append(metric)
                except Exception as e:
                    self.log.error(e)

            if consumer_metrics:
                total_lag = 0
                total_lag_metric_name = None
                for consumer_metric in consumer_metrics:
                    if total_lag_metric_name is None:
                        total_lag_metric_name = consumer_metric.get_total_lag_metric_name()
                    total_lag += consumer_metric.get_consumer_lag()
                self.publish(total_lag_metric_name, total_lag)
