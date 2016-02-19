# coding=utf-8

"""
The KafkaConsumerLagCollector collects consumer lag metrics
using ConsumerOffsetChecker.

#### Dependencies

 * bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

"""

import diamond.collector


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

                        prefix_keys = metrics[:3]
                        value = float(metrics[5])

                        if cluster_name:
                            prefix_keys.insert(0, cluster_name)
                        self.publish('.'.join(prefix_keys), value)
                except Exception as e:
                    self.log.error(e)
