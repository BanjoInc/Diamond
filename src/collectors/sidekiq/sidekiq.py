# coding=utf-8

"""
Collects sidekiq data from Redis

#### Dependencies

 * redis

"""

try:
    import redis
    from redis.sentinel import Sentinel
except ImportError:
    redis = None

import diamond.collector


class SidekiqCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(SidekiqCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': 'Redis hostname',
            'port': 'Redis port',
            'password': 'Redis Auth password',
            'dbs': 'Array of enabled DB Indexes',
            'sentinel_port': 'Redis sentinel port',
            'sentinel_name': 'Redis sentinel name',
            'cluster_prefix': 'Redis cluster name prefix'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SidekiqCollector, self).get_default_config()
        config.update({
            'path': 'sidekiq',
            'host': 'localhost',
            'port': 6379,
            'dbs': [0],
            'password': None,
            'sentinel_port': 26379,
            'sentinel_name': None,
            'cluster_prefix': None
        })
        return config

    def get_master(self, host, port, sentinel_port, sentinel_name):
        """
        :param host: Redis host to send request
        :param port: Redis port to send request
        :param sentinel_port: sentinel_port optional
        :param sentinel_name: sentinel_name optional
        :return: master ip and port
        """
        if sentinel_port and sentinel_name:
            master = Sentinel([(host, sentinel_port)], socket_timeout=1)\
                .discover_master(sentinel_name)
            return master
        return host, port

    def get_redis_client(self, db=0):
        """
        :param db: Redis database index
        :return: Redis client
        """
        host = self.config['host']
        port = self.config['port']
        sentinel_port = self.config['sentinel_port']
        sentinel_name = self.config['sentinel_name']
        password = self.config['password']
        master = self.get_master(host, port, sentinel_port, sentinel_name)
        pool = redis.ConnectionPool(
            host=master[0], port=master[1], password=password, db=db
        )
        return redis.Redis(connection_pool=pool)

    def collect(self):
        """
        Collect Sidekiq metrics
        :return:
        """
        if redis is None:
            self.log.error('Unable to import module redis')
            return {}

        for db in self.config['dbs']:
            redis_client = self.get_redis_client(db)
            self.publish_queue_length(redis_client, db)
            self.publish_schedule_length(redis_client, db)
            self.publish_retry_length(redis_client, db)

    def publish_schedule_length(self, redis_client, db):
        """
        :param redis_client: Redis client
        :param db: Redis Database index
        :return: Redis schedule length
        """
        schedule_length = redis_client.zcard('schedule')
        self.__publish(db, 'schedule', schedule_length)

    def publish_retry_length(self, redis_client, db):
        """
        :param redis_client: Redis client
        :param db: Redis Database index
        :return: Redis schedule length
        """
        retry_length = redis_client.zcard('retry')
        self.__publish(db, 'retry', retry_length)

    def publish_queue_length(self, redis_client, db):
        """
        :param redis_client: Redis client
        :param db: Redis Database index
        :return: Redis queue length
        """
        for queue in redis_client.smembers('queues'):
            queue_length = redis_client.llen(queue)
            self.__publish(db, queue, queue_length)

    def __publish(self, db, queue, queue_length):
        """
        :param db: Redis db index to report
        :param queue: Queue name to report
        :param queue_length: Queue length to report
        :return:
        """
        cluster = self.config['cluster_prefix']
        if cluster:
            metric_name = 'queue.{cluster}.{db}.{queue}'\
                .format(cluster=cluster, db=db, queue=queue)
        else:
            metric_name = 'queue.{db}.{queue}'.format(db=db, queue=queue)
        self.publish_gauge(name=metric_name, value=queue_length)
