# coding=utf-8

"""
The NvidiaGPUCollector collects GPU utilization metric using nvidia-smi.

See https://developer.nvidia.com/nvidia-system-management-interface

#### Dependencies

 * nvidia-smi

"""

from itertools import izip

import diamond.collector


class NvidiaGPUCollector(diamond.collector.ProcessCollector):

    def get_default_config_help(self):
        config_help = super(NvidiaGPUCollector, self).get_default_config_help()
        config_help.update({
            'bin': 'The path to the nvidia-smi binary',
            'stats': 'A list of Nvidia GPU stats to collect. '
                     'Use `nvidia-smi --help-query-gpu` for more information'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NvidiaGPUCollector, self).get_default_config()
        config.update({
            'path': 'nvidia',
            'bin': '/usr/bin/nvidia-smi',
            'stats': [
                'index',
                'memory.total',
                'memory.used',
                'memory.free',
                'utilization.gpu',
                'utilization.memory',
                'temperature.gpu'
            ]
        })
        return config

    def collect(self):
        """
        Collector GPU stats

        """

        stats_config = self.config['stats']
        raw_output = self.run_command([
            '--query-gpu={query_gpu}'.format(query_gpu=','.join(stats_config)),
            '--format=csv,nounits,noheader'
        ])

        if raw_output is None:
            return

        results = raw_output[0].strip().split("\n")
        for result in results:
            stats = result.strip().split(',')
            assert len(stats) == len(stats_config)
            index = stats[0]
            for stat_name, metric in izip(stats_config[1:], stats[1:]):
                metric_name = 'gpu_{index}.{stat_name}'.format(
                    index=str(index),
                    stat_name=stat_name
                )
                self.publish(metric_name, metric)
