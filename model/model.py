#!/usr/bin/env python3
# encoding: utf-8

import numpy as np

import model


class Model:

    def __init__(self):
        self.compute_nodes = list()
        # TODO: add performance counters e.g. how many cold starts, how many hot starts

    def add_compute_nodes(self, num_nodes=1, node_mem_mb=8192):
        # create compute node instances
        for i in range(num_nodes):
            self.compute_nodes.append(model.ComputeNode(i, mem_mb=node_mem_mb))

    def max_mem_avail_node_idx(self):
        return np.argmax([x.mem_available() for x in self.compute_nodes])

    def schedule(self, invocation):
        """
        The current implementation:
         - always schedules on node0
         - remove random function until enough memory is available
        TODO:
         (1) implement the scheduler from the paper
         (2) Improve e.g. different policy, reinforcement learning, ..
        """
        # make space if necessary (if the app already exists in memory -> we just need to update the metrics)
        if not self.compute_nodes[0].app_exists(invocation):
            while invocation["AverageMem"] > self.compute_nodes[0].mem_available():
                self.compute_nodes[0].remove_app(self.compute_nodes[0].get_random_app())
        # load app & function
        if not self.compute_nodes[0].function_exists(invocation):
            self.compute_nodes[0].add_function(invocation)
