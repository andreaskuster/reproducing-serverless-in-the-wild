from math import ceil
import pandas as pd
import numpy as np
from welford import Welford
import pmdarima as pm



class Controller:

    def __init__(self, method, keep_alive_period=3, CV_threshold=2, OOB_threshold=3, history_saved=10, RANGE_OF_HISTOGRAM = 240, PW = 5.0, KA = 99.0):
        self.method = method

        self.keep_alive_period = keep_alive_period
        self.CV_threshold = CV_threshold
        self.OOB_threshold = OOB_threshold
        self.history_saved = history_saved
        self.PW = PW
        self.KA = KA
        self.RANGE_OF_HISTOGRAM = RANGE_OF_HISTOGRAM

        # idx is to record the index where we store the distribution and the arrival history for the app. 
        self.histogram = pd.DataFrame(columns=["HashApp", "LastUsed", "OOB_times", "idx"])
        self.distribution = list()
        self.ArrivalHistory = list()

    def set_window(self, invocation, time):
        if self.method == 'keep_alive':
            pre_warm_window, keep_alive_window = self.set_window_keep_alive(invocation)
        elif self.method == 'hybrid':
            pre_warm_window, keep_alive_window = self.set_window_hybrid(invocation, time)
        elif self.method == 'reinfored':
            pre_warm_window, keep_alive_window = self.set_window_reinforeced(invocation)
        else:
            raise NotImplementedError
        invocation["pre_warm_window"] = pre_warm_window
        invocation["keep_alive_window"] = keep_alive_window
        invocation["arrival_time"] = time
        return invocation

    def set_window_keep_alive(self, invocation):
        pre_warm_window = 0
        return pre_warm_window, self.keep_alive_period

    def set_window_hybrid(self, invocation, time):
        self.update_distribution(invocation, time)
        if self.judge_out_of_bounds(invocation):
            pre_warm_window, keep_alive_window = self.time_series_forecast(invocation)
        elif self.judge_histogram_representative(invocation):
            pre_warm_window, keep_alive_window = self.set_window_from_distribution(invocation)
        else:
            pre_warm_window, keep_alive_window = self.set_window_keep_alive(invocation)
        return pre_warm_window, keep_alive_window
    
    def set_window_reinforeced(self, invocation):
        return NotImplementedError
    
    def set_window_from_distribution(self, invocation):
        pos = invocation["HashApp"] == self.histogram["HashApp"]
        distribution = self.distribution[self.histogram.loc[pos, "idx"][0]]
        cal = distribution[distribution > 0]
        perc = np.percentile(cal, np.array([self.PW, self.KA]))   # find the index (interval)
        for i in range(len(distribution)):
            if np.sum(distribution[:i+1]) >= perc[0]:
                low = np.max([0, i-1])
                break
        for i in range(len(distribution)):
            if np.sum(distribution[:i+1]) >= perc[1]:
                high = i - low
                break
        pre_warm_window, keep_alive_window = np.int64(low) * 0.9, max(np.ceil(high), 1) * 1.1
        # print(int(pre_warm_window), np.ceil(keep_alive_window))
        return int(pre_warm_window), np.ceil(keep_alive_window)    

    def update_distribution(self, invocation, time):
        if not self.histogram_exist(invocation):
            df = pd.DataFrame({
            "HashApp": [invocation["HashApp"]],
            "LastUsed": [time],
            "OOB_times": [0],
            "idx": len(self.histogram)
            })
            self.histogram = pd.concat([self.histogram, df], axis=0)
            self.ArrivalHistory.append([time])
            self.distribution.append(np.zeros(self.RANGE_OF_HISTOGRAM))
        else:
            pos = invocation["HashApp"] == self.histogram["HashApp"]
            idle_time = time - self.histogram.loc[pos, "LastUsed"][0]
            idx = self.histogram.loc[pos, "idx"][0]
            if idle_time < self.RANGE_OF_HISTOGRAM:
                self.distribution[idx][idle_time] += 1
            else:
                self.histogram.loc[pos, "OOB_times"] += 1
            self.histogram.loc[pos, "LastUsed"] = time
            
            history = self.ArrivalHistory[idx]
            history.append(idle_time)
            if len(history) > self.history_saved:
                del history[0]  # remove the first saved data once detected oversize.
            self.ArrivalHistory[idx] = history
        return

    def histogram_exist(self, invocation):
        return invocation["HashApp"] in self.histogram["HashApp"].values

    def judge_out_of_bounds(self, invocation):
        pos = invocation["HashApp"] == self.histogram["HashApp"]
        return  self.histogram.loc[pos, "OOB_times"][0] > self.OOB_threshold
    
    def time_series_forecast(self, invocation):
        "https://blog.csdn.net/lam_yx/article/details/107887284 for pm.auto_arima"
        pos = invocation["HashApp"] == self.histogram["HashApp"]
        history = self.ArrivalHistory[self.histogram.loc[pos, "idx"]]
        arima = pm.auto_arima(history)
        next_idle_time = arima.predict(1)
        pre_warm_window, keep_alive_window = next_idle_time * 0.85, next_idle_time * 0.15 * 2
        return pre_warm_window, keep_alive_window

    def judge_histogram_representative(self, invocation):

        #TODO: turn to welford algorithm. Now just use the normal method. 
        # Initialize Welford object
        # w = Welford()
        
        # pos = invocation["HashApp"] == self.histogram["HashApp"]

        # w.add(np.reshape(self.distribution[self.histogram.loc[pos, "idx"][0]], [-1, 1]))
        # CV = w.var_s/w.mean

        pos = invocation["HashApp"] == self.histogram["HashApp"]
        dis = self.distribution[self.histogram.loc[pos, "idx"][0]]
        if dis.mean() < 1e-9:
            return False
        else:
            CV = dis.var()/dis.mean()
        return CV > self.CV_threshold
    