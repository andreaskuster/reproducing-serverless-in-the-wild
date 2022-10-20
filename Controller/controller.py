import pandas as pd


class Controller:

    def __init__(self, method):
        self.method = method
    
    def set_window(self, invocation):
        pre_warm_window = 0
        keep_alive_window = 0
        if self.method is 'keep_alive':
            self.set_window_keep_alive(invocation)
        elif self.method is 'hybrid':
            self.set_window_hybrid(invocation)
        elif self.method is 'reinfored':
            self.set_window_reinforeced(invocation)
        else:
            raise NotImplementedError
        return pre_warm_window, keep_alive_window

    def set_window_keep_alive(self, invocation):
        return NotImplementedError

    def set_window_hybrid(self, invocation):
        return NotImplementedError
    
    def set_window_reinforeced(self, invocation):
        return NotImplementedError

    def update_distribution(self, invocation):
        return NotImplementedError
    
    def time_series_forecast(self, invocation):
        return NotImplementedError
    



    