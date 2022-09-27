#!/usr/bin/env python3
# encoding: utf-8

import os
import sys
import tarfile

from urllib.request import urlretrieve
from urllib.parse import urlparse

import pandas as pd

import matplotlib.pyplot as plt


class Dataset:

    def __init__(self):
        self.data_uri = "https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz"

        # extract file name
        path = urlparse(self.data_uri)
        self.file_name = os.path.basename(path.path)

        # local vars
        self.path = os.path.dirname(__file__)
        self.data_path = os.path.join(self.path, "data")

        # data containers
        self.app_memory = pd.DataFrame()
        self.app_duration = pd.DataFrame()
        self.app_invocation = pd.DataFrame()

    def data_import(self, day_index=range(12)):
        if not os.path.exists(os.path.join(self.path, self.file_name)):
            self.fetch_data()
        if not os.path.exists(os.path.join(self.path, self.data_path)):
            self.extract_data()
        self.parse_data(day_index)

    def fetch_data(self):
        print(f"Downloading {self.file_name}")
        urlretrieve(self.data_uri, os.path.join(self.path, self.file_name))

    def extract_data(self):
        print(f"Extracting {self.file_name}")
        # open and extract file
        file = tarfile.open(self.file_name)
        try:
            file.extractall(self.data_path)
        except:
            pass
        file.close()

    def parse_data(self, day_index=range(12)):
        # TODO: we currently load ~2GB of data into memory, resulting n a memory consumption of ~4GB, which is
        #  suboptimal. Loading dataset for single days reduces the memory consumption to ~1.2GB
        for i in range(12):  # we omit 13, 14 as we have no data for app_memory
            if i in day_index:
                # index -> day
                day = i + 1

                # import new data from .csv file
                df_memory = pd.read_csv(os.path.join(self.data_path, f'app_memory_percentiles.anon.d{day:02d}.csv'))
                df_duration = pd.read_csv(
                    os.path.join(self.data_path, f'function_durations_percentiles.anon.d{i + 1:02d}.csv'))
                df_invocation = pd.read_csv(
                    os.path.join(self.data_path, f'invocations_per_function_md.anon.d{i + 1:02d}.csv'))

                # add day column
                df_memory["day"] = day
                df_duration["day"] = day
                df_invocation["day"] = day

                # concatenate with existing data
                self.app_memory = pd.concat([self.app_memory, df_memory], axis=0)
                self.app_duration = pd.concat([self.app_duration, df_duration], axis=0)
                self.app_invocation = pd.concat([self.app_invocation, df_invocation], axis=0)

        # match Mem usage and Function duration into the function invocation Dataframe
        self.app_invocation = pd.merge(self.app_invocation, self.app_memory[["HashApp", "AverageAllocatedMb"]],
                                       on="HashApp", how="inner")
        self.app_invocation.rename(columns={"AverageAllocatedMb": "AverageMem"}, inplace=True)
        self.app_invocation = pd.merge(self.app_invocation, self.app_duration[["HashFunction", "Average"]],
                                       on="HashFunction", how="inner")
        self.app_invocation.rename(columns={"Average": "AverageDuration"}, inplace=True)

    def get_function_invocations(self, day, time):
        # day [1..12], time [1, .., 1440]
        df = self.app_invocation[self.app_invocation["day"] == day].get(
            key=["HashApp", "HashFunction", str(time), "AverageMem", "AverageDuration"])
        return df

    def data_analysis(self):
        """"
        TODO: produce nice plots about input data (including var/mean/..):
            - histograms
            - changes between days
            - changes within a day
            - changes per Owner/App/Function
        """

        # input data
        df = self.app_invocation

        # define column array for all minute bins of a day
        column_list = [str(x + 1) for x in range(1440)]

        # plot distribution of invocations per function
        df["sum"] = df[column_list].sum(axis=1)
        df.hist(column='sum', bins=100)
        plt.yscale('log')
        plt.ylabel("functions")
        plt.xlabel("invocations")
        plt.title("Distribution of invocations per function")
        plt.show()

        # plot distribution of function duration
        df.hist(column='AverageDuration', bins=100)
        plt.yscale('log')
        plt.ylabel("functions")
        plt.xlabel("duration")
        plt.title("Distribution of duration per function")
        plt.show()


if __name__ == "__main__":
    dataset = Dataset()
    dataset.data_import(day_index=range(1))  # only load day zero (possible values: subset of [0, .., 11])
    dataset.data_analysis()
    sys.exit(0)
