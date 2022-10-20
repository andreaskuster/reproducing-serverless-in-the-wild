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

        # analysis parameters
        self.savefig = True

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
        print(f'Parsing {self.file_name}')
        for i in day_index:  # we omit 13, 14 as we have no data for app_memory
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

        # cleanup and reduce input data before join (solves out-of-memory issue)
        # application memory
        self.app_memory = self.app_memory.drop_duplicates(subset=['HashApp'])
        self.app_memory = self.app_memory[
            ["HashApp", "AverageAllocatedMb", "AverageAllocatedMb_pct1", "AverageAllocatedMb_pct100"]]
        # application duration
        self.app_duration = self.app_duration.drop_duplicates(subset=['HashFunction'])
        self.app_duration = self.app_duration[["HashFunction", "Average", "Minimum", "Maximum"]]

        # delete old df
        del df_memory
        del df_duration
        del df_invocation

        # match Mem usage and Function duration into the function invocation Dataframe
        self.app_invocation = pd.merge(self.app_invocation, self.app_memory, on="HashApp", how="inner")
        self.app_invocation.rename(columns={"AverageAllocatedMb": "AverageMem"}, inplace=True)
        self.app_invocation = pd.merge(self.app_invocation, self.app_duration, on="HashFunction", how="inner")

        self.app_invocation.rename(
            columns={"Average": "AverageDuration", "Minimum": "MinimumDuration", "Maximum": "MaximumDuration"},
            inplace=True)

    def get_function_invocations(self, day, time):
        # day [1..12], time [1, .., 1440]
        df = self.app_invocation[(self.app_invocation['day']==day) &
            (self.app_invocation[str(time)]!=0)].get(
                key=["HashApp", "HashFunction", str(time), "AverageMem", "AverageDuration"])
        # print(df)
        return df

    def plot_interv_between_invocations(self):
        # input data
        df = self.app_invocation

        # define column array for all minute bins of a day
        column_list = [str(x + 1) for x in range(1440)]

        # plot distribution of function duration
        _, _, patches0 = plt.hist(x=df[column_list].apply(func=lambda x: x.isin([0]).sum() / (~x.isin([0])).sum()),
                                  cumulative=True, histtype='step', bins=1000, density=1, label="average")

        # fix: just delete the last point, which was at y=0
        patches0[0].set_xy(patches0[0].get_xy()[:-1])
        # end fix

        # plt.xscale('log')
        # plt.legend()

        plt.ylabel("CDF")
        plt.xlabel("Interval between invocations [min]")
        plt.title("Distribution of interval between invocations")

        if self.savefig:
            plt.savefig("plot_interv_between_invocations.pdf")
        else:
            plt.show()

    def plot_mem_per_app(self):
        # input data
        df = self.app_invocation

        # plot distribution of function duration
        _, _, patches0 = plt.hist(x=df['AverageMem'], cumulative=True, histtype='step', bins=1000, density=1,
                                  label="average")

        _, _, patches1 = plt.hist(x=df['AverageAllocatedMb_pct1'], cumulative=True, histtype='step', bins=1000,
                                  density=1, label="1st percentile")
        _, _, patches2 = plt.hist(x=df['AverageAllocatedMb_pct100'], cumulative=True, histtype='step', bins=1000,
                                  density=1, label="max")
        # fix: just delete the last point, which was at y=0
        patches0[0].set_xy(patches0[0].get_xy()[:-1])
        patches1[0].set_xy(patches1[0].get_xy()[:-1])
        patches2[0].set_xy(patches2[0].get_xy()[:-1])
        # end fix

        plt.xscale('log')
        plt.legend()

        plt.ylabel("CDF")
        plt.xlabel("Allocated Memory [MB]")
        plt.title("Distribution of memory per application")

        if self.savefig:
            plt.savefig("plot_mem_per_app.pdf")
        else:
            plt.show()

    def plot_duration_per_fn(self):
        # input data
        df = self.app_invocation

        # plot distribution of function duration
        _, _, patches0 = plt.hist(x=df['AverageDuration'], cumulative=True, histtype='step', bins=10000000, density=1,
                                  label="average")
        _, _, patches1 = plt.hist(x=df['MinimumDuration'], cumulative=True, histtype='step', bins=10000000, density=1,
                                  label="min")
        _, _, patches2 = plt.hist(x=df['MaximumDuration'], cumulative=True, histtype='step', bins=10000000, density=1,
                                  label="max")

        # fix: just delete the last point, which was at y=0
        patches0[0].set_xy(patches0[0].get_xy()[:-1])
        patches1[0].set_xy(patches1[0].get_xy()[:-1])
        patches2[0].set_xy(patches2[0].get_xy()[:-1])
        # end fix

        plt.xscale('log')
        plt.legend()

        plt.ylabel("CDF")
        plt.xlabel("Time duration [ms]")
        plt.title("Distribution of duration per function")

        if self.savefig:
            plt.savefig("plot_duration_per_fn.pdf")
        else:
            plt.show()

    def plot_fn_per_app(self):
        # functions per app
        df = self.app_invocation

        # define column array for all minute bins of a day
        column_list = [str(x + 1) for x in range(1440)]

        # count trigger types
        df['HashApp'] = df['HashApp'].astype("category")

        # .cat.categories
        df_apps = pd.DataFrame()
        for cat in df['HashApp'].cat.categories:
            df_apps = df_apps.append({'HashApp': cat,
                                      'FunctionCount': df.loc[df["HashApp"] == cat].shape[0],  # #rows
                                      'InvocationCount': df.loc[df["HashApp"] == cat][column_list].sum(axis=1).sum(
                                          axis=0)}, ignore_index=True)

        _, _, patches0 = plt.hist(x=df_apps['FunctionCount'], cumulative=True, histtype='step', bins=10000000,
                                  density=1, label="Function")
        _, _, patches1 = plt.hist(x=df_apps['InvocationCount'], cumulative=True, histtype='step', bins=10000000,
                                  density=1, label="Invocation")

        # fix: just delete the last point, which was at y=0
        patches0[0].set_xy(patches0[0].get_xy()[:-1])
        patches1[0].set_xy(patches1[0].get_xy()[:-1])
        # end fix

        plt.xscale('log')
        plt.legend()

        plt.ylabel("CDF")
        plt.xlabel("Functions / invocations")
        plt.title("Functions and invocations per application")

        if self.savefig:
            plt.savefig("plot_fn_per_app.pdf")
        else:
            plt.show()

    def plot_trigger_events(self):
        df = self.app_invocation

        # define column array for all minute bins of a day
        column_list = [str(x + 1) for x in range(1440)]

        # count trigger types
        df['Trigger'] = df['Trigger'].astype("category")

        # .cat.categories
        df_types = pd.DataFrame()
        for cat in df['Trigger'].cat.categories:
            df_types = df_types.append({'Trigger': cat,
                                        'FunctionCount': df.loc[df["Trigger"] == cat].shape[0],  # #rows
                                        'InvocationCount': df.loc[df["Trigger"] == cat][column_list].sum(axis=1).sum(
                                            axis=0)}, ignore_index=True)
        total = df_types['FunctionCount'].sum(axis=0)
        df_types['FunctionRelative'] = df_types['FunctionCount'] / total

        total = df_types['InvocationCount'].sum(axis=0)
        df_types['InvocationRelative'] = df_types['InvocationCount'] / total

        fig, axs = plt.subplots(2)
        fig.suptitle('Trigger Events')
        axs[0].bar(df_types['Trigger'], df_types['FunctionRelative'])
        axs[0].set(ylabel="% Functions")
        axs[1].bar(df_types['Trigger'], df_types['InvocationRelative'])
        axs[1].set(ylabel="% Invocations")

        axs[0].set(xticklabels=[])
        plt.xlabel("Trigger")

        plt.xticks(rotation=15)

        if self.savefig:
            plt.savefig("plot_trigger_events.pdf")
        else:
            plt.show()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--plot_trigger_events', action='store_true')
    parser.add_argument('--plot_fn_per_app', action='store_true')
    parser.add_argument('--plot_interv_between_invocations', action='store_true')
    parser.add_argument('--plot_mem_per_app', action='store_true')
    parser.add_argument('--plot_duration_per_fn', action='store_true')

    args = parser.parse_args()

    dataset = Dataset()
    dataset.data_import(day_index=range(12))  # only load day zero (possible values: subset of [0, .., 11])

    """"
    Produce nice plots about input data (including var/mean/..):
        - histograms
        - changes between days
        - changes within a day
        - changes per Owner/App/Function
    """

    if args.plot_trigger_events:
        print("plot_trigger_events")
        dataset.plot_trigger_events()

    if args.plot_fn_per_app:
        print("plot_fn_per_app")
        dataset.plot_fn_per_app()

    if args.plot_interv_between_invocations:
        print("plot_interv_between_invocations")
        dataset.plot_interv_between_invocations()

    if args.plot_mem_per_app:
        print("plot_mem_per_app")
        dataset.plot_mem_per_app()

    if args.plot_duration_per_fn:
        print("plot_duration_per_fn")
        dataset.plot_duration_per_fn()

    sys.exit(0)
