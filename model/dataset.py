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
        self.app_memory = list()
        self.app_duration = list()
        self.app_invocation = list()

    def data_import(self, day_index=range(12)):
        if not os.path.exists(self.file_name):
            self.fetch_data()
        if not os.path.exists(self.data_path):
            self.extract_data()
        self.parse_data(day_index)

    def fetch_data(self):
        print(f"Downloading {self.file_name}")
        urlretrieve(self.data_uri, self.file_name)

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
                self.app_memory.append(
                    pd.read_csv(os.path.join(self.data_path, f'app_memory_percentiles.anon.d{i + 1:02d}.csv')))
                self.app_duration.append(
                    pd.read_csv(os.path.join(self.data_path, f'function_durations_percentiles.anon.d{i + 1:02d}.csv')))
                self.app_invocation.append(
                    pd.read_csv(os.path.join(self.data_path, f'invocations_per_function_md.anon.d{i + 1:02d}.csv')))
            else:
                # add placeholder for index == day
                self.app_memory.append(None)
                self.app_duration.append(None)
                self.app_invocation.append(None)


    def get_app_data(self):

        df = self.app_invocation[0]#.head(n=1000)

        # self.app_invocation[0].head().hist(column=[str(i+1) for i in range(10)]) # 1440
        # df.plot(kind='hist')

        # remove non-numbers

        column_list = list(df)

        print(column_list)

        column_list.remove("HashOwner")
        column_list.remove("HashApp")
        column_list.remove("HashFunction")
        column_list.remove("Trigger")

        df["sum"] = df[column_list].sum(axis=1)

        df.hist(column='sum', bins=10)

        plt.show()






if __name__ == "__main__":

    dataset = Dataset()
    dataset.data_import(day_index=[0])  # only load day zero (possible values: subset of [0, .., 11])

    dataset.get_app_data()

    sys.exit(0)
