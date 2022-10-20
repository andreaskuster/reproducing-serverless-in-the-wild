import matplotlib.pyplot as plt
import pandas as pd


def analysis_mem(df_mem, num_nodes=1, node_mem_mb=8192, type='available'): 
    """
    Args:
        df_mem (DataFrame): to draw memory plot
        type (str, optional): available or occupied. Defaults to 'available'.
    """
    fig, axs = plt.subplots(3)
    for i in range(num_nodes):
        axs[i].plot(df_mem[i])
        axs[i].set_ylabel(f"memory_{type}")
        axs[i].set_xlabel("times")
        axs[i].set_title(f"The {i} compute node's memory {type} changing curve.")
    plt.show()

# tmp = pd.DataFrame({0:[1,2,3,4]})
# analysis_mem(tmp)
