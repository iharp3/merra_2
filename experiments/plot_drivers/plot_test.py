import pandas as pd
import matplotlib.pyplot as plt
import sys

def plot_files(filepaths, labels=None, alphas=["s", "^", "o"]):
    # Read data from each file
    dataframes = [pd.read_csv(f) for f in filepaths]
    
    # Determine all unique t_res values across all files
    unique_t_res = sorted(set().union(*[set(df['t_res'].unique()) for df in dataframes]))

    # Create a plot for each unique t_res
    for t in unique_t_res:
        plt.figure(figsize=(8, 5))
        for idx, df in enumerate(dataframes):
            label = labels[idx] if labels else f'File {idx+1}'
            subset = df[df['t_res'] == t]
            sub = subset.groupby(['s_res'])['total_time'].mean().reset_index()
            # total_t = subset.groupby(['total_time'])['s_res'].mean().reset_index()
            plt.plot(sub['s_res'], sub['total_time'], marker=alphas[idx], label=label, fillstyle='none')

        plt.title(f"t_res = {t}")
        plt.xlabel("s_res")
        plt.ylabel("total_time")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"test_plot_{t}.png")
        plt.show()

if __name__ == "__main__":
    # Example usage with three file paths
    file1 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions_one_file.csv"
    file2 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions_yearly_files.csv"
    file3 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions.csv"
    
    # Optionally add custom labels
    labels = ["One File", "Yearly Files", "Mixed"]
    
    plot_files([file1, file2, file3], labels)
