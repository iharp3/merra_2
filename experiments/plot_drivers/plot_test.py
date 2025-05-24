'''
Edited from github repo: https://github.com/iharp3/experiment-kit/blob/main/round2/figs/code/5cplot.py
    Accessed: May 23, 2025
'''

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# Load the CSV file
csv_file_path = "/home/uribe055/merra_2/experiments/results_all/" + "fig8.csv"
df = pd.read_csv(csv_file_path)

cur_plot = "t_res"
x = "filter_value"
line = "sys"
y = "total_time"

# Get unique plot values
unique_plots = ["hour", "day", "month", "year"]

marker_size = 25
m_fill = "none"
font_size = 30
tick_font_size = 30
tick_size = 30
tick_list = [205,240, 275,310]
tick_labels = tick_list
line_width = 4
above = "bottom"
below = "top"
y_label = "Execution time (sec)"
viridis = matplotlib.colormaps["viridis"]
colors = [viridis(i) for i in [0, 0.25, 0.5, 0.75]]
x_label = "Spatial resolution (degrees)"

# Define style dictionary based on 'line' values
style_dict = {
    "Polaris": {"marker": "o", "markersize": marker_size, "linewidth": line_width, "color": "red", "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels, "fill": "none"},
    "Polaris-ERA5": {"marker": "o", "markersize": marker_size, "linewidth": line_width, "color": "red", "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels, "fill": "full"},
    "Polaris-MERRA2": {"marker": "d", "markersize": marker_size, "linewidth": line_width, "color": "red", "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels, "fill": "full"},
    "Vanilla": {"marker": "v", "markersize": marker_size, "linewidth": line_width, "color": colors[1], "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels, "fill": "none"},
    "TDB": {"marker": "s", "markersize": marker_size, "linewidth": line_width, "color": colors[3], "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels, "fill": "none"},
}

# Determine global y-axis limits
y_min = df[y].min()
y_max = df[y].max()

legend_position = ["best", "lower left", "best", "center"]    # 'best', 'upper right', 'upper left', 'lower left', 'lower right', 'right', 'center left', 'center right', 'lower center', 'upper center', 'center'

# Generate and save individual plots
for plot_value, position  in zip(unique_plots, legend_position):
    fig, ax = plt.subplots(figsize=(8, 6))
    subset = df[df[cur_plot] == plot_value] # df[df[t_res]]==hour
    
    for line_value in subset[line].unique():
        line_data = subset[subset[line] == line_value]  # df[df[system] == polaris]

        # if line_value == "Polaris":
        #     line_data = line_data.groupby(x, as_index=False)["tr"].mean()   # Average over tr values
        # else:
        line_data = line_data.groupby(x, as_index=False)[y].mean()  # Average over x values
        
        line_data = line_data.sort_values(by=x)  # Ensure lines are connected correctly

        # Get style properties from dictionary, use defaults if not found
        style = style_dict.get(line_value, {"marker": "o", "markersize": 4, "linewidth": 1.5, "color": "black", "labelsize": 10, "ticksize": 8})
        
        # if line_value == "Polaris":
        #     ax.plot(line_data[x], line_data["tr"], 
        #             marker=style["marker"], markersize=style["markersize"], fillstyle=m_fill,
        #             linewidth=style["linewidth"], color=style["color"], label=f"{line_value}")
        # else:
        ax.plot(line_data[x], line_data[y], 
                marker=style["marker"], markersize=style["markersize"], fillstyle=style["fill"],
                linewidth=style["linewidth"], color=style["color"], label=f"{line_value}")
    
    ax.set_xlabel(x_label, fontsize=font_size)
    if style["ticklist"] is not None:
        ax.set_xticks(ticks=style["ticklist"], labels=style["ticklabels"])
    ax.set_ylabel(y_label, fontsize=font_size)
    ax.set_yscale("log")  # Set y-axis to log scale
    ax.set_ylim(y_min, y_max)
    if plot_value == "day" or plot_value == "year":
        # ax.legend(fontsize=font_size-10, loc=position, bbox_to_anchor=(0.37,0.53))   # for datasets_separate
        ax.legend(fontsize=font_size-5, loc="best")   # for datasets_averaged
    else:
        # ax.legend(fontsize=font_size-10, loc=position)     # for datasets_separate
        ax.legend(fontsize=font_size, loc=position)     # for datasets_averaged
    ax.tick_params(axis='both', labelsize=tick_font_size)
    
    # test
    plt.tight_layout()
    plt.savefig(f"/home/uribe055/merra_2/experiments/plots/TEST_fig8_{plot_value}.png")  # Save the plot to a file
    plt.close(fig)

    # final
    # plt.tight_layout()
    # plt.savefig(f"")  # Save the plot to a file
    # plt.close(fig)



# import pandas as pd
# import matplotlib.pyplot as plt
# import sys

# def plot_files(filepaths, labels=None, alphas=["s", "^", "o"]):
#     # Read data from each file
#     dataframes = [pd.read_csv(f) for f in filepaths]
    
#     # Determine all unique t_res values across all files
#     unique_t_res = sorted(set().union(*[set(df['t_res'].unique()) for df in dataframes]))

#     # Create a plot for each unique t_res
#     for t in unique_t_res:
#         plt.figure(figsize=(8, 5))
#         for idx, df in enumerate(dataframes):
#             label = labels[idx] if labels else f'File {idx+1}'
#             subset = df[df['t_res'] == t]
#             sub = subset.groupby(['s_res'])['total_time'].mean().reset_index()
#             # total_t = subset.groupby(['total_time'])['s_res'].mean().reset_index()
#             plt.plot(sub['s_res'], sub['total_time'], marker=alphas[idx], label=label, fillstyle='none')

#         plt.title(f"t_res = {t}")
#         plt.xlabel("s_res")
#         plt.ylabel("total_time")
#         plt.legend()
#         plt.grid(True)
#         plt.tight_layout()
#         plt.savefig(f"test_plot_{t}.png")
#         plt.show()

# if __name__ == "__main__":
#     # Example usage with three file paths
#     file1 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions_one_file.csv"
#     file2 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions_yearly_files.csv"
#     file3 = "/home/uribe055/merra_2/experiments/results/results_changing_resolutions.csv"
    
#     # Optionally add custom labels
#     labels = ["One File", "Yearly Files", "Mixed"]
    
#     plot_files([file1, file2, file3], labels)
