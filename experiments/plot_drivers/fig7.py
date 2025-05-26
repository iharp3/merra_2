import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# Load the CSV file
# csv_file_path = "/home/uribe055/merra_2/experiments/results_all/fig7.csv"
csv_file_path = "/home/uribe055/merra_2/experiments/results/results_heatmap.csv"
df = pd.read_csv(csv_file_path)

# cur_plot = "s_res"
x = "time_span"
line = "sys"
y = "total_time"

# Get unique plot values
unique_plots = ["025_H", "05_Y", "025_M", "05_H", "05_Y"]
cur_plot = [[0, "hour"], [1, "year"], [0, "month"], [1, "hour"], [1, "year"]]


marker_size = 25
m_fill="none"
font_size = 30
tick_font_size = 30
tick_size = 30
tick_list = [1, 2.5, 5, 10]
tick_labels = [1, 2.5, 5, 10]
line_width = 4
above = "bottom"
below = "top"
y_label = "Execution time (sec)"
viridis = matplotlib.colormaps["viridis"]
colors = [viridis(i) for i in [0, 0.25, 0.5, 0.75]]
x_label = "Time span (years)"

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

# legend position ["025_H" = , "025_Y" = , "05_M" = , "1_H" = , "1_Y = "]
legend_position = ["center"]*5


# Generate and save individual plots
for plot_value, vals, positon in zip(unique_plots, cur_plot, legend_position): # strings, [0.25, H]...
    fig, ax = plt.subplots(figsize=(8, 6))
    sub1 = df[df["s_res"] == vals[0]]
    subset = sub1[sub1["t_res"] == vals[1]]
    
    for line_value in subset[line].unique():    # for unique system "line"
        line_data = subset[subset[line] == line_value]
        line_data = line_data.groupby(x, as_index=False)[y].mean()  # Average over x values
        line_data = line_data.sort_values(by=x)  # Ensure lines are connected correctly
        
        # Get style properties from dictionary, use defaults if not found
        style = style_dict.get(line_value, {"marker": "o", "markersize": 4, "linewidth": 1.5, "color": "black", "labelsize": 10, "ticksize": 8})
        
        ax.plot(line_data[x], line_data[y], 
                marker=style["marker"], markersize=style["markersize"], fillstyle=m_fill,
                linewidth=style["linewidth"], color=style["color"], label=f"{line_value}")
    
    ax.set_xlabel(x_label, fontsize=font_size)
    if style["ticklist"] is not None:
        ax.set_xticks(ticks=style["ticklist"], labels=style["ticklabels"])
    ax.set_ylabel(y_label, fontsize=font_size)
    ax.set_yscale("log")  # Set y-axis to log scale
    ax.set_ylim(y_min, y_max)
    ax.legend(fontsize=font_size, loc=positon)
    ax.tick_params(axis='both', labelsize=tick_font_size)
    
    # test
    plt.tight_layout()
    plt.savefig(f"/home/uribe055/merra_2/experiments/plot_drivers/plots/fig7_{plot_value}.png")  # Save the plot to a file
    plt.close(fig)

    # # final
    # plt.tight_layout()
    # plt.savefig(f"/home/uribe055/experiment-kit/round2/all_final_figs/h/heatmap_{plot_value}.eps")  # Save the plot to a file
    # plt.close(fig)