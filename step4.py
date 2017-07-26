'''
16:137:602:900C
Introduction to Cloud and Big Data Systems (Spring 2017)

Assignment 4: Heat Map Generation
Maruthi Ayyappan – Aishwarya Gunde – Beethoven Plaisir
'''

#Importing libraries to be used
import numpy as np
import matplotlib.pyplot as plt

# Loading output file to a csv, converting list into values of x y and z.
def get_xyz_from_csv_file_np(csv_file_path): 
    x, y, z = np.loadtxt(csv_file_path, delimiter=', ', dtype=np.int).T
    plt_z = np.zeros((y.max()+1, x.max()+1))
    plt_z[y, x] = z

    return plt_z

# Function to generate heatmap
def draw_heatmap(plt_z):
    plt_y = np.arange(plt_z.shape[0])
    plt_x = np.arange(plt_z.shape[1])
    z_min = plt_z.max()
    z_max = plt_z.min() 

    plot_name = "demo"

    color_map = plt.cm.gist_heat 
    fig, ax = plt.subplots()
    cax = ax.pcolor(plt_x, plt_y, plt_z, cmap=color_map, vmin=z_min, vmax=z_max)
    ax.set_xlim(plt_x.min(), plt_x.max())
    ax.set_ylim(plt_y.min(), plt_y.max())
    fig.colorbar(cax).set_label(plot_name, rotation=270) 
    ax.set_title(plot_name)  
    ax.set_aspect('equal')
    figure = plt.gcf()
    fname = "heatmap.png"
    plt.savefig(fname)

fname = "Average.txt"
res = get_xyz_from_csv_file_np(fname)
draw_heatmap(res)
