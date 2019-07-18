"""Test file for visualization."""
import os.path
from stream_graph import StreamGraph
from stream_graph import Visualizer
from stream_graph import TemporalLinkSetDF

def test_visualize_sg_fig(remove=True):
    df = [(1, 2, 2, 3), (1, 2, 3, 5), (1, 2, 6, 8)] 
    Visualizer(TemporalLinkSetDF(df, disjoint_intervals=False, discrete=False)).produce()
    assert os.path.exists('test_visualize.fig')
    if remove:
        os.remove('test_visualize.fig')

def test_visualize_sg_svg(remove=True):
    df = [(1, 2, 2, 3), (1, 2, 3, 5), (1, 2, 6, 8)] 
    Visualizer(TemporalLinkSetDF(df, disjoint_intervals=False, discrete=False), image_type='svg').produce()
    assert os.path.exists('test_visualize.svg')
    if remove:
        os.remove('test_visualize.svg')

if __name__ == "__main__":
    test_visualize_sg_fig(False)
    test_visualize_sg_svg(False)
