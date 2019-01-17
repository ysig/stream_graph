"""Test file for visualization."""
import os.path
from stream_graph import StreamGraph
from stream_graph import Visualizer
from stream_graph import LinkStreamDF

def test_visualize_sg(remove=True):
    df = [(1, 2, 2, 3), (1, 2, 3, 5), (1, 2, 6, 8)] 
    Visualizer(LinkStreamDF(df, disjoint_intervals=False)).produce()
    assert os.path.exists('test_visualize.fig')
    if remove:
        os.remove('test_visualize.fig')

if __name__ == "__main__":
    test_visualize_sg(False)
    
