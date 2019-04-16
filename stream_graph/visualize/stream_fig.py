# -*- coding: utf-8 -*-
from __future__ import print_function
import sys


def drange(start, stop, step):
    """
        Helper function generating a range of numbers.

        :param start: Range start
        :param end: Range end
        :param step: Range step (the difference between two subsequent elements in the range equals step)
        :type start: float
        :type end: float
        :type step: float
        :return: an iterator over the range
        :rtype: generator

        :Example:

        >>> [i for i in drange(0.0,1.0,0.1)]
        [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    """
    r = start
    while r <= stop:
        yield r
        r += step

class Drawing(object):
    """FIG Drawing class."""
    __version__ = '1.1.1'
    _alpha = 0.0
    _omega = 0.0
    _discrete = -1

    _time_unit = 500
    _node_unit = 600
    _offset_x = 3450
    _offset_y = 2250
    
    _num_node_intervals = 0
    #?Is incremented by one if time marks are used too
    _num_time_intervals = 0
    _num_parameters = 0
    _totalval_parameters = 0

    _first_node = ""

    _nodes = {}
    _node_cpt = 1

    _colors = {}
    _color_cpt = 31
    _directed = False


    def __init__(self, filename, alpha=0.0, omega=10.0, time_width=500, discrete=0, directed=False):
        """Initializes a stream graph drawing.

        Parameters
        ----------
        alpha: float
            Start time of stream graph.
        
        omega: float
            End time of stream graph.

        time_width: int > 0
            Width (in the final fig) of one time unit.

        discrete: int > 0
            Duration of the time step if time is discrete. 0 if time is continuous.

        directed: Bool
            Defines if links are given with directions.
        
        Example
        -------
        
        >>> d = Drawing(alpha=0, omega=5.5)
        >>> d = Drawing(alpha=0, omega=6, discrete=2)
        """

        self._alpha = float(alpha)
        self._omega = float(omega)
        self._time_unit = time_width
        self._discrete = discrete
        self._directed = directed
        self._filename = open(filename, '+w')

        self.linetype = 2

        self._print("""#FIG 3.2  Produced by xfig version 3.2.5b\n\
Landscape\n\
Center\n\
Inches\n\
Letter\n\
100.00\n\
Single\n\
-2\n\
1200 2\n""")
        # Useful predefined colors
        self.addColor("grey", "#888888")

    def _print(self, txt):
        print(txt, file=self._filename)

    def setLineType(def_linetype):
        """
            Changes the linetype for nodes (i.e. from dashed to dotted). Default is dotted (linetype=2).
            See FIG documentation for all values.

            :param def_linetype: the new linetype
            :type def_linetype: int
        """

        self.linetype = def_linetype

    def addColor(self, name, hex):
        """
            Adds a new RGB color for use.

            :param name: Color identifier (must be unique, case sensitive)
            :param hex: Color in hexadecimal format
            :type name: str
            :type hex: str

            :Example:

            >>> d.addColor("red", "#FF0000")
        """

        self._color_cpt += 1
        self._colors[name] = self._color_cpt

        self._print("0 " + str(self._color_cpt) + " " + str(hex))

    def addNode(self, u, times=[], color=0, linetype=None):
        """
            Adds a new node to the stream graph.

            :param u: Name of the node (should be unique).
            :param times: List of tuples indicating when the node is present.
            :param color: Color of the node, either a XFIG int or a user-defined color.
            :param linetype: ?

            :type u: str
            :type times: list of 2-tuples
            :type color: int or str
        
            :Example:
            
            >>> # Adds a node "v" from alpha to omega
            >>> d.addNode("v")
            >>> # Adds a node "v" from time 1 to time 2.5 and from time 4 to time 8.
            >>> d.addNode("v", times=[(1,2.5),(4,8)])
        """
        if self._discrete > 0:
            self.__addDiscreteNode(u, times, color)
        else:
            self.__addContinuousNode(u, times, color, linetype)

    def __addDiscreteNode(self, u, times=[], color="grey", width=1):

        if color in self._colors:
            color = self._colors[color]

        if self._node_cpt == 1:
            self._first_node = u

        self._node_cpt += 1
        self._nodes[u] = self._node_cpt

        self._print("4 0 " + str(color) + " 50 -1 0 30 0.0000 4 135 120 " + str(self._offset_x + int(self._alpha * self._time_unit) - 400) + " " + str(self._offset_y + 125 + int(self._node_cpt * self._node_unit)) + " " + str(u) + "\\001")

        
        if len(times) == 0:
            for i in drange(self._alpha, self._omega, self._discrete):
                self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + self._nodes[u]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")
        else:
            for (i,j) in times:
                for x in drange(i, j, self._discrete):
                    self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(x * self._time_unit)) + " " + str(self._offset_y + self._nodes[u]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")



    def __addContinuousNode(self, u, times=[], color=0, linetype=None):
        """ nodeId : identifiant du noeud
            times : suite d'intervalles de temps ou le noeud est actif
        """

        if linetype is None:
            linetype = self.linetype

        if color in self._colors:
            color = self._colors[color]

        if self._node_cpt == 1:
            self._first_node = u

        self._node_cpt += 1
        self._nodes[u] = self._node_cpt

        self._print("4 0 " + str(color) + " 50 -1 0 30 0.0000 4 135 120 " + str(self._offset_x + int(self._alpha * self._time_unit) - 400) + " " + str(self._offset_y + 125 + int(self._node_cpt * self._node_unit)) + " " + str(u) + "\\001")

        if len(times) == 0:
            self._print("""2 1 """ + str(linetype) + """ 2 """ + str(color) + """ 7 50 -1 -1 6.000 0 0 7 0 0 2\n \
          """ + str(self._offset_x + int(self._alpha * self._time_unit)) + """ """ + str(self._offset_y + int(self._node_cpt * self._node_unit)) + """ """ + str(self._offset_x + int(self._omega * self._time_unit)) + """ """ + str(self._offset_y + int(self._node_cpt * self._node_unit)))
        else:
            for (i,j) in times:
                self._print("""2 1 """ + str(linetype) + """ 2 """ + str(color) + """ 7 50 -1 -1 6.000 0 0 7 0 0 2\n \
          """ + str(self._offset_x + int(i* self._time_unit)) + """ """ + str(self._offset_y + int(self._node_cpt * self._node_unit)) + """ """ + str(self._offset_x + int(j * self._time_unit)) + """ """ + str(self._offset_y + int(self._node_cpt * self._node_unit)))

    def addLink(self, u, v, b, e, curving=0.0, color=0, height=0.5, width=3):
        """
            Adds a link from time b to time e between nodes u and v.

            :param u: Node to be linked
            :param v: Node to be linked
            :param b: Start time of the link
            :param e: End time of the link
            :param curving: Curving of the link. 0 corresponds to a straight link, 
                            negative values will draw the link bent on the left, 
                            positive values will draw the link bent on the right
            :param color: the link's color (see addColor())
            :param height:  Fixes the position of the duration bar; values are between 0 and 1.
                            0 would draw the duration bar at node u's level, 1 at node's v, 0.5 in between, etc.
            :param width: The link's width

            :type u: str
            :type v: str
            :type b: float
            :type e: float
            :type curving: float
            :type color: str/int
            :type height: float
            :type width: int

            :Example:

            >>> # Add link from time 1 to time 3 between nodes u and v
            >>> d.addLink("u", "v", 1, 3)
            >>> # Add a right curved link from time 1 to time 3 between nodes u and v
            >>> d.addLink("u", "v", 1, 3, curving=0.3)
        """
        if self._discrete > 0:
            self.__addDiscreteLink(u, v, b, e, curving, color, height, width)
        else:
            self.__addContinuousLink(u, v, b, e, curving, color, height, width)

    def __addDiscreteLink(self, u, v, b, e, curving=0.0, color=0, height=0.5, width=3):
        if color in self._colors:
            color = self._colors[color]
        if self._directed:
            if self._nodes[u] > self._nodes[v]:
                (u,v) = (v,u)
                arrow_type = "0 1"
            else:
                arrow_type = "1 0"
        else:
            if self._nodes[u] > self._nodes[v]:
                (u,v) = (v,u)
            arrow_type = "0 0"

        for i in drange(b,e, self._discrete):
            # Draw circles for u and v
            self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + self._nodes[u]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")
            self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + self._nodes[v]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")
            
            # Link them
            x1, y1 = self._offset_x + int(i * self._time_unit), self._offset_y + self._nodes[u]*self._node_unit
            x2 = self._offset_x + int((i + curving) * self._time_unit)
            y2 = int((self._offset_y + self._nodes[v]*self._node_unit) - 0.5 * (self._nodes[v]-self._nodes[u]) * self._node_unit) 
            x3 = self._offset_x + int(i * self._time_unit)
            y3 = self._offset_y + self._nodes[v]*self._node_unit

            if self._directed:
                dir_arg1 = "2"
                dir_arg2 = "1.000"
            else:
                dir_arg1 = "0"
                dir_arg2 = "-1.000"

            self._print("3 2 0 " + str(width) + " " + str(color) + " 7 50 -1 -1 0.000 0 " + str(arrow_type) + " 3")
            # arrow type
            if self._directed:
                self._print("1 1 3.00 90.00 150.00")
            self._print("%s %s %s %s %s %s" % (x1, y1, x2, y2, x3, y3))
            self._print("0.000 " + str(dir_arg2) + " 0.000")

            numnodes = abs(self._nodes[u] - self._nodes[v])


    def __addContinuousLink(self, u, v, b, e, curving=0.0, color=0, height=0.5, width=3):
        if color in self._colors:
            color = self._colors[color]
        if self._directed:
            if self._nodes[u] > self._nodes[v]:
                (u,v) = (v,u)
                arrow_type = "0 1"
            else:
                arrow_type = "1 0"
        else:
            if self._nodes[u] > self._nodes[v]:
                (u,v) = (v,u)
            arrow_type = "0 0"

        # Draw circles for u and v
        self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[u]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")
        self._print("1 3 0 " + str(width) + " " + str(color) + " " + str(color) + " 49 -1 20 0.000 1 0.0000 " + str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[v]*self._node_unit) + " 45 45 -6525 -2025 -6480 -2025")
        
        # Link them
        x1, y1 = self._offset_x + int(b * self._time_unit), self._offset_y + self._nodes[u]*self._node_unit
        x2 = self._offset_x + int((b + curving) * self._time_unit)
        y2 = int((self._offset_y + self._nodes[v]*self._node_unit) - 0.5 * (self._nodes[v]-self._nodes[u]) * self._node_unit) 
        x3 = self._offset_x + int(b * self._time_unit)
        y3 = self._offset_y + self._nodes[v]*self._node_unit

        if self._directed:
            dir_arg1 = "1"
            dir_arg2 = "1.000"
        else:
            dir_arg1 = "0"
            dir_arg2 = "-1.000"

        self._print("3 2 0 " + str(width) + " " + str(color) + " 7 50 -1 -1 0.000 0 " + str(arrow_type) + " 3")
        # arrow type
        if self._directed:
            self._print("1 1 3.00 90.00 150.00")
        self._print("%s %s %s %s %s %s" % (x1, y1, x2, y2, x3, y3))
        self._print("0.000 " + str(dir_arg2) +" 0.000")

        numnodes = abs(self._nodes[u] - self._nodes[v])

        # Add duration
        self._print("2 1 0 " + str(width) + " " + str(color) + " 7 50 -1 -1 0.000 0 0 -1 0 0 2")
        self._print(str(self._offset_x + int((b + curving) * self._time_unit)) + " " + str(self._offset_y + int(self._nodes[u]*self._node_unit + (numnodes*self._node_unit*height))) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + self._nodes[v]*self._node_unit - (numnodes*self._node_unit*(1-height))))

    def addNodeCluster(self, u, times=[], color=0, width=200):
        """
            Adds a node cluster (drawn as a rectangle) for one node over time.

            :param u: The node in the cluster
            :param times: The times at which u is in the cluster
            :param color: The color of the rectangle
            :param width: The width of the rectangle

            :type u: str
            :type times: list of tuples
            :type color: str/int
            :type width: int

            :Example:

            >>> # Create the blue node cluster {u}x[3,4] U {v}x[5,7.5] U {x}x[2,4]
            >>> d.addNodeCluster("u", [(3,4)], color=11)
            >>> d.addNodeCluster("v", [(5,7.5)], color=11)
            >>> d.addNodeCluster("x", [(2,4)], color=11)

        """

        margin = int(width / 2)

        if color in self._colors:
            color = self._colors[color]        

        if len(times) == 0:
            times = [(self._alpha, self._omega)]

        for (i,j) in times:
            (x1, y1) = ( self._offset_x + int(i * self._time_unit), self._offset_y + int(self._nodes[u]*self._node_unit) - margin )
            (x2, y2) = ( self._offset_x + int(j * self._time_unit), self._offset_y + int(self._nodes[u]*self._node_unit) - margin ) 
            (x3, y3) = ( self._offset_x + int(j * self._time_unit), self._offset_y + int(self._nodes[u]*self._node_unit) + margin ) 
            (x4, y4) = ( self._offset_x + int(i*self._time_unit), self._offset_y + int(self._nodes[u]*self._node_unit) + margin ) 

            self._print("2 2 0 0 0 " + str(color) + " 51 -1 20 0.000 0 0 -1 0 0 5")
            self._print(str(x1) + " " + str(y1) + " " + str(x2) + " " + str(y2) + " " + str(x3) + " " + str(y3) + " " + str(x4) + " " + str(y4) + " " + str(x1) + " " + str(y1))

    def addParameter(self, letter, value, color=0, width=1):
        """
            Adds a parameter (like Delta=2). Multiple parameters will be placed at the top of the drawing, on each other's side 

            :param letter: The letter for the parameter, in ascii (will be translated in greek, i.e. d gives delta, m gives mu, etc.)
            :param value: The value for the parameter
            :param color: The color (see addColor())
            :param width: The interval's width

            :type letter: str
            :type value: float
            :type color: int/str
            :type width: int

            :Example:
            
            >>> # Adds a parameter delta with value 3
            >>> d.addParameter("d", 3)
        """

        if color in self._colors:
            color = self._colors[color]

        #?Place at top? Confusing with time intervals...
        pos_segment_y = self._offset_y + (self._nodes[self._first_node] * self._node_unit) - (150 * self._num_time_intervals) - 400
        # Place at bottom instead? Then needs to be written last.
        # pos_segment_y = self._offset_y + self._node_cpt * self._node_unit + 2*self._node_unit

        if self._num_parameters == 0:
            paramoffset = 0
        else:
            paramoffset = 200

        self._print("2 1 " + str(color) + " " + str(width) + " 0 7 50 -1 -1 0.000 0 0 -1 1 1 2")
        self._print("13 1 1.00 60.00 120.00")
        self._print("13 1 1.00 60.00 120.00")
        self._print(str(self._offset_x + (self._totalval_parameters * self._time_unit  + (self._num_parameters * paramoffset))) + " " + str(pos_segment_y) + " " + str(self._offset_x + int(value * self._time_unit) + (self._totalval_parameters * self._time_unit + (self._num_parameters * paramoffset))) + " " + str(pos_segment_y))

        valtocenter = int(value * self._time_unit / 2) - (200 + (50 * max(int(value) - 2, 0))) 

        self._print("4 0 0 50 -1 32 14 0.0000 4 180 375 " + str(self._offset_x + (self._totalval_parameters * self._time_unit + int(self._num_parameters * paramoffset)) + valtocenter)  + " " + str(pos_segment_y - 150)  + " " + str(letter) + " = " + str(value) + "\\001")
        self._totalval_parameters += value
        self._num_parameters += 1

    def addNodeIntervalMark(self, u, v, color=0, width=1):
        if color in self._colors:
            color = self._colors[color]

        pos_segment_x = self._offset_x - (150 * self._num_node_intervals) - 600

        self._print("2 1 " + str(color) + " " + str(width) + " 0 7 50 -1 -1 0.000 0 0 -1 1 1 2")
        self._print("13 1 1.00 60.00 120.00")
        self._print("13 1 1.00 60.00 120.00")
        self._print(str(pos_segment_x) + " " + str(self._offset_y + self._nodes[u] * self._node_unit) + " " + str(pos_segment_x) + " " + str(self._offset_y + self._nodes[v] * self._node_unit))
        self._num_node_intervals += 1


    def addTimeNodeMark(self, t, v, color=0, width=2, depth=49):
        """
            Adds a mark (a cross) at a given node and time.

            :param t: The time at which to add the mark
            :param v: The node at which to add the mark
            :param color: The mark's color (see addColor())
            :param width: The mark's width
            :param depth: Layer for XFIG. Higher values will put the mark in the background, lower in the foreground.

            :type t: float
            :type v: str
            :type color: int/str
            :type width: int
            :type depth: int

            :Example:

            >>> d.addTimeNodeMark(2, "u", color=11, width=3)
        """
        if color in self._colors:
            color = self._colors[color]

        self._print("2 1 0 "+ str(width) + " " + str(color) + " 7 " + str(depth) + " -1 -1 0.000 0 0 -1 0 0 2")
        self._print(str(self._offset_x + int(t * self._time_unit) - 50 ) + " " + str(self._offset_y + self._nodes[v]*self._node_unit - 50) + " " + str(self._offset_x + int(t * self._time_unit) + 50 ) + " " + str(self._offset_y + self._nodes[v]*self._node_unit + 50))

        self._print("2 1 0 "+ str(width) + " " + str(color) + " 7 " + str(depth) + " -1 -1 0.000 0 0 -1 0 0 2")
        self._print(str(self._offset_x + int(t * self._time_unit) - 50 ) + " " + str(self._offset_y + self._nodes[v]*self._node_unit + 50) + " " + str(self._offset_x + int(t * self._time_unit) + 50 ) + " " + str(self._offset_y + self._nodes[v]*self._node_unit - 50))

    def addTimeIntervalMark(self, b, e, color=0, width=1):
        pos_segment_y = self._offset_y + (self._nodes[self._first_node] * self._node_unit) - (100 * self._num_time_intervals) - 200

        self._print("2 1 0 1 0 7 50 -1 -1 0.000 0 0 -1 1 1 2")
        self._print("13 1 1.00 60.00 120.00")
        self._print("13 1 1.00 60.00 120.00")
        self._print(str(self._offset_x + b * self._time_unit) + " " + str(pos_segment_y) + " " + str(self._offset_x + (e * self._time_unit)) + " " + str(pos_segment_y))
        self._num_time_intervals += 1

    def addPath(self, path, start, end, gamma=0, color=0, width=1, depth=51):
        """
            Adds a temporal path from a sequence of (t,u,v) meaning that there was a hop from u to v at time t.

            :param path: A list of (t,u,v) that are the hops in the path
            :param start: The start time of the path
            :param end: The end time of the path
            :param gamma: Useful for gamma-path (if gamma > 0, the hops from u to v will take gamma time units)
            :param color: The path's color (see addColor())
            :param width: The path's width
            :param depth: Layer for XFIG. Higher values will put the mark in the background, lower in the foreground.

            :type path: list
            :type start: float
            :type end: float
            :type gamma: float
            :type color: int/str
            :type width: int
            :type depth: int

            :Example:

            >>> # Path from u to x from time 1 to time 9
            >>> d.addPath([(2,u,v), (5, v, x)], 1, 7)
            >>> # gamma=2-path from u to x from time 1 to time 9
            >>> d.addPath([(2,u,v), (5, v, x)], 1, 9, gamma=2)
        """

        if color in self._colors:
            color = self._colors[color]
        
        t0 = path[0][0]
        u0 = path[0][1]
        xi = self._offset_x + int(start * self._time_unit)
        yi = self._offset_y + self._nodes[u0] * self._node_unit
            
        xj = self._offset_x + int((t0) * self._time_unit) 
        yj = self._offset_y + self._nodes[u0] * self._node_unit


        coords = [(xi,yi), (xj,yj)]

        for (t,u,v) in path:
            xi = self._offset_x + int(t * self._time_unit)
            yi = self._offset_y + self._nodes[u] * self._node_unit
            
            xj = self._offset_x + int((t + gamma) * self._time_unit) 
            yj = self._offset_y + self._nodes[v] * self._node_unit
            
            coords.append((xi,yi))
            coords.append((xj,yj))

        tk = path[-1][0]
        vk = path[-1][2]
        xi = self._offset_x + int(tk * self._time_unit)
        yi = self._offset_y + self._nodes[vk] * self._node_unit
            
        xj = self._offset_x + int((end) * self._time_unit) 
        yj = self._offset_y + self._nodes[vk] * self._node_unit
        coords.append((xi,yi))
        coords.append((xj,yj))

        self._print("2 1 0 " + str(width) + " " + str(color) + " 7 " + str(depth) + " -1 -1 0.000 0 0 -1 0 0 " + str(len(coords)))
        self._print(" ".join([ " ".join(map(str, i)) for i in coords ] ))

    def addRectangle(self, u, v, b, e, width=100, depth=51, color=0, border="", bordercolor=0, borderwidth=2):
        """
            Adds a rectangle from node u to node v and from time b to time e.
            The corners of the rectangle will be (u,b), (u,e), (v,b), (v,e)

            :param u: Start node
            :param v: End node
            :param b: Start time
            :param e: End time
            :param width: The rectangle's width (to add an offset)
            :param depth: Layer for XFIG. Higher values will put the mark in the background, lower in the foreground
            :param color: Background color (see addColor())
            :param border: If borders should be drawn, takes "lrtb" (for left, right, top, bottom) as arguments
            :param bordercolor: The border's color (see addColor())
            :param borderwidth: The border's width

            :type u: str
            :type v: str
            :type b: float
            :type e: float
            :type width: int
            :type depth: int
            :type color: int/str
            :type border: str
            :type bordercolor: int/str
            :type borderwidth: int

            :Example:

            >>> # Rectangle without border
            >>> d.addRectangle("u", "v", 2, 6, color=11) 
            >>> # Rectangle with border all around
            >>> d.addRectangle("u", "v", 2, 6, color=11, border="lrtb")
            >>> # Rectangle with borders except on top
            >>> d.addRectangle("u", "v", 2, 6, color=11, border="lrb")
        """

        margin = int(width/2)

        if color in self._colors:
            color = self._colors[color]
        if bordercolor in self._colors:
            bordercolor = self._colors[bordercolor]

        # Print border lrtb (if any)
        if "l" in border:
            self._print("2 1 0 " + str(borderwidth) + " " + str(bordercolor) + " 7 48 -1 -1 0.000 0 0 -1 0 0 2")
            self._print(str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[u] * self._node_unit - margin) + " " + str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[v] * self._node_unit + margin) + "\n")
        if "r" in border:
            self._print("2 1 0 " + str(borderwidth) + " " + str(bordercolor) + " 7 48 -1 -1 0.000 0 0 -1 0 0 2")
            self._print(str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + self._nodes[u] * self._node_unit - margin) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + self._nodes[v] * self._node_unit + margin) + "\n")
        if "t" in border:
            self._print("2 1 0 " + str(borderwidth) + " " + str(bordercolor) + " 7 48 -1 -1 0.000 0 0 -1 0 0 2")
            self._print(str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[u] * self._node_unit - margin) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + self._nodes[u] * self._node_unit - margin))
        if "b" in border:
            self._print("2 1 0 " + str(borderwidth) + " " + str(bordercolor) + " 7 48 -1 -1 0.000 0 0 -1 0 0 2")
            self._print(str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + self._nodes[v] * self._node_unit + margin) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + self._nodes[v] * self._node_unit + margin))

        if color > -1:
            # Print rectangle
            self._print("2 2 0 0 0 " + str(color) + " " + str(depth) + " -1 20 0.000 0 0 -1 0 0 5")
            self._print(str(self._offset_x + int(b * self._time_unit)) + " " + str(self._offset_y + int(self._nodes[u]*self._node_unit) - margin) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + int(self._nodes[u]*self._node_unit) - margin) + " " + str(self._offset_x + int(e * self._time_unit)) + " " + str(self._offset_y + int(self._nodes[v]*self._node_unit) + margin) + " " + str(self._offset_x + int(b*self._time_unit)) + " " + str(self._offset_y + int(self._nodes[v]*self._node_unit) + margin) + " " + str(self._offset_x + int(b*self._time_unit)) + " " + str(self._offset_y + int(self._nodes[u]*self._node_unit) - margin) )

    def addTime(self, t, label="", width=1, font=12, color=0):
        """
            Adds a vertical dotted line at a given time.

            :param t: the time at which the line will be drawn
            :param label: the label that will be displayed on top of the vertical line
            :param width: the line's width
            :param font: the label's font (in pt)
            :param color: the line's color (XFIG defined or user-defined, see addColor() )

            :Example:
        
            >>> # Adds a vertical line labelled "t" at time 2
            >>> d.addTime(2, "t")
        """

        if self._num_time_intervals == 0:
            self._num_time_intervals = 1

        if color in self._colors:
            color = self._colors[color]

        linetype = 1

        self._print("""2 1 """ + str(linetype) + """ """ + str(width) + """  """ + str(color) + """ 7 50 -1 -1 2.000 0 0 7 0 0 2\n \
          """ + str(self._offset_x + int(t * self._time_unit)) + """ """ + str(self._offset_y + int(2 * self._node_unit - 150)) + """ """ + str(self._offset_x + int(t * self._time_unit)) + """ """ + str(self._offset_y + int(self._node_cpt * self._node_unit + 300)))

        # Add label if any
        self._print("4 0 " + str(color) + " 50 -1 0 " + str(font) + " 0.0000 4 135 120 " + str(self._offset_x + int(t * self._time_unit) - (2 * font * len(label))) + " " + str(self._offset_y - 175 + int(2 * self._node_unit)) + " " + str(label) + "\\001")

    def addTimeLine(self, ticks=1, marks=None):
        """
            Adds a time line at the bottom of the stream graph.

            /!\ Should be called last /!\

            :param ticks: Granularity a which ticks should be outputted (every 2, every 1, etc.)
            :param marks: Custom ticks in the form (t, l)

            :type ticks: float
            :type marks: list

            :Example:
            
            >>> # Most common usage
            >>> d.addTimeLine(ticks=2)
            >>> # With one custom tick labeled "a" at time 2.5
            >>> d.addTimeLine(ticks=2, marks=[(2.5, "a")])
        """

        timeline_y = self._node_cpt * self._node_unit + int(self._node_unit / 2)
        
        vals = []
        i = self._alpha
        while i < self._omega:
            if (i).is_integer():
                vals.append((int(i),int(i)))
            else:
                vals.append((i,i))
            i = i+ticks
        
        if marks is not None:
            # Ajouter/remplacer les valeurs (t, v)
            for (t,v) in marks:
                found = False
                for x in range(0, len(vals)):
                    if vals[x][0] == t:
                        vals[x] = (t,v)
                        found = True
                if not found:
                    vals.append((t,v))

        # Time arrow
        if self._discrete > 0:
            start, end = self._omega - 0.5, self._omega
        else:
            start, end = self._alpha, self._omega     

        self._print("2 1 0 1 0 7 50 -1 -1 0.000 0 0 -1 1 0 2")
        self._print("1 1 1.00 60.00 120.00")
        self._print(str(self._offset_x + int(start * self._time_unit)) + " " + str(self._offset_y + timeline_y) + " " + str(self._offset_x + int(end * self._time_unit)) + " " + str(self._offset_y + timeline_y))

        # Time ticks
        for (i,j) in vals:
            self._print("2 1 0 1 0 7 50 -1 -1 0.000 0 0 -1 0 0 2")
            self._print(str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + timeline_y) + " " + str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + timeline_y + 30))
            if i < self._omega - 1:
                self._print("4 1 0 50 -1 0 20 0.0000 4 135 120 " + str(self._offset_x + int(i * self._time_unit)) + " " + str(self._offset_y + timeline_y + 250) + " " + str(j) + "\\001")

        # Write "time"
        self._print("4 2 0 50 -1 0 20 0.0000 4 135 120 " + str(self._offset_x + int(self._omega * self._time_unit)) + " " + str(self._offset_y + timeline_y + 250) + " time\\001")

    def __del__(self):
        # Adds white rectangle in background around first node (for EPS bounding box)
        self.addRectangle(self._first_node, self._first_node, self._alpha, self._omega, width=300,depth=60, color=7)
        self._filename.close()
