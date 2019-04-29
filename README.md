# StreamGraph: A prototype for [stream-graphs](https://hal.archives-ouvertes.fr/hal-01665084)



Installing `stream_graph`
-------------------------

The `stream_graph` library requires:

* Python [>=2.7, >=3.5]
* Numpy [>=1.14.0]
* Pandas [>=0.24.0]
* Cython [>=0.27.3]
* six [>=1.11.0]
* svgwrite [>=1.2.1]

Installing Dependencies
-----------------------

For installing dependencies the procedure is the well known:

```shell
pip install extension>=extension_version
```

or simply

```shell
pip install -r requirements.txt
```
Please add a `sudo` if `pip` doesn't have superuser privilages.


Installing the master-version
-----------------------------

```shell
pip install git+https://github.com/ysig/stream_graph/
```


Getting Starting
----------------

For a first tutorial please you can have a look at [emailEU](https://nbviewer.jupyter.org/github/ysig/stream_graph/blob/master/tutorials/emailEU/email-Eu.ipynb).

[Documentation](https://ysig.github.io/stream_graph/doc/)
----------------------------------------------------------

The project documentation is automatically generated online using Sphinx.
To generate it yourself, move to doc folder and execute: `make clean hmtl`, while installing any needed dependencies.
