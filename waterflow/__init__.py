"""dataflow package provides a framework to build data analysis pipeline

class:
    Flow -- main class for the dataflow package ;
        provides functionnal tools to transform a dataset
        and do machine learning with it
"""

from flow import Flow, Action

__all__ = [Flow, Action]

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
