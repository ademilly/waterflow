# -*- coding: utf-8 -*-

from distutils.core import setup

import versioneer

setup(
    name='waterflow',
    packages=['waterflow'],
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Dataflow package provides a data analysis pipeline' +
    'framework for data transformation and machine learning',
    author='Aurélien Demilly',
    author_email='demilly.aurelien@gmail.com',
    url="https://github.com/ademilly/dataflow",
    keywords=['data science', 'data analysis', 'machine learning'],
    classifiers=[],
)
