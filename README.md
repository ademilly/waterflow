#Waterflow

[![Build Status](https://travis-ci.org/ademilly/waterflow.svg?branch=master)](https://travis-ci.org/ademilly/waterflow)

Waterflow package provides a data analysis pipeline framework
for data transformation and machine learning

##In one go
- read data files (possibly massive)
- add transformations and new features
- train a model
- test a model
- score

##Usage

Example display first line of `somefile.txt` after applying a function lamba
and used another lambda for filtering.
```
from flow import Flow

flow = Flow()
print flow.read_file('somefile.txt').map(lambda).filter(lambda).batch(10)[0]
```
