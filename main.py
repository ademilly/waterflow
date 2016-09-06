# -*- coding: utf-8 -*-

# import numpy
import timeit

from dataflow import Flow


def chrono(f):

    N = 100
    mu_time = 0

    for _ in range(N):

        start_time = timeit.default_timer()

        f()

        mu_time += timeit.default_timer() - start_time

    print '{0:.2} s'.format(mu_time / N)


def run():

    flow = Flow()

    print flow.from_csv(
        '/Users/ademilly/Work/projects/numerai/data/' +
        'numerai_training_data.csv'
    ).filter(lambda x: 'target' not in x) \
        .map(lambda x: [float(_) for _ in x]) \
        .map(lambda x: 2 * x).filter(lambda x: x[-1] != 0) \
        .map(lambda x: 1).reduce(lambda a, b: a + b)

if __name__ == '__main__':
    print("Let's test dataflow package!")

    run()
