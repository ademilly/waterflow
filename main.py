# -*- coding: utf-8 -*-

from dataflow import Flow


def run():

    flow = Flow()

    print flow.from_csv(
        '/Users/ademilly/Work/projects/numerai/data/numerai_training_data.csv'
    )

    return flow

if __name__ == '__main__':
    print 'Hello World!'

    run()
