# -*- coding: utf-8 -*-
"""Grab geospatial data from a list of sources and make a shapefile for each of them."""

import asyncio
import logging
import os
from collections import namedtuple

import aiohttp
import shapefile


class ShapeCreator(object):

    def __init__(self, filename):
        self.filename = filename
        self.w = shapefile.Writer(shapefile.POINT)

    def set_fields(self, field_list):
        for field in field_list:
            self.w.field(field, 'C', 50)

    def add_feature(self, geometry, properties):
        # TODO: check correct coordinates
        points = geometry['coordinates']
        self.w.point(*points)
        self.w.record(*properties.values())

    def make_shape(self):
        self.w.save(self.filename)


class Grabber(object):

    def __init__(self, tasks, log_level='INFO'):
        self.tasks = tasks
        self.sem = asyncio.Semaphore(5)
        self.loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession()

        log_format = '%(asctime)-15s | %(levelname)s | %(name)s [%(pid)s] | %(message)s'
        logging.basicConfig(format=log_format)
        self.logger = logging.getLogger('Grabber')
        self.extra = {'pid': os.getpid()}

        logger_log_level = getattr(logging, log_level, None)
        if not logger_log_level:
            logger_log_level = logging.DEBUG
            self.logger.error('incorrect a log level argument, DEBUG level will be used',
                              extra=self.extra)
        self.logger.setLevel(logger_log_level)

    async def get(self, url, params):
        async with self.session.get(url, params=params) as response:
            return await response.json()

    async def process_url(self, task):
        with (await self.sem):
            page = await self.get(task.url, task.params)
            self.loop.run_in_executor(None, self.create_shape, page, task)
            self.create_shape(page, task)

    def create_shape(self, page, task):
        shape_creator = ShapeCreator(filename=task.file)
        shape_creator.set_fields(page['features'][0]['properties'].keys())
        for feature in page['features']:
            shape_creator.add_feature(geometry=feature['geometry'],
                                      properties=feature['properties'])
        self.logger.debug('time to create a shapefile %s for %s / %s',
                          task.file, task.url, task.params,
                          extra=self.extra)
        shape_creator.make_shape()

    def run(self):
        self.logger.info('start working', extra=self.extra)
        self.loop.run_until_complete(
            asyncio.wait([self.process_url(task) for task in self.tasks]))
        self.session.close()


if __name__ == '__main__':
    url_global = 'http://www.seismicportal.eu/fdsnws/event/1/query'
    params_global = {'limit': 10,
                     'start': '2015-01-01T00:00:00',
                     'end': '2016-01-01T00:00:00',
                     'lat': 44.95212,
                     'lon': 34.10242,
                     'minradius': 0.0,
                     'maxradius': 180.0,
                     'format': 'json'}

    task_class = namedtuple('Task', ['url', 'params', 'file'])
    task1 = task_class(url_global, params_global, 'shape1')
    task2 = task_class(url_global, params_global, 'shape2')
    tasks_arr = [task1, task2]

    grabber = Grabber(tasks=tasks_arr, log_level='DEBUG')
    grabber.run()
