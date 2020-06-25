# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
from datetime import datetime, timezone
from math import floor
from multiprocessing.pool import ThreadPool
from django.http import HttpResponse, JsonResponse
import logging
import requests
# Create your views here.
from ipython_genutils.py3compat import xrange
from rest_framework.parsers import JSONParser
from rest_framework.views import APIView
from rest_framework import status

logger = logging.getLogger(__name__)


class Post_log(APIView):

    def post(self, request):

        data = JSONParser().parse(request)

        pfpc = data.get('parallelFileProcessingCount')
        logFiles = data.get('logFiles')

        if pfpc <= 0:
            res = {"status": "failure", "reason": "Parallel File Processing count must be greater than zero!"}
            return JsonResponse(res, status=status.HTTP_400_BAD_REQUEST)

        data_process = self.run_async(logFiles, pfpc, self.data_process)

        flat_list = []
        for sublist in data_process:
            for item in sublist:
                flat_list.append(item)

        insertion = self.run_async(flat_list, 1, self.log_processing)

        print(type(logFiles))
        return HttpResponse(insertion, status=status.HTTP_200_OK)

    @classmethod
    def data_process(cls, logFiles):
        data = []
        for i in logFiles:
            resp = requests.get(i)
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    timeInLogs = line.split()

                    roundedTime = floor(int(timeInLogs[1]) / (15 * 60 * 1000)) * (15 * 60 * 1000)

                    data.append({
                        "time": roundedTime,
                        "data": timeInLogs[-1]
                    })

        return data

    @classmethod
    def log_processing(cls, data):

        data_counted = []
        for d in data:
            for d_c in data_counted:
                if d['time'] == d_c['time'] and d['data'] == d_c['data']:
                    d_c['count'] += 1
                    break
            else:
                d_new = d.copy()
                d_new['count'] = 1
                data_counted.append(d_new)

        data2 = []
        for i in xrange(len(data_counted)):
            date = datetime.utcfromtimestamp(int(data_counted[i]['time']) / 1000)

            data2.append({
                "second": date.second,
                "minute": date.minute,
                "hour": date.hour,
                "data": data_counted[i]['data'],
                "count": data_counted[i]['count']
            })

        resultant_arr = []

        j = 1
        for i in data2:

            exception_name = i['data']
            hour = i['hour']
            mins = i['minute']
            sec = i['second']
            count_num = i['count']

            minInterval = floor((mins + (sec / 100)) / 15)

            hour1 = hour
            hour2 = hour
            if (minInterval + 1) * 15 >= 60:
                if hour2 == 24:
                    hour2 = 0
                else:
                    hour2 += 1

            if hour1 >= 24:
                hour1 = hour1 - 24
            if hour2 >= 24:
                hour2 = hour2 - 24

            min1 = minInterval * 15
            if min1 >= 60:
                min1 = min1 - 60

            min2 = (minInterval + 1) * 15
            if min2 >= 60:
                min2 = min2 - 60

            final_tuple = "{:02}:{:02}-{:02}:{:02} {} {}".format(hour1, min1, hour2, min2, exception_name,
                                                                 count_num)
            resultant_arr.append(final_tuple)
            j += 1

            comma = ','
            if j <= len(data2):
                resultant_arr.append(comma)

        str1 = ''.join(resultant_arr)
        return str1

    @classmethod
    def run_async(cls, all_items, max_pool_size, func):
        logger.info(
            'running items of size: {} in a thread pool in async way with max pool size: {}'.format(len(all_items),
                                                                                                    max_pool_size))
        items_list = cls.get_items_for_thread_pool(max_pool_size, all_items)
        return cls.run_in_process_thread_pool(items_list, func)

    @classmethod
    def get_items_for_thread_pool(cls, all_items, max_pool_size):

        logger.info('get items of size: {} for a thread pool with max pool size: {}'.format(len(all_items),
                                                                                            max_pool_size))
        total_items = len(all_items)
        total_processes = max_pool_size
        items_per_process = floor(total_items / total_processes)
        remaining_items = total_items % max_pool_size
        if items_per_process == 0:
            total_processes = remaining_items

        items_per_thread_list = []
        new_start_index = 0
        for iter in range(0, total_processes):
            start_index = new_start_index
            if remaining_items > 0:
                new_start_index = start_index + items_per_process + 1
            else:
                new_start_index = start_index + items_per_process
            spliced_items = all_items[start_index:new_start_index]
            items_per_thread_list.append(spliced_items)
            remaining_items -= 1
        return items_per_thread_list

    @classmethod
    def run_in_process_thread_pool(cls, items_list, func):

        logger.info('running items of size (thread pool size): {} in a thread pool'.format(len(items_list)))
        pool = ThreadPool(processes=len(items_list))

        response = pool.map(func, items_list)
        pool.close()
        pool.join()
        return response
