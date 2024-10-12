#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/10/12 下午11:45
# @Author : 龙翔
# @File    :test.py.py
# @Software: PyCharm

import os
import sys
import time

from SqliteMQ.sql_queue import SqlQueueTask, SqlMQ

# 将当前文件夹添加到环境变量
if os.path.basename(__file__) in ['run.py', 'main.py', '__main__.py']:
    if '.py' in __file__:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    else:
        sys.path.append(os.path.abspath(__file__))

if __name__ == '__main__':
    '''
    建议使用多线程队列 和 MQ消息队列
    '''
    test_queue = SqlQueueTask("test", './')
    test_queue.start()

    test_queue.put("test1")
    test_queue.put("test2")
    print(test_queue.get())
    test_mq = SqlMQ()
    test_mq.start_receive(callback=lambda ch, body: print(ch, body) or time.sleep(5) or ch.basic_ack(),
                          sql_server=test_queue, count=1)
    test_mq.stop()
    test_queue.stop()
