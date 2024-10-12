#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/10/4 上午3:35
# @Author : 龙翔
# @File    :sql_queue.py
# @Software: PyCharm
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from queue import Queue

# 将当前文件夹添加到环境变量
if os.path.basename(__file__) in ['run.py', 'main.py', '__main__.py']:
    if '.py' in __file__:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    else:
        sys.path.append(os.path.abspath(__file__))


class SqliteQueue:
    '''
    单线程队列
    '''

    def __init__(self, queue_name, db_path_dir='./'):
        '''

        :param queue_name: 队列名称
        :param db_path_dir: db存放位置
        '''
        self.topic = queue_name
        self.conn = sqlite3.connect(os.path.join(db_path_dir, "queue_" + queue_name + '.db'))
        self.cursor = self.conn.cursor()
        self.queue_name = queue_name
        self.ack_queue_name = f"ack_{queue_name}"
        self.create_table()

    def create_table(self):
        self.cursor.execute(
            f'''CREATE TABLE IF NOT EXISTS {self.queue_name} 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'''
        )
        self.cursor.execute(
            f'''CREATE TABLE IF NOT EXISTS {self.ack_queue_name}
            (id TEXT PRIMARY KEY,
            data TEXT, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        self.conn.commit()

    def put(self, data):
        self.cursor.execute(f"INSERT INTO {self.queue_name} (data) VALUES (?)", (data,))
        self.conn.commit()
        return 'ok'

    def ack_put(self, id_, data):
        self.cursor.execute(f"INSERT INTO {self.ack_queue_name} (id,data) VALUES (?,?)", (id_, data))
        self.conn.commit()
        return 'ok'

    def get(self):
        self.cursor.execute(
            f"SELECT id,data,CAST(strftime('%s',created_at) as INTEGER) FROM {self.queue_name} ORDER BY created_at ASC LIMIT 1")
        row = self.cursor.fetchone()
        if row:
            id_ = row[0]
            self.cursor.execute(f"DELETE FROM {self.queue_name} WHERE id=?", (id_,))
            self.conn.commit()
            return row
        return None

    def get_all(self):
        self.cursor.execute(
            f"SELECT id,data,CAST(strftime('%s',created_at) as INTEGER) FROM {self.queue_name} ORDER BY created_at ASC")
        rows = self.cursor.fetchall()
        if rows:
            return rows
        return None

    def size(self):
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        count = self.cursor.fetchone()[0]
        return count

    def clear(self):
        self.cursor.execute(f"DELETE FROM {self.queue_name}")
        self.cursor.execute(f"DELETE FROM {self.ack_queue_name}")
        self.conn.commit()
        return 'ok'

    def close(self):
        self.conn.close()
        self.cursor.close()
        return 'ok'

    def get_mul(self, num):
        self.cursor.execute(f"SELECT * FROM {self.queue_name} ORDER BY created_at ASC LIMIT ?", (num,))
        rows = self.cursor.fetchall()
        if rows:
            return rows
        return None

    def re_data(self):
        self.cursor.execute(f"SELECT * FROM {self.ack_queue_name}")
        rows = self.cursor.fetchall()
        if rows:
            for row in rows:
                self.cursor.execute(f"INSERT INTO {self.queue_name} (data) VALUES (?)", (row[1],))
                self.cursor.execute(f"DELETE FROM {self.ack_queue_name} WHERE id=?", (row[0],))
            self.conn.commit()
            return len(rows)
        return 0

    def qsize(self):
        return self.size()

    def delete(self, id_):
        self.cursor.execute(f"DELETE FROM {self.queue_name} WHERE id=?", (id_,))
        self.conn.commit()
        return 'ok'

    def ack_delete(self, id_):
        self.cursor.execute(f"DELETE FROM {self.ack_queue_name} WHERE id=?", (id_,))
        self.conn.commit()
        return 'ok'

    def ack_keys(self):
        self.cursor.execute(f"SELECT id,data,CAST(strftime('%s',created_at) as INTEGER) FROM {self.ack_queue_name}")
        rows = self.cursor.fetchall()
        if rows:
            return rows
        return []


class SqlCh:
    def __init__(self, topic, data, sql_queue):
        self.topic = topic
        self.sql_queue = sql_queue
        self.id = uuid.uuid4().hex
        sql_queue.ack_put(self.id, data)

    def basic_ack(self):
        self.sql_queue.ack_delete(self.id)


class SqlQueueTask:
    """
    多线程队列，使用前请先在全局实例化。并执行start方法
    """

    def __init__(self, topic, db_path_dir='./'):
        '''
        :param topic: 消息主题
        :param db_path_dir: db 存放位置
        '''
        self.topic = topic
        self.db_path_dir = db_path_dir
        self.put_queue = Queue()
        self.get_queue = Queue()
        self.result_queue = Queue()
        self.ack_delete_queue = Queue()
        self.ack_put_queue = Queue()
        self.size = 0
        self._close = False
        self._clear = False
        self._ack_keys = []
        self.switch = True
        self.re_flag = False
        self.ack_timeout_limit = 0

    def run(self):
        sql_queue = SqliteQueue(self.topic, db_path_dir=self.db_path_dir)
        sql_queue.re_data()
        while self.switch:
            if self.re_flag:
                sql_queue.re_data()
                self.re_flag = True
            if self._clear:
                sql_queue.clear()
                self._clear = False
                continue
            while self.put_queue.qsize():
                sql_queue.put(self.put_queue.get())
                continue
            while self.get_queue.qsize():
                self.get_queue.get()
                self.result_queue.put(sql_queue.get())
                continue
            while self.ack_delete_queue.qsize():
                sql_queue.ack_delete(self.ack_delete_queue.get())
                self._ack_keys = sql_queue.ack_keys()
                continue
            while self.ack_put_queue.qsize():
                sql_queue.ack_put(*self.ack_put_queue.get())
                self._ack_keys = sql_queue.ack_keys()
                continue
            if self._close:
                sql_queue.close()
                break

            self.inspect_ack_timeout(sql_queue)
            self.size = sql_queue.qsize()
            self._ack_keys = sql_queue.ack_keys()
            time.sleep(0.1)

    def start(self):
        threading.Thread(target=self.run).start()

    def get(self):
        self.get_queue.put(1)
        while True:
            if self.result_queue.qsize():
                return self.result_queue.get()
            time.sleep(0.1)

    def put(self, data):
        if isinstance(data, (list, tuple, dict)):
            data = json.dumps(data, ensure_ascii=False)
        self.put_queue.put(data)

    def qsize(self):
        return self.size

    def close(self):
        self._close = True

    def ack_put(self, _id, data):
        self.ack_put_queue.put((_id, data))
        self.waiting_queue(self.ack_put_queue)

    def ack_delete(self, _id):
        self.ack_delete_queue.put(_id)
        self.waiting_queue(self.ack_delete_queue)

    def waiting_queue(self, q):
        while q.qsize():
            time.sleep(0.1)

    def ack_keys(self):
        return self._ack_keys

    def ack_size(self):
        return len(self._ack_keys)

    def clear(self):
        self._clear = True
        while self._clear:
            time.sleep(1)

    def stop(self):
        self.switch = False

    def inspect_ack_timeout(self, sql_queue):
        ch_keys = self.ack_keys()
        for key_data in ch_keys:
            id_, data, t = key_data
            if id_:
                if self.ack_timeout_limit and time.time() - t > self.ack_timeout_limit:
                    sql_queue.ack_delete(id_)
                    self.put(data)


class SqlMQ:
    """
    多线程，消息队列,支持ack_back,当数据确认消费后才会消除，否则重新实例化或者，超时后将会加入队列尾部，时间可自行调整,默认10分钟
    """

    def __init__(self, ack_timeout_limit: int = 600):
        self.switch = 1
        self.link_queue = Queue()
        self.ack_timeout_limit = ack_timeout_limit

    def start_receive(self, callback, sql_server: SqlQueueTask, count=-1):
        '''
        :param callback: 回调函数 args(ch:SqlCh,body:str)。
        :param sql_server: 请先实例化sql_task,并执行start方法后，传入obj。
        :param count:限制获取消息数量 1，默认为-1 不限制。
        :return:
        '''
        sql_server.ack_timeout_limit = self.ack_timeout_limit
        while self.switch:
            while self.link_queue.qsize():
                data = self.link_queue.get()
                sql_server.put(data)
                continue
            data = sql_server.get()
            if data:
                ch = SqlCh(sql_server.topic, data[1], sql_server)
                callback(ch, data)
                if count == 1:
                    return
                continue
            time.sleep(1)

        sql_server.close()

    def stop(self):
        self.switch = 0
