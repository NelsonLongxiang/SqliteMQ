#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/10/4 上午3:35
# @Author : 龙翔
# @File    :sql_queue.py
# @Software: PyCharm
import datetime
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from queue import Queue, Empty

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
        self.db_path_dir = os.path.join(db_path_dir, "queues")
        os.makedirs(self.db_path_dir, exist_ok=True)
        self.conn = sqlite3.connect(os.path.join(self.db_path_dir, "queue_" + queue_name + '.db'))
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
        """
        插入数据
        """
        self.cursor.execute(f"INSERT INTO {self.queue_name} (data) VALUES (?)", (data,))
        self.conn.commit()
        return 'ok'

    def put_mul(self, data_list):
        """
        批量插入数据
        """
        # 开启事务
        self.cursor.execute("BEGIN TRANSACTION")
        for data in data_list:
            self.cursor.execute(f"INSERT INTO {self.queue_name} (data) VALUES (?)", (data,))
        self.conn.commit()
        return 'ok'

    def ack_put(self, id_, data):
        """
        将数据插入到ack队列中
        """
        self.cursor.execute(f"REPLACE INTO {self.ack_queue_name} (id,data) VALUES (?,?)", (id_, data))
        self.conn.commit()
        return self.ack_keys()

    def get(self):
        """
        获取队列中的第一条数据，并删除这条数据
        """
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
        """
        获取队列中的所有数据。
        """
        self.cursor.execute(
            f"SELECT id,data,CAST(strftime('%s',created_at) as INTEGER) FROM {self.queue_name} ORDER BY created_at ASC")
        self.conn.commit()
        return self.cursor.fetchall()

    def size(self):
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        self.conn.commit()
        return self.cursor.fetchone()[0]

    def clear(self):
        self.cursor.execute(f"DELETE FROM {self.queue_name}")
        self.cursor.execute(f"DELETE FROM {self.ack_queue_name}")
        self.conn.commit()
        return 'ok'

    def close(self):
        self.cursor.close()
        self.conn.close()
        return 'ok'

    def get_mul(self, num):
        """
        获取队列中的前num条数据，并删除这些数据
        """
        self.cursor.execute(f"SELECT * FROM {self.queue_name} ORDER BY created_at ASC LIMIT ?", (num,))
        self.conn.commit()
        rows = self.cursor.fetchall()
        if rows:
            ids = [row[0] for row in rows]
            placeholders = ','.join('?' for _ in ids)
            self.cursor.execute("BEGIN TRANSACTION")
            self.cursor.execute(f"DELETE FROM {self.queue_name} WHERE id IN ({placeholders})", ids)
            self.conn.commit()
            return rows
        return []

    def re_data(self) -> int:
        """
        将ack队列中的数据重新放回队列中,并删除ack队列中的数据,返回重新放回的数据数量
        """
        self.cursor.execute(f"SELECT * FROM {self.ack_queue_name}")
        self.conn.commit()
        rows = self.cursor.fetchall()
        if rows:
            self.cursor.execute("BEGIN TRANSACTION")
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

    def ack_delete(self, _id):
        """
        删除ack队列中的数据
        """
        self.cursor.execute(f"DELETE FROM {self.ack_queue_name} WHERE id=?", (_id,))
        self.conn.commit()
        return self.ack_keys()

    def ack_keys(self):
        """
        获取ack队列中的所有数据
        """
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

        self.work_queue = Queue()
        self.result_queue = Queue(1)
        self.ack_queue = Queue(1)
        self.db_size = 0
        self.ack_size = 0

        self._ack_keys = []

        self.switch = True

        self.ack_timeout_limit = 0

    def run(self):
        sql_queue = SqliteQueue(self.topic, db_path_dir=self.db_path_dir)
        sql_queue.re_data()
        count = 0
        while self.switch:
            try:
                try:
                    if self.ack_queue.qsize():
                        command, data = self.ack_queue.get_nowait()
                        getattr(self, command)(*(sql_queue,) + data)
                        continue
                    else:
                        command, data = self.work_queue.get_nowait()
                        getattr(self, command)(*(sql_queue,) + data)
                    count += 1
                    if count > 1000:
                        count = 0
                        self.inspect_ack_timeout(sql_queue)
                except Empty:
                    self.inspect_ack_timeout(sql_queue)
                    time.sleep(0.1)
            except Exception as e:
                print(e, e.__traceback__.tb_lineno, self.topic)

    def ack_put_work(self, *args):
        sql_queue = args[0]
        self._ack_keys = sql_queue.ack_put(*args[1:])

    def ack_delete_work(self, *args):
        sql_queue = args[0]
        self._ack_keys = sql_queue.ack_delete(*args[1:])

    def get_work(self, *args):
        sql_queue = args[0]
        self.result_queue.put(sql_queue.get())
        self.db_size = sql_queue.size()

    def put_work(self, *args):
        sql_queue = args[0]
        data = args[1]
        if isinstance(data, list):
            sql_queue.put_mul(data)
        else:
            sql_queue.put(data)
        self.db_size = sql_queue.size()

    def close_work(self, *args):
        sql_queue = args[0]
        sql_queue.close()
        self.stop()

    def clear_work(self, *args):
        sql_queue = args[0]
        sql_queue.clear()

    def re_data_work(self, *args):
        sql_queue = args[0]
        sql_queue.re_data()
        self.db_size = sql_queue.size()
        self._ack_keys = sql_queue.ack_keys()

    def start(self):
        threading.Thread(target=self.run).start()

    def get(self):
        try:
            return self.result_queue.get_nowait()
        except Empty:
            self.work_queue.put(("get_work", (None,)))
            time.sleep(0.1)
            return None

    def put(self, data):
        if isinstance(data, (list, tuple, dict)):
            data = json.dumps(data, ensure_ascii=False)
        self.work_queue.put(("put_work", (data,)))

    def pul_mul(self, datas):
        d = []
        for data in datas:
            if isinstance(data, (list, tuple, dict)):
                data = json.dumps(data, ensure_ascii=False)
            d.append(data)
        self.work_queue.put(("put_work", (d,)))

    def qsize(self):
        return self.db_size + self.result_queue.qsize()

    def close(self):
        self.work_queue.put(("close_work", (None,)))

    def ack_put(self, _id, data):
        self.ack_queue.put(("ack_put_work", (_id, data)))

    def ack_delete(self, _id):
        self.ack_queue.put(("ack_delete_work", (_id,)))

    def ack_size(self):
        return len(self._ack_keys)

    def clear(self):
        self.work_queue.put(("clear_work", (None,)))

    def re_data(self):
        self.ack_queue.put(("re_data_work", (None,)))

    def stop(self):
        self.switch = False

    def inspect_ack_timeout(self, sql_queue):
        ch_keys = sql_queue.ack_keys()
        for key_data in ch_keys:
            id_, data, t = key_data
            if id_:
                if self.ack_timeout_limit and time.time() - t > self.ack_timeout_limit:
                    sql_queue.ack_delete(id_)
                    sql_queue.put(data)


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
            time.sleep(0.1)

        sql_server.close()

    def stop(self):
        self.switch = 0
