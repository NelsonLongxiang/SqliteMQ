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

    def put_mul(self, data_list):
        # 开启事务
        self.cursor.execute("BEGIN TRANSACTION")
        for data in data_list:
            self.cursor.execute(f"INSERT INTO {self.queue_name} (data) VALUES (?)", (data,))
        self.conn.commit()
        return 'ok'

    def ack_put(self, id_, data):
        self.cursor.execute(f"REPLACE INTO {self.ack_queue_name} (id,data) VALUES (?,?)", (id_, data))
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
        self.conn.commit()
        rows = self.cursor.fetchall()
        if rows:
            return rows
        return None

    def size(self):
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        self.conn.commit()
        count = self.cursor.fetchone()[0]
        return count

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

    def re_data(self):
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

    def ack_delete(self, ids_):
        self.cursor.execute("BEGIN TRANSACTION")
        for id_ in ids_:
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
        self.get_count_limit = 1

    def run(self):
        sql_queue = SqliteQueue(self.topic, db_path_dir=self.db_path_dir)
        sql_queue.re_data()
        while self.switch:
            try:
                self.inspect_ack_timeout(sql_queue)
                self.size = sql_queue.qsize() + self.result_queue.qsize()
                self._ack_keys = sql_queue.ack_keys()
                if self.re_flag:
                    sql_queue.re_data()
                    self.re_flag = True
                if self._clear:
                    sql_queue.clear()
                    self._clear = False
                    continue

                while self.ack_put_queue.qsize():
                    sql_queue.cursor.execute("BEGIN TRANSACTION")
                    [sql_queue.ack_put(*self.ack_put_queue.get()) for i in range(self.ack_put_queue.qsize())]
                    sql_queue.conn.commit()
                    self._ack_keys = sql_queue.ack_keys()
                    continue

                while self.ack_delete_queue.qsize() and self.ack_put_queue.qsize() == 0:
                    sql_queue.ack_delete([self.ack_delete_queue.get() for _ in range(self.ack_delete_queue.qsize())])
                    self._ack_keys = sql_queue.ack_keys()
                    continue

                while self.get_queue.qsize() and sql_queue.qsize() and self.result_queue.qsize() < 10:
                    self.get_queue.get()
                    if self.get_count_limit == 1:
                        self.result_queue.put(sql_queue.get())
                    else:
                        [self.result_queue.put(res) for res in sql_queue.get_mul(self.get_count_limit)]
                    continue

                while self.put_queue.qsize():
                    sql_queue.put(self.put_queue.get_nowait())
                    if self.put_queue.qsize() > self.get_count_limit:
                        d = []
                        try:
                            for _ in range(100):
                                d.append(self.put_queue.get_nowait())
                        except Empty:
                            print("put_queue is empty")
                        sql_queue.put_mul(d)

                if self._close:
                    sql_queue.close()
                    break

                time.sleep(1)
            except Exception as e:
                print(e, e.__traceback__.tb_lineno, self.topic)
        print("队列结束！！！！")

    def start(self):
        threading.Thread(target=self.run).start()
        # threading.Thread(target=self.direct).start()

    def get(self):
        if self.result_queue.qsize():
            try:
                return self.result_queue.get_nowait()
            except Empty:
                return None
        if self.get_queue.qsize() < 10:
            self.get_queue.put(1)

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

    @staticmethod
    def waiting_queue(q):
        while q.qsize():
            time.sleep(0.5)

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

    # 直连
    def direct(self):
        while self.switch:
            if self.put_queue.qsize() and self.get_queue.qsize():
                self.get_queue.get()
                self.result_queue.put((0, self.put_queue.get(), ""))
            else:
                time.sleep(1)


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
