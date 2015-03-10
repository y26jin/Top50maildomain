__author__ = 'yuhang'
import os
import sys
import random
import time
import datetime
import string
import pymysql
import threading
import heapq
import logging
import logging.handlers

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Connector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self,name="connector")

        log_path = "test.log"
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=104857600, backupCount=10)
        logger.addHandler(handler)

        self.logger = logger

        self.generate_data_thread = GenerateData(self.logger)
        self.show_ranking_thread = AnalyzeData(self.logger)

        self.generate_data_thread.daemon = True
        self.show_ranking_thread.daemon = True

    def run(self):
        self.generate_data_thread.start()
        self.show_ranking_thread.start()

        self.generate_data_thread.join()
        if not self.generate_data_thread.isAlive():
            self.logger.debug("Data generator has finished.")

        self.show_ranking_thread.join()
        if not self.show_ranking_thread.isAlive():
            self.logger.debug("Data analyzer has finished.")

        time.sleep(1)

class GenerateData(threading.Thread):
    def __init__(self,logger):
        threading.Thread.__init__(self,name="generator")
        self.logger=logger
        self.db_conn = pymysql.connect(host='localhost',
                                       port=3306,
                                       user='yuhang',
                                       passwd='',
                                       db='test',
                                       autocommit=True)
        self.db_cursor = self.db_conn.cursor()

    def rand_email(self):
        len = random.randint(1,1000000) % 10 + 5
        name = ''.join(random.SystemRandom().choice(string.lowercase+string.digits) for _ in xrange(len))
        domain = ''.join(random.SystemRandom().choice(string.lowercase) for _ in xrange(3))
        domain += '.com'
        return name + "@" + domain

    def rand_date(self, start, end, prop):
        format = '%Y/%m/%d'
        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)

        return time.strftime(format, time.localtime(ptime))

    def run(self):
        addr_count = 0
        while addr_count < 1000:
            addr = self.rand_email()
            try:
                query = 'insert ignore into mailing (addr) values ("'+addr+'");'
                self.db_cursor.execute(query)
            except Exception as e:
                raise e

            domain = addr.split("@")[1]
            date = self.rand_date("2014/1/1", "2014/6/1", random.random())
            try:
                query = 'insert into domain (domain_date,domain_name) values ("'+str(date)+'", "'+domain+'");'
                self.db_cursor.execute(query)
                query = 'update domain set domain_count=domain_count+1 where domain_date="'+str(date)+'" and domain_name="'+domain+'";'
                self.db_cursor.execute(query)
            except Exception as e:
                raise e

            addr_count = addr_count + 1
            time.sleep(0.1)

class AnalyzeData(threading.Thread):
    def __init__(self,logger):
        threading.Thread.__init__(self,name="analyzer")
        self.logger = logger
        self.db_conn = pymysql.connect(host='localhost',
                                       port=3306,
                                       user='yuhang',
                                       passwd='',
                                       db='test',
                                       autocommit=True)
        self.db_cursor = self.db_conn.cursor()

    def run(self):
        time.sleep(100)
        top_count = 0
        domain_total = dict()

        while top_count < 1:
            # get the total number first
            self.db_cursor.execute('select domain_date,domain_name,domain_count from domain where domain_date>="2014/1/1" and domain_date<="2014/6/1";')
            results = self.db_cursor.fetchall()

            for ret in results:
                if not ret[1] in domain_total:
                    domain_total[ret[1]] = int(ret[2])
                else:
                    domain_total[ret[1]] += int(ret[2])

            # get each month
            for month in range(1,6):
                ranking = []
                monthly = dict()
                month_start = dict()
                startkey = "2014/"+str(month)+"/1"
                endkey = "2014/"+str(month+1)+"/1"
                query = 'select domain_date,domain_name,domain_count from domain where domain_date>="'+startkey+'" and domain_date<="'+endkey+'";'
                self.db_cursor.execute(query)
                results = self.db_cursor.fetchall()
                for ret in results:
                    if not ret[1] in monthly:
                        monthly[ret[1]] = int(ret[2])
                    else:
                        monthly[ret[1]] += int(ret[2])
                query = 'select domain_date,domain_name,domain_count from domain where domain_date="'+startkey+'";'
                self.db_cursor.execute(query)
                results = self.db_cursor.fetchall()
                for ret in results:
                    if not ret[1] in month_start:
                        month_start[ret[1]] = int(ret[2])
                    else:
                        month_start[ret[1]] += int(ret[2])
                for key in monthly:
                    if not key in domain_total:
                        continue

                    growth = monthly[key]
                    if key in month_start:
                        growth -= month_start[key]
                    total = domain_total[key]
                    heapq.heappush(ranking, (float(growth/total), key))
                top50 = heapq.nlargest(50,ranking)
                print "Top 50 domains from "+startkey+" to "+endkey
                print top50
                self.logger.debug('To 50 domains from '+startkey+" to "+endkey)
                self.logger.debug(top50)
            top_count+=1
            time.sleep(1)


if __name__=="__main__":
    connector = Connector()
    connector.start()
    if not connector.isAlive():
        connector.close()
