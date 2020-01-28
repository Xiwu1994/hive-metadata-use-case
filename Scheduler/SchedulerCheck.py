# coding:utf-8
import sys
import ConfigParser
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.HbaseUtil import HbaseUtil
from util.FeiShuUtil import FeiShuUtil


class SchedulerCheck(object):
    cf = ConfigParser.ConfigParser()
    cf.read(os.path.abspath(os.path.dirname(__file__)) + "/config.ini")

    @staticmethod
    def check(date_str=time.strftime("%Y%m%d", time.localtime())):
        # 当前时间戳
        now_timestamp = int(time.time())
        now_time = time.strftime("%Y%m%d %H:%M", time.localtime(now_timestamp))

        for hive_table_name in SchedulerCheck.cf.sections():
            # 限定最晚完成时间
            check_time = SchedulerCheck.cf.get(hive_table_name, "check_time")
            check_exact_time = "%s %s" % (date_str, check_time)

            # 如果 当前时间 和 限定最晚完成时间 相等，判断任务是否执行完成
            if now_time == check_exact_time:
                row_key, data = HbaseUtil.get_hive_table_latest_metadata(date_str, hive_table_name)
                if row_key is None:
                    msg = "%s don't finish now.. now_time: %s" % (hive_table_name, now_time)
                    FeiShuUtil.send("异常-关键任务没按时完成", msg)
                else:
                    finish_time = data["scheduler:end_time"]
                    msg = "%s finish at %s" % (hive_table_name, finish_time)
                    FeiShuUtil.send("通知-关键任务完成时间", msg)


if __name__ == "__main__":
    SchedulerCheck.check()
