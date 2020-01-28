# coding:utf-8
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.HbaseUtil import HbaseUtil
from util.FeiShuUtil import FeiShuUtil


class ComputingCheck(object):
    @staticmethod
    def find_max_tasks(date_str=time.strftime("%Y%m%d", time.localtime()), top_num=30):
        hive_table_dict = dict()
        for row_key, data in HbaseUtil.get_computing_high_task_day(date_str):
            action_id, hive_table = row_key.split("__", 1)
            hive_table_dict.setdefault(hive_table, [0, "", ""])
            for computing_key in data:
                map_reduce_key = computing_key.split(":")[1].split("__")[0].split("_")[0]
                job_id = computing_key.split(":")[1].split("__")[1]
                task_nums = int(data[computing_key])
                if task_nums > hive_table_dict[hive_table][0]:
                    hive_table_dict[hive_table][0] = task_nums
                    hive_table_dict[hive_table][1] = map_reduce_key
                    hive_table_dict[hive_table][2] = job_id

        msg = '{:<120}{:<20}{:<10}{:<}\n'.format("表名", "Map or Reduce", "task数量", "yarn job id")
        for hive_table, (task_nums, map_reduce_key, job_id) in \
            sorted(hive_table_dict.items(), key=lambda x: x[1][0], reverse=True)[:top_num]:
            owner = HbaseUtil.get_hive_table_assign_metadata(date_str, hive_table, "scheduler", "owner")
            msg += '%s%s%s%s\n' % (FeiShuUtil.format_str('('+owner+') ' + hive_table, 70),
                FeiShuUtil.format_str(map_reduce_key, 10),
                FeiShuUtil.format_str(str(task_nums), 5), job_id)
        FeiShuUtil.send("YARN TASK 任务数TOP%s" % top_num, msg)

    @staticmethod
    def find_skew_hive_table(date_str=time.strftime("%Y%m%d", time.localtime())):
        long_job_set = set()

        # 找到 yarn job 执行时间较长的任务
        for row_key, data in HbaseUtil.get_computing_last_long_job_day(date_str):
            for qualifier in data:
                job_id = qualifier.split("__")[1]
                long_job_set.add("%s__%s" % (row_key, job_id))

        # 找到 有数据倾斜的任务
        msg = '{:<120}{:<}\n'.format("表名", "yarn job id")
        for row_key, data in HbaseUtil.get_computing_abnormal_skew_day(date_str):
            action_id, hive_table = row_key.split("__", 1)
            for qualifier in data:
                job_id = qualifier.split("__")[1]
                # 数据倾斜 & 执行时间长
                if "%s__%s" % (row_key, job_id) in long_job_set:
                    owner = HbaseUtil.get_hive_table_assign_metadata(date_str, hive_table, "scheduler", "owner")
                    msg += '%s%s\n' % (FeiShuUtil.format_str('(' + owner + ') ' + hive_table, 70), job_id)
        FeiShuUtil.send("数据倾斜的任务详情", msg)


if __name__ == "__main__":
    ComputingCheck.find_max_tasks()
    ComputingCheck.find_skew_hive_table()
