# coding:utf-8
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.HbaseUtil import HbaseUtil
from util.FeiShuUtil import FeiShuUtil
import time


class StoreCheck(object):
    @staticmethod
    def get_top_store_length_day(date_str=time.strftime("%Y%m%d", time.localtime()), top_num=30):
        """
        :return: 占用HDFS空间TOP
        """
        hive_table_store_dict = dict()

        all_metadata_list = HbaseUtil.get_all_metadata_day(date_str)
        for row_key, data in all_metadata_list:
            action_id, hive_table = row_key.split("__", 1)
            store_length = float(data["store:length"]) if data["store:length"] not in ("None", "") else 0
            directory_count = data["store:directory_count"] if data["store:directory_count"] not in ("None", "") else 0
            file_count = data["store:file_count"] if data["store:file_count"] not in ("None", "") else 0
            owner = data["scheduler:owner"] if data["scheduler:owner"] not in ("None", "") else ""
            if hive_table not in hive_table_store_dict:
                hive_table_store_dict[hive_table] = [store_length, directory_count, file_count, owner]
            else:
                if store_length > hive_table_store_dict[hive_table][0]:
                    hive_table_store_dict[hive_table][0] = store_length
                    hive_table_store_dict[hive_table][1] = directory_count
                    hive_table_store_dict[hive_table][2] = file_count
                    hive_table_store_dict[hive_table][3] = owner

        msg = '{:<145}{:<25}{:<15}{:<15}\n'.format("表名", "空间(MB)", "目录数", "文件数")
        for hive_table, (store_length, directory_count, file_count, owner) in \
                sorted(hive_table_store_dict.items(), key=lambda x: x[1][0], reverse=True)[:top_num]:
            msg += '%s%s%s%s\n' % (FeiShuUtil.format_str("("+owner+") " + hive_table, 80),
                FeiShuUtil.format_str(str(store_length), 17), FeiShuUtil.format_str(str(directory_count), 8), str(file_count))
        FeiShuUtil.send("占用HDFS空间TOP%s" % top_num, msg)

    @staticmethod
    def hive_table_file_count_daily_add(date_str=time.strftime("%Y%m%d", time.localtime()), top_num=30):
        """
        :return: 文件数新增TOP
        """
        all_metadata_list = HbaseUtil.get_all_metadata_day(date_str)
        hive_table_store_dict = {}
        for row_key, data in all_metadata_list:
            action_id, hive_table = row_key.split("__", 1)
            owner = data["scheduler:owner"] if data["scheduler:owner"] not in ("None", "") else ""
            if "store:file_count" in data and data["store:file_count"] not in ("None", ""):
                file_count = int(data["store:file_count"])
                if hive_table not in hive_table_store_dict:
                    hive_table_store_dict[hive_table] = [file_count, None, owner]
                else:
                    if hive_table_store_dict[hive_table][0] < file_count:
                        hive_table_store_dict[hive_table][0] = file_count

        yesterday = time.strftime("%Y%m%d", time.localtime(time.mktime(time.strptime(date_str, "%Y%m%d")) - 24 * 3600))
        all_metadata_list = HbaseUtil.get_all_metadata_day(yesterday)
        for row_key, data in all_metadata_list:
            action_id, hive_table = row_key.split("__", 1)
            if "store:file_count" in data and data["store:file_count"] not in ("None", ""):
                file_count = int(data["store:file_count"])
                if hive_table in hive_table_store_dict:
                    hive_table_store_dict[hive_table][1] = file_count
                    if hive_table_store_dict[hive_table][1] < file_count:
                        hive_table_store_dict[hive_table][1] = file_count

        hive_table_daily_add_dict = {}
        for hive_table in hive_table_store_dict:
            if hive_table_store_dict[hive_table][1] is not None:
                hive_table_daily_add_dict[hive_table] = \
                    (hive_table_store_dict[hive_table][0] - hive_table_store_dict[hive_table][1],
                     hive_table_store_dict[hive_table][2])

        msg = '{:<180}{:<}\n'.format("表名", "新增文件数")
        for hive_table, (file_count_daily_add, owner) in \
                sorted(hive_table_daily_add_dict.items(), key=lambda x: x[1], reverse=True)[:top_num]:
            msg += '%s%s\n' % (FeiShuUtil.format_str("(" + owner + ") " + hive_table, 100), str(file_count_daily_add))
        FeiShuUtil.send("文件数新增TOP%s" % top_num, msg)


if __name__ == "__main__":
    StoreCheck.get_top_store_length_day()
    StoreCheck.hive_table_file_count_daily_add()
