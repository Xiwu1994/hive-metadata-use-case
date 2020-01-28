# coding:utf-8
import happybase


class HbaseUtil(object):
    connection = happybase.Connection(host="xxxx", port=9090)
    table = connection.table("hive_metadata_collect")

    @staticmethod
    def put(row_key, data):
        HbaseUtil.table.put(row_key, data)

    @staticmethod
    def get_hive_table_metadata_day(date_str, hive_table):
        query_str = """RowFilter(=,'regexstring:%s[^_]*__%s')""" % (date_str, hive_table)
        return HbaseUtil.table.scan(filter=query_str, row_prefix=date_str)

    @staticmethod
    def get_computing_abnormal_skew_day(date_str):
        query_str = """FamilyFilter(=,'substring:computing') AND QualifierFilter(=,'regexstring:abnormal_index') AND ValueFilter(=,'regexstring:Skew')"""
        return HbaseUtil.table.scan(filter=query_str, row_prefix=date_str)

    @staticmethod
    def get_computing_last_long_job_day(date_str):
        query_str = """FamilyFilter(=,'substring:computing') AND QualifierFilter(=,'regexstring:last_time') AND ValueFilter(=,'regexstring:\d{3,}')"""
        return HbaseUtil.table.scan(filter=query_str, row_prefix=date_str)

    @staticmethod
    def get_hive_table_assign_metadata(date_str, hive_table, family, qualifier):
        result = None
        query_str = """RowFilter(=,'regexstring:%s[^_]*__%s') AND FamilyFilter(=,'substring:%s') AND QualifierFilter(=,'substring:%s')""" % (
        date_str, hive_table, family, qualifier)
        for row_key, data in HbaseUtil.table.scan(filter=query_str, row_prefix=date_str):
            result = data["%s:%s" % (family, qualifier)]
        return result

    @staticmethod
    def get_computing_high_task_day(date_str):
        query_str = """FamilyFilter(=,'substring:computing') AND QualifierFilter(=,'regexstring:[(mapper|reduce)]_tasks_num_.*') AND ValueFilter(=,'regexstring:\d{3,}')"""
        return HbaseUtil.table.scan(filter=query_str, row_prefix=date_str)

    @staticmethod
    def get_hive_table_latest_metadata(date_str, hive_table):
        row_key, data = None, None
        for row_key, data in HbaseUtil.get_hive_table_metadata_day(date_str, hive_table):
            continue
        return row_key, data

    @staticmethod
    def get_all_metadata_day(date_str):
        return HbaseUtil.table.scan(row_prefix=date_str)


if __name__ == "__main__":
    print HbaseUtil.get_hive_table_assign_metadata("20200127", "db_name.table_name", "scheduler", "owner")
