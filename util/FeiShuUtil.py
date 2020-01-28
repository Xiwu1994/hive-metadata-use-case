# coding:utf-8
import requests
import json

url = "https://open.feishu.cn/open-apis/bot/hook/xxxxx"


class FeiShuUtil(object):
    @staticmethod
    def format_str(str, num):
        return str + (num - len(str)) * "  "

    @staticmethod
    def send(title, text):
        data = {
            "title": "%s" % title,
            "text": "%s" % text
        }
        headers = {"Content-Type": "application/json"}
        requests.post(url, json.dumps(data), headers=headers)


if __name__ == "__main__":
    FeiShuUtil.send("1", "2")
