import requests
import json
import pandas as pd
import sqlalchemy

# 全国及分城乡居民收支基本情况
income_expenditure_info = "https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgnd&rowcode=zb&colcode=sj&wds=%5B%5D&dfwds=%5B%7B%22wdcode%22%3A%22zb%22%2C%22valuecode%22%3A%22A0A01%22%7D%5D&k1=1618923213076&h=1"

# 数据库连接配置
DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8".format("mysql", "pymysql", "root", "123456", "49.232.12.36", "3306",
                                                      "chart")

if __name__ == '__main__':

    engine = sqlalchemy.create_engine(DB_URI)

    # https请求忽略证书
    r = requests.get(income_expenditure_info, verify=False)
    r.encoding = "utf-8"
    rJson = json.loads(r.text)
    result, label = {}, ["year"]
    for data in list(map(lambda x: (x["wds"][0]["valuecode"], x["wds"][1]["valuecode"], x["data"]["data"]),
                         rJson["returndata"]["datanodes"])):
        if data[0] not in label:
            label.append(data[0])
        if data[1] not in result.keys():
            result[data[1]] = [data[1], data[2]]
        else:
            result[data[1]].append(data[2])
    data = pd.DataFrame(tuple(result.values()), columns=label)
    print(data)
    # 数据库写入数据集
    data.to_sql(name="income_expenditure_info", con=engine, if_exists="replace", index=True)

    contrast = pd.DataFrame(list(map(lambda x: (x["cname"], x["code"]), rJson["returndata"]["wdnodes"][0]["nodes"])),
                            columns=['chinese_name', 'english_name'])
    # 数据库写入中英文对照表
    contrast.to_sql(name="income_expenditure_info_contrast", con=engine, if_exists="replace", index=True)
