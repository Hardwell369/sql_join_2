"""连接(join)两个SQL"""
import os

from bigmodule import I  # noqa: N812

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据处理"
# 模块显示名
friendly_name = "SQL合并 2"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/"
# 是否自动缓存结果
cacheable = True


LEARNING_ALGORITHMS = {
    "rank": "lambdarank",
    "lambdarank": "lambdarank",
    "regression": "regression",
    "binaryclassification": "binaryclassification",
    "排序": "lambdarank",
    "回归": "regression",
    "二分类": "binaryclassification",
    "logloss": "logloss",
}
FAI_CLUSTERS = {"不加速": None}


SQL_JOIN = """WITH
sql1 AS (
    {sql1}
),
sql2 AS (
    {sql2}
)

SELECT * FROM sql1 JOIN sql2 USING (date, instrument)
"""


def run(
    sql1: I.port("sql 1，SQL 1", specific_type_name="DataSource"),
    sql2: I.port("sql 2，SQL 2", specific_type_name="DataSource"),
    sql_join: I.code("因子与标注合并SQL", default=SQL_JOIN, auto_complete_type="sql") = SQL_JOIN,
) -> [I.port("数据", "data")]:
    """DAI 合并SQL。"""
    import dai

    # 拆分features_sql和label
    sql_statements_1 = dai._functions.parse_query(sql1.read_text())
    sql_statements_2 = dai._functions.parse_query(sql2.read_text())

    join_sql = sql_join.format(sql1=sql_statements_1.pop(), sql2=sql_statements_2.pop())
    join_sql = ";\n".join(list(set(sql_statements_1 + sql_statements_2))) + ";\n" + join_sql

    data = dai.DataSource.write_text(join_sql)
    return I.Outputs(data=data)


def post_run(outputs):
    """后置运行函数"""
    return outputs

