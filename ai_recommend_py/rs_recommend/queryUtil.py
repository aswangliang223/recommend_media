# -*- coding:utf-8 -*-
__author__ = 'wangliang'
from rs_recommend.dbUtil import BaseUtil


class QueryUtil(object):
    '''
    SQL 语句拼接工具类：
    - 主方法: querySql(sql, filters)
    参数说明:
    - @param sql：需要拼接的 SQL 语句
    - @param filters：拼接 SQL 的过滤条件
    filters 过滤条件说明：
    - 支持拼接条件如下：
    - 1、等于（如：{"id": 2}, 拼接后为：id=2)
    - 2、不等于（如：{"_ne_id": 2}, 拼接后为：id != 2）
    - 3、小于（如：{"_lt_id": 2}，拼接后为：id < 2）
    - 4、小于等于（如：{"_le_id": 2}，拼接后为：id <= 2）
    - 5、大于（如：{"_gt_id": }，拼接后为：id > 2）
    - 6、大于等于（如：{"_ge_id": }，拼接后为：id >=2）
    - 7、in（如：{"_in_id": "1,2,3"}，拼接后为：id IN(1,2,3)）
    - 8、not in（如：{"_nein_id": "4,5,6"}，拼接后为：id NOT IN(4,5,6)）
    - 9、like（如：{"_like_name": }，拼接后为：name LIKE '%zhang%'）
    - 10、like（如：{"_llike_name": }，拼接后为：name LIKE '%zhang'）
    - 11、like（如：{"_rlike_name": }，拼接后为：name LIKE 'zhang%'）
    - 12、分组（如：{"groupby": "status"}，拼接后为：GROUP BY status）
    - 13、排序（如：{"orderby": "createDate"}，拼接后为：ORDER BY createDate）
    '''
    NE = "_ne_"  # 拼接不等于
    LT = "_lt_"  # 拼接小于
    LE = "_le_"  # 拼接小于等于
    GT = "_gt_"  # 拼接大于
    GE = "_ge_"  # 拼接大于等于
    IN = "_in_"  # 拼接 in
    NE_IN = "_nein_"  # 拼接 not in
    LIKE = "_like_"  # 拼接 like
    LEFT_LIKE = "_llike_"  # 拼接左 like
    RIGHT_LIKE = "_rlike_"  # 拼接右 like
    GROUP = "groupby"  # 拼接分组
    ORDER = "orderby"  # 拼接排序
    ORDER_TYPE = "ordertype"  # 排序类型：asc（升序）、desc（降序）

    @staticmethod
    def __filter_params(filters):
        '''过滤参数条件'''
        res = " WHERE 1=1"
        for key, value in filters.items():
            if key.startswith(QueryUtil.IN):  # 拼接 in
                res += " AND `%s` IN (" % (key[len(QueryUtil.IN):])
                value_list = value.split(",")
                for value in value_list:
                    res += " %s," % value
                res = res[0:len(res) - 1] + ") "
            elif key.startswith(QueryUtil.NE_IN):  # 拼接 not in
                res += " AND `%s` NOT IN (" % (key[len(QueryUtil.NE_IN):])
                value_list = value.split(",")
                for value in value_list:
                    res += " %s," % value
                res = res[0:len(res) - 1] + ") "
            elif key.startswith(QueryUtil.LIKE):  # 拼接 like
                res += " AND `%s` LIKE '%%%s%%' " % (key[len(QueryUtil.LIKE):], value)
            elif key.startswith(QueryUtil.LEFT_LIKE):  # 拼接左 like
                res += " AND `%s` LIKE '%%%s' " % (
                    key[len(QueryUtil.LEFT_LIKE):], value)
            elif key.startswith(QueryUtil.RIGHT_LIKE):  # 拼接右 like
                res += " AND `%s` LIKE '%s%%' " % (
                    key[len(QueryUtil.RIGHT_LIKE):], value)
            elif key.startswith(QueryUtil.NE):  # 拼接不等于
                res += " AND `%s` != '%s' " % (key[len(QueryUtil.NE):], value)
            elif key.startswith(QueryUtil.LT):  # 拼接小于
                res += " AND `%s` < '%s' " % (key[len(QueryUtil.LT):], value)
            elif key.startswith(QueryUtil.LE):  # 拼接小于等于
                res += " AND `%s` <= '%s' " % (key[len(QueryUtil.LE):], value)
            elif key.startswith(QueryUtil.GT):  # 拼接大于
                res += " AND `%s` > '%s' " % (key[len(QueryUtil.GT):], value)
            elif key.startswith(QueryUtil.GE):  # 拼接大于等于
                res += " AND `%s` >= '%s' " % (key[len(QueryUtil.GE):], value)
            else:  # 拼接等于
                if isinstance(value, str):
                    res += " AND `%s`='%s' " % (key, value)
                elif isinstance(value, int):
                    res += " AND `%s`=%d " % (key, value)
        return res

    @staticmethod
    def __filter_group(filters):
        '''过滤分组'''
        group = filters.pop(QueryUtil.GROUP)
        res = " GROUP BY %s" % (group)
        return res

    @staticmethod
    def __filter_order(filters):
        '''过滤排序'''
        order = filters.pop(QueryUtil.ORDER)
        order_type = filters.pop(QueryUtil.ORDER_TYPE, "asc")
        res = " ORDER BY `%s` %s" % (order, order_type)
        return res

    @staticmethod
    def __filter_page(filters):
        '''过滤 page 对象'''
        page = filters.pop("page")
        return " LIMIT %d,%d" % (page.start_row, page.end_row)

    @staticmethod
    def query_sql(sql=None, filters=None):
        '''拼接 SQL 查询条件
        - @param sql SQL 语句
        - @param filters 过滤条件
        - @return 返回拼接 SQL
        '''
        if filters is None:
            return sql
        else:
            if not isinstance(filters, dict):
                raise Exception("Parameter [filters] must be dict.")
            group = None
            order = None
            page = None
            if filters.get("groupby") != None:
                group = QueryUtil.__filter_group(filters)
            if filters.get("orderby") != None:
                order = QueryUtil.__filter_order(filters)
            if filters.get("page") != None:
                page = QueryUtil.__filter_page(filters)
            sql += QueryUtil.__filter_params(filters)
            if group:
                sql += group
            if order:
                sql += order
            if page:
                sql += page
        return sql


def _test1():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test",
        "table": "province"
    }
    # 指定初始化 table
    test_dao = BaseUtil(**CONFIG)

    # 查询单条记录
    # one = test_dao.select_one()
    # print(one)

    # 查询所有记录
    # all = test_dao.select_all()
    # print(all)

    # 查询分页记录
    # page = test_dao.select_page()
    # print(page)

    # 按主键查询
    one_pk = test_dao.select_pk(primary_key=1)
    print(one_pk)


def _test2():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test"
    }
    # 初始化所有 table
    test_dao = BaseUtil(**CONFIG)

    # one1 = test_dao.select_one("province")
    # print(one1)
    # one2 = test_dao.select_one("city")
    # print(one2)

    # filters1 = {
    #     QueryUtil.GE + "id": 5,
    #     QueryUtil.LT + "id": 30,
    #     QueryUtil.ORDER: "id",
    #     QueryUtil.ORDER_TYPE: "desc"
    # }
    # all_filters = test_dao.select_all("province", filters1)
    # print(all_filters)

    filters2 = {
        QueryUtil.LEFT_LIKE + "province": "省",
    }
    page = Page(1, 20)
    page_filters = test_dao.select_page("province", page, filters2)
    print(page_filters)


def _test3():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test",
        "table": "province"
    }
    # 初始化所有 table
    test_dao = BaseUtil(**CONFIG)
    province = {
        "id": None,
        "province_id": "990000",
        "province": "测试"
    }
    test_dao.save(obj=province)

    # f1 = {
    #     "province": "测试"
    # }
    # item = test_dao.select_one(filters=f1)
    # print(item)
    # item["province"] = "测试1"
    # test_dao.update_by_primarikey_selective(obj=item)

    # item["province"] = "测试2"
    # test_dao.update_by_primarykey("province", item)

    # f2 = {
    #     "province": "测试1"
    # }
    # item = test_dao.select_one(filters=f2)
    # test_dao.remove_by_primarykey(value=item["id"])


class Page(object):
    def __init__(self, page, nums):
        self._page = page
        self._nums = nums


if __name__ == '__main__':
    # _test1()
    # _test2()
    _test3()
