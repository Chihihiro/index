import re
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
from utils.algorithm import timeutils as tu

update_time = {
    # "incremental": dt.datetime.fromtimestamp(time.mktime((dt.date.today() - relativedelta(days=2)).timetuple())),
    "incremental": dt.datetime.today() - relativedelta(days=2),
    "all": dt.datetime(2017, 1, 1)
}


class Similarity:
    @staticmethod
    def similarity_name(lc, rc, ignore=None):
        """
            Compare the similarity between two names, return
        Args:
            lc: str
                string for comparing;
            rc: str
                string to be compared with;
            ignore: list
                If ignore list is passed, words in the list will be ignored when comparing;
        Returns:
            Similarity <float> range from 0 to 1
        """
        # if len(lc) > len(rc):
        #     lc, rc = rc, lc

        if ignore is not None:
            for ign in ignore:
                lc = lc.replace(ign, "")
                rc = rc.replace(ign, "")

        if rc.find(lc) >= 0:
            return 1.0
        else:
            length = len(lc)
            lidx, ridx = 0, 0
            matched, unmatched = 0, 0
            for lchar in lc:
                for idx in range(len(rc[ridx:])):
                    if lchar == rc[ridx:][idx]:
                        ridx = idx + 1
                        matched += 1
                        break
            return matched / length

    @staticmethod
    def similarity_time(lc, rc, tolerance=45):
        if lc is None or rc is None:
            return 0
        lc, rc = tu.universal_time(lc)[1], tu.universal_time(rc)[1]
        return 1 - abs(lc - rc) / 86400 / tolerance

    @staticmethod
    def compare_ls(lc, ls, ignore=None):
        result = []
        for rc in ls:
            result.append((rc, Similarity.similarity_name(lc, rc, ignore)))
        return sorted(result, key=lambda x: x[1])[-1]


class Unit:
    UNITS = {
        "": 1,
        "百": 1e2,
        "千": 1e3,
        "万": 1e4,
        "亿": 1e8,
    }

    @staticmethod
    def tr(unit, target_unit=None):
        """
            Transform original unit to target unit, and return the scale
        Args:
            unit: str or None, optional {百, 千, 万, 亿, None};
                If `unit` is None, then the `target_unit` will be equal to `unit`, and scale will be 1;
            target_unit: str or None, optional {百, 千, 万, 亿, None};

        Returns:
            Scale<float>
        """
        if target_unit is None:
            return 1
        else:
            return Unit.UNITS[unit] / Unit.UNITS[target_unit]


class Patterns:
    num_patt = "(?P<num>(\d*)(\.\d*)?)"
    unit_patt = "(?P<unit>(万亿|万|亿))"


class StringParser:
    @staticmethod
    def filter(string, correct_fields=None, wrong_fields=None):
        pass

    @staticmethod
    def percentage(percentage_str):
        patt = "{num_patt}".format(num_patt=Patterns.num_patt)
        sre = re.search(patt, percentage_str)
        if sre is not None:
            try:
                num = float(sre.groupdict()["num"]) / 100
                return num
            except ValueError:
                return None

        else:
            return None

    @staticmethod
    def num_with_unit(num_str, target_unit=None):
        patt = "{num_patt}{unit_patt}".format(num_patt=Patterns.num_patt, unit_patt=Patterns.unit_patt)
        sre = re.search(patt, num_str)
        if sre is not None:
            num = sre.groupdict()["num"]
            unit = sre.groupdict()["unit"]
            if num != "" and unit != "":
                return float(num) * Unit.tr(unit, target_unit)
            else:
                return None
        else:
            return None

    @staticmethod
    def num_without_unit(num_str):
        patt = Patterns.num_patt
        sre = re.search(patt, num_str)
        if sre is not None:
            num = sre.groupdict()["num"]
            if num != "":
                return float(num)
            else:
                return None
        else:
            return None


def generate_attr_dict(dataframe, key_fields, value_fields):
    """
        Generate attribute dictionary of each object.

    Args:
        dataframe:
        key_fields: list

        value_fields: list
    Returns:
        dict
        e.g.
        {
            (key_field_1, key_field_2): {value_fields_1: val, value_fields_2: val, ...},
            ......
        }
    """
    if len(key_fields) == 1:
        keys = dataframe[key_fields[0]]
    else:
        keys = tuple(zip(*dataframe[key_fields].as_matrix().T))

    attrdict = dataframe[value_fields].to_dict(orient="records")
    return dict(zip(keys, attrdict))


class MultisourceDict(dict):
    def __missing__(self, key):
        return key[1]


class Enum:
    class DataSource:
        code = {
            "howbuy": "020001",
            "eastmoney": "020002",
            "fund123": "020003"
        }

    class SWS:
        # 申万一级行业分类代码名称
        name_1 = {
            "110000": "农林牧渔",
            "210000": "采掘",
            "220000": "化工",
            "230000": "钢铁",
            "240000": "有色金属",
            "270000": "电子",
            "280000": "汽车",
            "330000": "家用电器",
            "340000": "食品饮料",
            "350000": "纺织服装",
            "360000": "轻工制造",
            "370000": "医药生物",
            "410000": "公用事业",
            "420000": "交通运输",
            "430000": "房地产",
            "450000": "商业贸易",
            "460000": "休闲服务",
            "480000": "银行",
            "490000": "非银金融",
            "510000": "综合",
            "610000": "建筑材料",
            "620000": "建筑装饰",
            "630000": "电气设备",
            "640000": "机械设备",
            "650000": "国防军工",
            "710000": "计算机",
            "720000": "传媒",
            "730000": "通信",
        }

    class Wind:
        industry_code = {
            "A": "农、林、渔、牧业",
            "B": "采矿业",
            "C": "制造业",
            "D": "电力、热力、燃气及水生产和供应业",
            "E": "建筑业",
            "F": "批发和零售业",
            "G": "交通运输、仓储和邮政业",
            "H": "住宿和餐饮业",
            "I": "信息传输、软件和信息技术服务业",
            "J": "金融业",
            "K": "房地产业",
            "L": "租赁和商务服务业",
            "M": "科学研究和技术服务业",
            "N": "水利、环境和公共设施管理业",
            "P": "教育",
            "Q": "卫生和社会工作",
            "R": "文化、体育和娱乐业",
            "S": "综合",
        }

    class TypeMapping:
        typestandard_name = {
            "01": "运行方式",
            "02": "投资标的",
            "03": "投资期限",
            "04": "投资方式",
            "05": "分级基金",
        }

        type_name = {
            "0101": "封闭式",
            "0102": "开放式",
            "0201": "股票型基金",
            "0202": "债券型基金",
            "0203": "混合型基金",
            "0204": "货币型基金",
            "0205": "QDII基金",
            "0299": "其他基金",
            "0301": "短期",
            "0302": "中长期",
            "0399": "其他",
            "0401": "主动型",
            "0402": "指数型",
            "0499": "其他",
            "0501": "母基金",
            "0502": "一级基金",
            "0503": "二级基金",
        }

        stype_name = {
            "020201": "纯债型",
            "020202": "混合债券型",
            "020301": "偏股混合型",
            "020302": "偏债混合型",
            "020303": "平衡混合型",
            "020304": "灵活配置型",
            "020305": "FOF",
            "020399": "其他",
            "020501": "QDII股票型基金",
            "020502": "QDII混合型基金",
            "020503": "QDII债券型基金",
            "020504": "QDII另类投资基金",
            "029901": "商品型基金",
            "029902": "REITS",
            "029903": "另类投资基金",
            "029999": "另类投资基金",
            "040201": "被动指数型",
            "040202": "指数增强型",
            "040299": "其他",

        }


class EnumMap:
    class DFundInfo:
        __to__ = "fund_info"
        fund_status = {
            (Enum.DataSource.code["howbuy"], "封闭期"): "封闭期",
            (Enum.DataSource.code["howbuy"], "正常"): "正常",
            (Enum.DataSource.code["howbuy"], "认购期"): "认购期",
            (Enum.DataSource.code["eastmoney"], "内场交易"): "ENUM",
            (Enum.DataSource.code["eastmoney"], "封闭期"): "封闭期",
            (Enum.DataSource.code["eastmoney"], "封闭期（单日投资上限10000元）"): "封闭期",
            (Enum.DataSource.code["eastmoney"], "开放申购封闭期"): "封闭期",
        }

        purchase_status = MultisourceDict({
            (Enum.DataSource.code["howbuy"], "申购关闭"): "申购关闭",
            (Enum.DataSource.code["howbuy"], "申购打开"): "申购打开",
            (Enum.DataSource.code["howbuy"], "限额申购"): "申购关闭(限额)",
            (Enum.DataSource.code["eastmoney"], "封闭期"): "申购关闭",
            (Enum.DataSource.code["eastmoney"], "开放申购"): "申购打开",
            (Enum.DataSource.code["eastmoney"], "暂停申购"): "申购关闭",
            (Enum.DataSource.code["eastmoney"], "限大额"): "申购关闭(限额)",
            (Enum.DataSource.code["fund123"], "开放申购"): "申购打开",
            (Enum.DataSource.code["fund123"], "暂停申购"): "申购关闭",
        })

        redemption_status = {
            (Enum.DataSource.code["howbuy"], "赎回关闭"): "赎回关闭",
            (Enum.DataSource.code["howbuy"], "赎回打开"): "赎回打开",
            (Enum.DataSource.code["eastmoney"], "开放赎回"): "赎回打开",
            (Enum.DataSource.code["eastmoney"], "暂停赎回"): "赎回关闭",
            (Enum.DataSource.code["fund123"], "开放赎回"): "赎回打开",
            (Enum.DataSource.code["fund123"], "暂停赎回"): "赎回关闭",
        }

        aip_status = {
            (Enum.DataSource.code["howbuy"], "定投关闭"): "定投关闭",
            (Enum.DataSource.code["howbuy"], "定投打开"): "定投打开",
            (Enum.DataSource.code["eastmoney"], "定投关闭"): "定投关闭",
            (Enum.DataSource.code["eastmoney"], "定投关闭"): "定投打开",
        }

        typestandard_code = {
            (Enum.DataSource.code["howbuy"], "结构型"): "05",
            (Enum.DataSource.code["eastmoney"], "分级杠杆"): "05",
        }

        type_code2 = {
            (Enum.DataSource.code["howbuy"], "QDII"): "0205",
            (Enum.DataSource.code["howbuy"], "保本型"): "0203",
            (Enum.DataSource.code["howbuy"], "债券型"): "0202",
            (Enum.DataSource.code["howbuy"], "混合型"): "0203",
            (Enum.DataSource.code["howbuy"], "理财型"): "0204",
            (Enum.DataSource.code["howbuy"], "股票型"): "0201",
            (Enum.DataSource.code["howbuy"], "货币型"): "0204",
            (Enum.DataSource.code["eastmoney"], "QDII"): "0205",
            (Enum.DataSource.code["eastmoney"], "QDII-指数"): "0205",
            (Enum.DataSource.code["eastmoney"], "保本型"): "0203",
            (Enum.DataSource.code["eastmoney"], "债券型"): "0202",
            (Enum.DataSource.code["eastmoney"], "债券指数"): "0202",
            (Enum.DataSource.code["eastmoney"], "固定收益"): "0202",
            (Enum.DataSource.code["eastmoney"], "定开债券"): "0202",
            (Enum.DataSource.code["eastmoney"], "混合型"): "0203",
            (Enum.DataSource.code["eastmoney"], "股票型"): "0201",
            (Enum.DataSource.code["eastmoney"], "股票指数"): "0201",
            (Enum.DataSource.code["eastmoney"], "货币型"): "0204",
            (Enum.DataSource.code["eastmoney"], "理财型"): "0202",
            (Enum.DataSource.code["eastmoney"], "混合-FOF"): "0203",
            (Enum.DataSource.code["eastmoney"], "债券创新-场内"): "0202",
            (Enum.DataSource.code["eastmoney"], "QDII-ETF"): "0205",
            (Enum.DataSource.code["eastmoney"], "定息基金"): "0202",
            (Enum.DataSource.code["fund123"], "保本型"): "0203",
            (Enum.DataSource.code["fund123"], "QDII"): "0205",
            (Enum.DataSource.code["fund123"], "股票型"): "0201",
            (Enum.DataSource.code["fund123"], "混合型"): "0203",
            (Enum.DataSource.code["fund123"], "现金型"): "0204",
            (Enum.DataSource.code["fund123"], "债券型"): "0202",
        }

        type_code4 = {
            (Enum.DataSource.code["howbuy"], "指数型"): "0402",
            (Enum.DataSource.code["eastmoney"], "债券指数"): "0402",
            (Enum.DataSource.code["eastmoney"], "股票指数"): "0402",
            (Enum.DataSource.code["fund123"], "指数型"): "0402",
            (Enum.DataSource.code["fund123"], "QDUU-ETF"): "0402",
        }

    class DFundPortfolioAsset:
        type = {
            (Enum.DataSource.code["howbuy"], "买入返售证券"): "买入返售金融资产",
            (Enum.DataSource.code["howbuy"], "债券"): "固定收益投资",  #
            (Enum.DataSource.code["howbuy"], "其它"): "其它",  #
            (Enum.DataSource.code["howbuy"], "基金"): "基金投资",  #
            (Enum.DataSource.code["howbuy"], "存托凭证"): "权益投资",  #
            (Enum.DataSource.code["howbuy"], "权证投资市值"): "金融衍生品投资",
            (Enum.DataSource.code["howbuy"], "股票"): "权益投资",  #
            (Enum.DataSource.code["howbuy"], "货币市场工具"): "货币市场工具",  #
            (Enum.DataSource.code["howbuy"], "资产支持证券"): "固定收益投资",  #
            (Enum.DataSource.code["howbuy"], "金融衍生品"): "金融衍生品投资",
            (Enum.DataSource.code["howbuy"], "银行存款"): "现金",
            (Enum.DataSource.code["howbuy"], "货币资金"): "现金"
        }

        stype = {
            (Enum.DataSource.code["howbuy"], "买入返售证券"): "买入返售金融资产",
            (Enum.DataSource.code["howbuy"], "债券"): "债券",
            (Enum.DataSource.code["howbuy"], "其它"): "其它",
            (Enum.DataSource.code["howbuy"], "基金"): "基金",
            (Enum.DataSource.code["howbuy"], "存托凭证"): "存托凭证",
            (Enum.DataSource.code["howbuy"], "权证投资市值"): "权证",
            (Enum.DataSource.code["howbuy"], "股票"): "股票",
            (Enum.DataSource.code["howbuy"], "货币市场工具"): "货币市场工具",
            (Enum.DataSource.code["howbuy"], "资产支持证券"): "资产支持证券",
            (Enum.DataSource.code["howbuy"], "金融衍生品"): "",
            (Enum.DataSource.code["howbuy"], "银行存款"): "现金",
            (Enum.DataSource.code["howbuy"], "货币资金"): "现金",
            # (Enum.DataSource.code["eastmoney"], "货币资金"): "现金",
            # (Enum.DataSource.code["eastmoney"], "股票"): "股票",
            # (Enum.DataSource.code["eastmoney"], "债券"): "债券",
        }

    class DFundPortfolioIndustry:
        type_sws = {
            (Enum.DataSource.code["howbuy"], "001能源"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["howbuy"], "003工业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "006保健"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "008信息技术"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "010公共事业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "15原材料"): Enum.SWS.name_1["620000"],
            (Enum.DataSource.code["howbuy"], "20工业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "25非必需消费品"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "25非日常生活消费品"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "30日常消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "35医疗保健"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["howbuy"], "40金融"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "金融financials"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "45信息技术"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "45信息科技"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "50电信业务"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "CD非必需消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "CS必需消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "FN金融"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "HC医疗"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "IN工业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "IT信息技术"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "UT公用事业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "个人仓储类"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "互联网软件与服务"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "交通运输、仓储和邮政业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "企业债"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "住宅房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "住宅类"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "住宿和餐饮业"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "保健"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "保健HealthCare"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服务"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服务业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "信息技术"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "信息技术InformationTechnology"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "信息科技"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "公共事业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "公共事业Utilities"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "公寓类"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "公用事业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "公用事業"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "其他"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "其它"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "写字楼类"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "写字楼类&房地产股票"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "写字楼类房地产股票"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "农、林、牧、渔业"): Enum.SWS.name_1["110000"],
            (Enum.DataSource.code["howbuy"], "制药"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "制造业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "办公房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "医疗"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "医疗保健"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "医疗保健地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "医疗保健类"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "医疗类"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "卫生和社会工作"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "原材料"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "原材料Materials"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "可转债"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "国债"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "地方性商业中心"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "多元化类"): "ERROR",
            (Enum.DataSource.code["howbuy"], "多样化房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "家庭娱乐软件"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "居民服务、修理和其他服务业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "工业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "工业Industrials"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "工业房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "工业类"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "应用软件"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "建筑与工程"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "建筑业"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "必需消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "必需消费品ConsumerStaples"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "必须消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "房地产"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "房地产业"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "房地产开发"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "房地产股票"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "批发和零售业"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["howbuy"], "投资银行业与经纪业"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "政策性金融债"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "教育"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "数据处理与外包服务"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "文化、体育和娱乐业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "日常消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "服务"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "材料"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "材料Materials"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "水利、环境和公共设施管理业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "汽车制造商"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "消费品"): Enum.SWS.name_1["350000"],
            (Enum.DataSource.code["howbuy"], "消费者常用品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "消费者非必需品"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "物流/工业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "物流\工业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "特殊消费者服务"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "特殊类"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "特种房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "电信"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "电信服务TelecommunicationServices"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "电信业务"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "电信服务"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供应"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供应业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "电影与娱乐"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "科学研究和技术服务业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "科技"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "租赁和商务服务业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "系统软件"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "纸制品"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "综合"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "能源"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["howbuy"], "能源Energy"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["howbuy"], "航空航天与国防"): Enum.SWS.name_1["650000"], #
            (Enum.DataSource.code["howbuy"], "衛生保健"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "财产与意外伤害保险"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "购物中心"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "资本货物"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "通信"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "通信设备"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "通讯"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "通讯业"): Enum.SWS.name_1["730000"],
            (Enum.DataSource.code["howbuy"], "酒店及娱乐地产投资信托"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "采掘业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["howbuy"], "采矿业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["howbuy"], "金融"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "金融Financials"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "金融业"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "金融债"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "零售业"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["howbuy"], "零售业房地产投资信托"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "零售类"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["howbuy"], "非必需品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非必需消费"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非必需消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非必需消费品ConsumerDiscretionary"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非必须消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非日常生活消耗品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "非日常生活消费品"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["eastmoney"], "交通运输、仓储业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["eastmoney"], "交通运输、仓储和邮政业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["eastmoney"], "传播与文化产业"): Enum.SWS.name_1["720000"],
            (Enum.DataSource.code["eastmoney"], "住宿和餐饮业"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["eastmoney"], "信息传输、软件和信息技术服务业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["eastmoney"], "信息技术业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["eastmoney"], "其他制造业"): Enum.SWS.name_1["360000"],   #
            (Enum.DataSource.code["eastmoney"], "农、林、牧、渔业"): Enum.SWS.name_1["110000"],
            (Enum.DataSource.code["eastmoney"], "制造业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 其他制造业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 制造业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 医药、生物制品"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 木材、家具"): Enum.SWS.name_1["330000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 机械、设备、仪表"): Enum.SWS.name_1["630000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 电子"): Enum.SWS.name_1["270000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 石油、化学、塑胶、塑料"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 纺织、服装、皮毛"): Enum.SWS.name_1["350000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 造纸、印刷"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 金属、非金属"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["eastmoney"], "制造业 - 食品、饮料"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["eastmoney"], "卫生和社会工作"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["eastmoney"], "居民服务、修理和其他服务业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["eastmoney"], "建筑业"): Enum.SWS.name_1["620000"],
            (Enum.DataSource.code["eastmoney"], "房地产业"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["eastmoney"], "批发和零售业"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["eastmoney"], "批发和零售贸易"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["eastmoney"], "教育"): Enum.SWS.name_1["510000"],  #
            (Enum.DataSource.code["eastmoney"], "文化、体育和娱乐业"): Enum.SWS.name_1["720000"],  #
            (Enum.DataSource.code["eastmoney"], "机械、设备、仪表"): Enum.SWS.name_1["630000"],  #
            (Enum.DataSource.code["eastmoney"], "水利、环境和公共设施管理业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["eastmoney"], "电力、热力、燃气及水生产和供应业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["eastmoney"], "电力、煤气及水的生产和供应业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["eastmoney"], "社会服务业"): Enum.SWS.name_1["460000"],  #
            (Enum.DataSource.code["eastmoney"], "科学研究和技术服务业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["eastmoney"], "租赁和商务服务业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["eastmoney"], "综合"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["eastmoney"], "综合类"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["eastmoney"], "采掘业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["eastmoney"], "采矿业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["eastmoney"], "金融业"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["eastmoney"], "金融、保险业"): Enum.SWS.name_1["490000"],

        }

        type_wind = {
            # (Enum.DataSource.code["howbuy"], "其它行业"): Enum.Wind.industry_code["S"],
            # (Enum.DataSource.code["howbuy"], "农、林、牧、渔业"): Enum.Wind.industry_code["A"],
            # (Enum.DataSource.code["howbuy"], "制造业"): Enum.Wind.industry_code["C"],
            # (Enum.DataSource.code["howbuy"], "批发和零售业"): Enum.Wind.industry_code["F"],
            # (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供应业"): Enum.Wind.industry_code["D"],
            # (Enum.DataSource.code["howbuy"], "金融业"): Enum.Wind.industry_code["J"],
            # (Enum.DataSource.code["howbuy"], "建筑业"): Enum.Wind.industry_code["E"],
            # (Enum.DataSource.code["howbuy"], "水利、环境和公共设施管理业"): Enum.Wind.industry_code["N"],
            # (Enum.DataSource.code["howbuy"], "采矿业"): Enum.Wind.industry_code["B"],
            # (Enum.DataSource.code["howbuy"], "房地产业"): Enum.Wind.industry_code["K"],
            # (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服务业"): Enum.Wind.industry_code["I"],
            # (Enum.DataSource.code["howbuy"], "卫生和社会工作"): Enum.Wind.industry_code["Q"],
            # (Enum.DataSource.code["howbuy"], "文化、体育和娱乐业"): Enum.Wind.industry_code["R"],
            # (Enum.DataSource.code["howbuy"], "科学研究和技术服务业"): Enum.Wind.industry_code["M"],
            # (Enum.DataSource.code["howbuy"], "交通运输、仓储和邮政业"): Enum.Wind.industry_code["G"],
            # (Enum.DataSource.code["howbuy"], "机械、设备、仪表"): Enum.Wind.industry_code["C"],
            # (Enum.DataSource.code["howbuy"], "金融、保险业"): Enum.Wind.industry_code["J"],
            # (Enum.DataSource.code["howbuy"], "食品、饮料"): Enum.Wind.industry_code["H"],
            # (Enum.DataSource.code["howbuy"], "批发和零售贸易"): Enum.Wind.industry_code["F"],
            # (Enum.DataSource.code["howbuy"], "医药、生物制品"): Enum.Wind.industry_code["M"],
            # (Enum.DataSource.code["howbuy"], "租赁和商务服务业"): Enum.Wind.industry_code["L"],
            # (Enum.DataSource.code["howbuy"], "合计"): "ERROR",
            # (Enum.DataSource.code["howbuy"], "采掘业"): Enum.Wind.industry_code["B"],
            # (Enum.DataSource.code["howbuy"], "信息技术业"): Enum.Wind.industry_code["I"],
            # (Enum.DataSource.code["howbuy"], "电子"): Enum.Wind.industry_code["M"],
            # (Enum.DataSource.code["howbuy"], "金属、非金属"): Enum.Wind.industry_code["B"],
            # (Enum.DataSource.code["howbuy"], "电力、煤气及水的生产和供应业"): Enum.Wind.industry_code["D"],
            # (Enum.DataSource.code["howbuy"], "综合"): Enum.Wind.industry_code["S"],
            # (Enum.DataSource.code["howbuy"], "石油、化学、塑胶、塑料"): Enum.Wind.industry_code["C"],
            # (Enum.DataSource.code["howbuy"], "社会服务业"): Enum.Wind.industry_code["Q"],
            # (Enum.DataSource.code["howbuy"], "交通运输、仓储业"): Enum.Wind.industry_code["G"],
            # (Enum.DataSource.code["howbuy"], "住宿和餐饮业"): Enum.Wind.industry_code["H"],
            # (Enum.DataSource.code["howbuy"], "教育"): Enum.Wind.industry_code["P"],
            # (Enum.DataSource.code["howbuy"], "传播与文化产业"): Enum.Wind.industry_code["R"],
            # (Enum.DataSource.code["howbuy"], "纺织、服装、皮毛"): Enum.Wind.industry_code["C"],
            # (Enum.DataSource.code["howbuy"], "综合类"): Enum.Wind.industry_code["S"],
        }

    class DFundHolder:
        __to__ = "fund_holder"

        holder_type = {
            (Enum.DataSource.code["howbuy"], 1): "机构",
            (Enum.DataSource.code["howbuy"], 2): "个人",
            (Enum.DataSource.code["howbuy"], 3): "内部",
            (Enum.DataSource.code["eastmoney"], 1): "机构",
            (Enum.DataSource.code["eastmoney"], 2): "个人",
            (Enum.DataSource.code["eastmoney"], 3): "内部",
            (Enum.DataSource.code["fund123"], 1): "机构",
            (Enum.DataSource.code["fund123"], 2): "个人",
            (Enum.DataSource.code["fund123"], 3): "内部",
        }

    class DOrgInfo:
        __to__ = "org_info"
        org_type = {
            1: "公募基金公司",
            2: "托管银行"
        }

        form = {
            (Enum.DataSource.code["howbuy"], "中外合资企业"): "合资企业",
            (Enum.DataSource.code["howbuy"], "中资企业"): "中资企业",
            (Enum.DataSource.code["howbuy"], "国有企业"): "中资企业(国有)",
            (Enum.DataSource.code["howbuy"], "国有相对控股企业"): "中资企业(国有)",
            (Enum.DataSource.code["howbuy"], "民营企业"): "中资企业(民营)",
            (Enum.DataSource.code["howbuy"], "民营相对控股企业"): "中资企业(民营)",
            (Enum.DataSource.code["eastmoney"], "中资企业"): "中资企业",
            (Enum.DataSource.code["eastmoney"], "合资企业"): "合资企业"
        }

    class DOrgHolder:
        __to__ = "org_holder"

        holder_type = {
            (Enum.DataSource.code["howbuy"], 1): "机构",
            (Enum.DataSource.code["howbuy"], 2): "个人",
            (Enum.DataSource.code["howbuy"], 3): "内部",
        }

    class DPersonInfo:
        __to__ = "person_info"

        master_strategy = {
            (Enum.DataSource.code["howbuy"], "QDII"): "0205",
            (Enum.DataSource.code["howbuy"], "保本型"): "0203",
            (Enum.DataSource.code["howbuy"], "债券型"): "0202",
            (Enum.DataSource.code["howbuy"], "混合型"): "0203",
            (Enum.DataSource.code["howbuy"], "理财型"): "0204",
            (Enum.DataSource.code["howbuy"], "股票型"): "0201",
            (Enum.DataSource.code["howbuy"], "货币型"): "0204"
        }

    class DOrgPortfolioAsset:
        type = {
            (Enum.DataSource.code["howbuy"], "买入返售证券余额"): "买入返售金融资产",
            (Enum.DataSource.code["howbuy"], "债券"): "固定收益投资",  #
            (Enum.DataSource.code["howbuy"], "其他资产"): "其它",  #
            (Enum.DataSource.code["howbuy"], "基金"): "基金投资",  #
            (Enum.DataSource.code["howbuy"], "存托凭证"): "权益投资",  #
            (Enum.DataSource.code["howbuy"], "权证投资市值"): "金融衍生品投资",
            (Enum.DataSource.code["howbuy"], "股票"): "权益投资",  #
            (Enum.DataSource.code["howbuy"], "货币市场工具"): "货币市场工具",  #
            (Enum.DataSource.code["howbuy"], "资产支持证券"): "固定收益投资",  #
            (Enum.DataSource.code["howbuy"], "金融衍生品"): "金融衍生品投资",
            (Enum.DataSource.code["howbuy"], "银行存款"): "现金",
            (Enum.DataSource.code["howbuy"], "货币资金"): "现金"
        }

        stype = {
            (Enum.DataSource.code["howbuy"], "买入返售证券余额"): "买入返售金融资产",
            (Enum.DataSource.code["howbuy"], "债券"): "债券",
            (Enum.DataSource.code["howbuy"], "其他资产"): "其它",
            (Enum.DataSource.code["howbuy"], "基金"): "基金",
            (Enum.DataSource.code["howbuy"], "存托凭证"): "存托凭证",
            (Enum.DataSource.code["howbuy"], "权证投资市值"): "权证",
            (Enum.DataSource.code["howbuy"], "股票"): "股票",
            (Enum.DataSource.code["howbuy"], "货币市场工具"): "货币市场工具",
            (Enum.DataSource.code["howbuy"], "资产支持证券"): "资产支持证券",
            (Enum.DataSource.code["howbuy"], "金融衍生品"): "",
            (Enum.DataSource.code["howbuy"], "银行存款"): "现金",
            (Enum.DataSource.code["howbuy"], "货币资金"): "现金"
        }

    class DOrgPortfolioIndustry:
        type_sws = {
            (Enum.DataSource.code["howbuy"], "其它行业"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "农、林、牧、渔业"): Enum.SWS.name_1["110000"],
            (Enum.DataSource.code["howbuy"], "制造业"): Enum.SWS.name_1["360000"],
            (Enum.DataSource.code["howbuy"], "批发和零售业"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供应业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "金融业"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "建筑业"): Enum.SWS.name_1["610000"],
            (Enum.DataSource.code["howbuy"], "水利、环境和公共设施管理业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "采矿业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["howbuy"], "房地产业"): Enum.SWS.name_1["430000"],
            (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服务业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "卫生和社会工作"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "文化、体育和娱乐业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "科学研究和技术服务业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "交通运输、仓储和邮政业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "机械、设备、仪表"): Enum.SWS.name_1["640000"],
            (Enum.DataSource.code["howbuy"], "金融、保险业"): Enum.SWS.name_1["490000"],
            (Enum.DataSource.code["howbuy"], "食品、饮料"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "批发和零售贸易"): Enum.SWS.name_1["450000"],
            (Enum.DataSource.code["howbuy"], "医药、生物制品"): Enum.SWS.name_1["370000"],
            (Enum.DataSource.code["howbuy"], "租赁和商务服务业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "合计"): "ERROR",
            (Enum.DataSource.code["howbuy"], "采掘业"): Enum.SWS.name_1["210000"],
            (Enum.DataSource.code["howbuy"], "信息技术业"): Enum.SWS.name_1["710000"],
            (Enum.DataSource.code["howbuy"], "电子"): Enum.SWS.name_1["270000"],
            (Enum.DataSource.code["howbuy"], "金属、非金属"): Enum.SWS.name_1["270000"],
            (Enum.DataSource.code["howbuy"], "电力、煤气及水的生产和供应业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "综合"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "石油、化学、塑胶、塑料"): Enum.SWS.name_1["220000"],
            (Enum.DataSource.code["howbuy"], "社会服务业"): Enum.SWS.name_1["410000"],
            (Enum.DataSource.code["howbuy"], "交通运输、仓储业"): Enum.SWS.name_1["420000"],
            (Enum.DataSource.code["howbuy"], "住宿和餐饮业"): Enum.SWS.name_1["340000"],
            (Enum.DataSource.code["howbuy"], "教育"): Enum.SWS.name_1["510000"],
            (Enum.DataSource.code["howbuy"], "传播与文化产业"): Enum.SWS.name_1["460000"],
            (Enum.DataSource.code["howbuy"], "纺织、服装、皮毛"): Enum.SWS.name_1["350000"],
            (Enum.DataSource.code["howbuy"], "综合类"): Enum.SWS.name_1["510000"],
        }

        type_wind = {
            (Enum.DataSource.code["howbuy"], "其它行业"): Enum.Wind.industry_code["S"],
            (Enum.DataSource.code["howbuy"], "农、林、牧、渔业"): Enum.Wind.industry_code["A"],
            (Enum.DataSource.code["howbuy"], "制造业"): Enum.Wind.industry_code["C"],
            (Enum.DataSource.code["howbuy"], "批发和零售业"): Enum.Wind.industry_code["F"],
            (Enum.DataSource.code["howbuy"], "电力、热力、燃气及水生产和供应业"): Enum.Wind.industry_code["D"],
            (Enum.DataSource.code["howbuy"], "金融业"): Enum.Wind.industry_code["J"],
            (Enum.DataSource.code["howbuy"], "建筑业"): Enum.Wind.industry_code["E"],
            (Enum.DataSource.code["howbuy"], "水利、环境和公共设施管理业"): Enum.Wind.industry_code["N"],
            (Enum.DataSource.code["howbuy"], "采矿业"): Enum.Wind.industry_code["B"],
            (Enum.DataSource.code["howbuy"], "房地产业"): Enum.Wind.industry_code["K"],
            (Enum.DataSource.code["howbuy"], "信息传输、软件和信息技术服务业"): Enum.Wind.industry_code["I"],
            (Enum.DataSource.code["howbuy"], "卫生和社会工作"): Enum.Wind.industry_code["Q"],
            (Enum.DataSource.code["howbuy"], "文化、体育和娱乐业"): Enum.Wind.industry_code["R"],
            (Enum.DataSource.code["howbuy"], "科学研究和技术服务业"): Enum.Wind.industry_code["M"],
            (Enum.DataSource.code["howbuy"], "交通运输、仓储和邮政业"): Enum.Wind.industry_code["G"],
            (Enum.DataSource.code["howbuy"], "机械、设备、仪表"): Enum.Wind.industry_code["C"],
            (Enum.DataSource.code["howbuy"], "金融、保险业"): Enum.Wind.industry_code["J"],
            (Enum.DataSource.code["howbuy"], "食品、饮料"): Enum.Wind.industry_code["H"],
            (Enum.DataSource.code["howbuy"], "批发和零售贸易"): Enum.Wind.industry_code["F"],
            (Enum.DataSource.code["howbuy"], "医药、生物制品"): Enum.Wind.industry_code["M"],
            (Enum.DataSource.code["howbuy"], "租赁和商务服务业"): Enum.Wind.industry_code["L"],
            (Enum.DataSource.code["howbuy"], "合计"): "ERROR",
            (Enum.DataSource.code["howbuy"], "采掘业"): Enum.Wind.industry_code["B"],
            (Enum.DataSource.code["howbuy"], "信息技术业"): Enum.Wind.industry_code["I"],
            (Enum.DataSource.code["howbuy"], "电子"): Enum.Wind.industry_code["M"],
            (Enum.DataSource.code["howbuy"], "金属、非金属"): Enum.Wind.industry_code["B"],
            (Enum.DataSource.code["howbuy"], "电力、煤气及水的生产和供应业"): Enum.Wind.industry_code["D"],
            (Enum.DataSource.code["howbuy"], "综合"): Enum.Wind.industry_code["S"],
            (Enum.DataSource.code["howbuy"], "石油、化学、塑胶、塑料"): Enum.Wind.industry_code["C"],
            (Enum.DataSource.code["howbuy"], "社会服务业"): Enum.Wind.industry_code["Q"],
            (Enum.DataSource.code["howbuy"], "交通运输、仓储业"): Enum.Wind.industry_code["G"],
            (Enum.DataSource.code["howbuy"], "住宿和餐饮业"): Enum.Wind.industry_code["H"],
            (Enum.DataSource.code["howbuy"], "教育"): Enum.Wind.industry_code["P"],
            (Enum.DataSource.code["howbuy"], "传播与文化产业"): Enum.Wind.industry_code["R"],
            (Enum.DataSource.code["howbuy"], "纺织、服装、皮毛"): Enum.Wind.industry_code["C"],
            (Enum.DataSource.code["howbuy"], "综合类"): Enum.Wind.industry_code["S"],
        }
