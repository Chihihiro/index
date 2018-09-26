
class UptSQL:
    class Private:
        class FundInfo:
            REG_PRI = """
            UPDATE fund_info AS fi 
            JOIN fund_id_match AS fim ON fi.fund_id = fim.fund_ID  
            JOIN fund_info_private AS fip ON fim.source_ID = fip.fund_id  
            SET fi.reg_code = fip.reg_code_amac, fi.reg_time = fip.reg_time_amac, fi.foundation_date = fip.foundation_date_amac
            WHERE fim.match_type = 5 AND fip.flag = 1;
            """

            REG_ACC = """
            UPDATE fund_info AS fi 
            JOIN fund_id_match AS fim ON fi.fund_id = fim.fund_ID 
            JOIN fund_info_fundaccount AS fif ON fim.source_ID = fif.fund_id 
            SET fi.reg_code = fif.reg_code_amac, fi.reg_time = fif.reg_time_amac            
            WHERE fim.match_type = 5;
            """

            REG_FUT = """
            UPDATE fund_info AS fi 
            JOIN fund_id_match AS fim ON fi.fund_id = fim.fund_ID 
            JOIN fund_info_futures AS fif ON fim.source_ID = fif.fund_id 
            SET fi.reg_code = fif.reg_code_amac,
            fi.foundation_date = fif.foundation_date_amac
            WHERE fim.match_type = 5;
            """

            REG_SEC = """
            UPDATE fund_info AS fi 
            JOIN fund_id_match AS fim ON fi.fund_id = fim.fund_ID 
            JOIN fund_info_securities AS fis ON fim.source_ID = fis.fund_id 
            SET fi.reg_code = fis.reg_code_amac, fi.foundation_date = fis.foundation_date_amac
            WHERE fim.match_type = 5;
            """

            IS_REG = """
            UPDATE fund_info AS fi JOIN fund_id_match fim ON fi.fund_id = fim.fund_id SET is_reg = 1 WHERE fim.match_type = 5;
            UPDATE fund_info SET is_reg = 0 where is_reg IS NULL;
            """

            # IS_DEPOSIT = """
            # UPDATE fund_info  SET is_deposit=1 WHERE fund_custodian LIKE '%%证券%%' OR fund_custodian LIKE '%%银行%%';
            # UPDATE fund_info  SET is_deposit=0 WHERE is_deposit IS NULL;
            # """

            # IS_ABNORMAL_LIQUIDATION = """
            # UPDATE fund_info SET is_abnormal_liquidation = '1' WHERE liquidation_cause IN ('提前清盘', '提前清算');
            # UPDATE fund_info SET is_abnormal_liquidation = '0' WHERE liquidation_cause IN ('正常清盘', '终止');
            # """

            # FUND_STATUS = """
            # UPDATE fund_info SET fund_status = '运行中' WHERE (
            # (fund_time_limit = '无期限') OR  (fund_time_limit > CURDATE()) OR (fund_status IS NULL)
            # );
            # """

            IS_UMBRELLA = """
            UPDATE fund_info SET is_umbrella_fund = 1 WHERE fund_full_name LIKE '%%伞形%%';
            UPDATE fund_info SET is_umbrella_fund = 0 WHERE is_umbrella_fund IS NULL;
            """

        class FundInfoSubsidiary:
            INVRNG_PRI = """
            UPDATE fund_info_subsidiary AS fis
            JOIN fund_id_match AS fim ON fis.fund_id = fim.fund_ID
            JOIN fund_info_private AS fip ON fim.source_ID = fip.fund_id
            SET fis.investment_range = fip.orientation_amac
            WHERE fim.match_type = 5 AND fip.flag = 1;
            """

            INVRNG_ACC = """
            UPDATE fund_info_subsidiary AS fis
            JOIN fund_id_match AS fim ON fis.fund_id = fim.fund_ID
            JOIN fund_info_fundaccount AS fia ON fim.source_ID = fia.fund_id
            SET fis.investment_range=fia.orientation_amac
            WHERE fim.match_type = 5 AND fis.investment_range IS NULL;
            """

            INVRNG_FUT = """
            UPDATE fund_info_subsidiary AS fis
            JOIN fund_id_match AS fim ON fis.fund_id = fim.fund_ID
            JOIN fund_info_futures AS fif ON fim.source_ID = fif.fund_id
            SET fis.investment_range = fif.orientation_amac
            WHERE fim.match_type = 5 AND fis.investment_range IS NULL;
            """

            INVRNG_SEC = """
            UPDATE fund_info_subsidiary AS fis
            JOIN fund_id_match AS fim ON fis.fund_id = fim.fund_ID
            JOIN fund_info_securities AS fisec ON fim.source_ID = fisec.fund_id
            SET fis.investment_range = fisec.orientation_amac
            WHERE fim.match_type = 5 AND fis.investment_range IS NULL;
            """

        class OrgInfo:
            #todo 该判断逻辑可能有错
            IS_REG_NOW = """
            UPDATE org_info SET is_reg_now = '是'
            WHERE reg_code IN (SELECT reg_code FROM crawl.x_fund_org) AND entry_time> '2017-05-15';
            UPDATE org_info SET is_reg_now = '否' where is_reg_now IS NULL and org_category=4;
            """