from pyjin import pyjin
import pandas as pd
import numpy as np
from x_x import account_info as ai
import re
from master import prd_master

class MemorySearch():
    def __init__(self):
        anal_acc = ai.analdb_sql_info()
        self.master_df = pyjin.conn_exec_close(anal_acc, """
                select A.pid, A.rawname, concat(A.rawname,'/ L-',E.delivery_day) as rawname_LT, A.origin, A.unit, A.net_contents, A.product_cate,A.character, A.manufacturer, A.product_type, A.brand, A.main_attr, A.vws, C.supplier_id, C.price, C.price_per, C.s_code, D.master_index, E.is_public 
                from prd_master2.xprd_master_source A
                left join (select id,supplier_id, if((org_price_per is null) or (org_price_per=0),price,org_price_per) as price, price_per,s_code from ople.rest_product) C on A.pid=C.id
                left join prd_master2.xprd_master_master D on A.mastercode_id = D.id
                join (select id, delivery_day,  is_public from ople.rest_product) E
                on A.pid= E.id
                """, output='df')

    def final_sort(self, res_df, output_rows_num):
        res_df= res_df.sort_values(['exist_matching_score','none_matching_score','master_index'], ascending=[False, False, False])
        return res_df.iloc[:output_rows_num]

    def search_score(self,
                     output,
                     supplier_ids, #list
                     search_must_list, #list
                     is_public):

        res_df = self.master_df.copy()
        if is_public is not None:
            res_df = res_df[res_df['is_public'] == is_public]
        if len(supplier_ids)!=0:
            res_df = res_df[res_df['supplier_id'].isin(supplier_ids)]

        ## output 이 있을경우 matching 하는 score
        res_df['exist_matching_score'] = 0
        ## output 이 없을경우 matching 하는 score
        res_df['none_matching_score'] = 0

        all_list = ['product_cate','product_type','unit', 'origin', 'brand', 'manufacturer', 'character', 'net_contents', 'main_attr']

        search_should_list = [col for col in all_list if col not in search_must_list]

        for key in search_should_list:
            if key not in output:
                output.update({key : None})

        ## must search
        for idx, ele in enumerate(search_must_list):
            if pd.notnull(output[ele]):
                compare_string = str(output[ele]) if pd.notnull(output[ele]) else None
                res_df = res_df[res_df[ele]  == compare_string].reset_index(drop=True)
                if len(res_df) == 0:
                    return res_df

        # should search
        for idx, ele in enumerate(search_should_list):
            ## pandas data frame 이 list값도 str 로 db에서 가져오므로 비교하기 위하여 materoutput 도 string으로 변환
            compare_string = str(output[ele])if type(output[ele]) == list else None if pd.isnull(output[ele]) else str(output[ele])

            ## matching score 계산
            if output[ele] is not None:
                res_df['exist_matching_score'] = res_df.apply(
                    lambda row: row['exist_matching_score'] + 1 if row[ele] == compare_string else row[
                        'exist_matching_score'], axis=1)
            ## non-matching score 계산
            else:
                res_df['none_matching_score'] = res_df.apply(
                    lambda row: row['none_matching_score'] + 1 if row[ele] == compare_string else row[
                        'none_matching_score'], axis=1)
        return res_df


    def out_from_pid(self, pid):
        self.master_df=self.master_df.set_index('pid', drop=False)
        return pyjin.jin_df_json(self.master_df.loc[[pid]])

    def single_search(self, key_word=None, supplier_ids=[], search_must_list=['product_type'], output_rows_num=1, output=None, is_public=None):
        if output is None:
            output = prd_master.convert_masterproduct(key_word)
        return pyjin.jin_df_json(self.final_sort(self.search_score(output, supplier_ids, search_must_list, is_public), output_rows_num))

    def search(self, key_word_list=[], supplier_ids=[] ,search_must_list=['product_type'], output_rows_num=1, output_list=[], is_public=None):
        ## public None 일경우 모두검색,

        if len(output_list)==0:
            search_list = [{'rawname' : key_word} for key_word in key_word_list]
            output_list = prd_master.convert_masterproduct_bulk(search_list)

        final_json_list= [pyjin.jin_df_json( self.final_sort(self.search_score(output, supplier_ids, search_must_list,is_public), output_rows_num)) for output in output_list]
        return final_json_list