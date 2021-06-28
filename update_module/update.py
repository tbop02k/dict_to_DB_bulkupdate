import pandas as pd
from ..pyjin import pyjin

# json 형태의 자료를 table에 insert하는 함수
def bulk_insert_table(insert_json_list, table_name, to_conn):
    pd.DataFrame(insert_json_list).to_sql(table_name, index=False, if_exists='replace', con=to_conn ,method='multi', chunksize=300)

def update_query_row(db, table, dict_input, dict_condition):
    set_conditions = ",".join(["{key} = :set_{key}".format(key=key) for key in dict_input.keys()])
    where_conditions = " and ".join(["{key} = :where_{key}".format(key=key) for key in dict_condition.keys()])    
    
    kwargs_set_conditions = {"set_"+key : value for key, value in dict_input.items()}
    kwargs_where_conditions = {"where_"+key : value for key, value in dict_condition.items()} 
    
    query = """
    update {db}.{table}
    set {set_conditions}
    where {where_conditions}
    """.format(db=db,
               table=table, 
               set_conditions= set_conditions,
               where_conditions = where_conditions)
    
    return query, kwargs_set_conditions, kwargs_where_conditions

# 방법1. 반복문으로 행 하나하나에 접근하여 update
def update_rows(acc,         
                db,         
                table,
                dict_input,
                join_key):  

    df = pd.DataFrame(dict_input)
    df_col = df.columns

    for key in join_key:
        if key not in df_col:
            return 'join key not exists'

    list_dict_input = df[df.columns.difference(join_key)].to_dict(orient='records')
    list_dict_condition = df[join_key].to_dict(orient='records')    

    with pyjin.connectDB(**acc) as con:                               
        for dict_input, dict_condition in zip(list_dict_input, list_dict_condition):
            query, kwargs_set_conditions, kwargs_where_conditions = update_query_row(db, table, dict_input, dict_condition)
            
            try:
                pyjin.execute_query(con, query, **kwargs_set_conditions, **kwargs_where_conditions, is_return=False)
            except BaseException as e:
                print(e)
                  
# 방법2. dummy table에 데이터 삽입 후 join update
# create_mode=0 : dummy table이 없을시 stop, create_mode=1 : dummy table이 없을시 새로 생성
def bulk_update_rows(acc,
                     db,
                     table,
                     dict_input,
                     join_key,
                     create_mode=0):

    df = pd.DataFrame(dict_input)
    df_col = df.columns

    for key in join_key:
        if key not in df_col:
            return 'join key not exists'

    list_dict_input = df[df.columns.difference(join_key)].to_dict(orient='records')
    list_dict_condition = df[join_key].to_dict(orient='records')

    set_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_input[0].items()])
    join_conditions = " AND ".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_condition[0].items()])
    
    join_query = """
            update {db}.{table} A
            inner join {db}.{table_dummy} B 
            on {join_conditions}
            set {set_conditions}
            """.format(db=db,
                       table=table,
                       table_dummy=table+'_dummy',
                       join_conditions=join_conditions,
                       set_conditions=set_conditions)

    with pyjin.connectDB(**acc) as con:
        try:
            # dummy 테이블이 있다면 delete, insert
            if pyjin.check_is_table(acc=acc,
                                    schema_name=db,
                                    table_name=table+'_dummy'):

                pyjin.execute_query(con,'delete from {}.{}'.format(db,table+'_dummy'))
                df.to_sql(table+'_dummy', schema=db, con=con, if_exists='append', index=False, chunksize=300, method='multi')
            
            else:
                if create_mode == 0:
                    return
                else:
                    # dummy table이 없고, create_mode가 1이면 create (if_exists='replace')
                    df.to_sql(table+'_dummy', schema=db, con=con, if_exists='replace', index=False, chunksize=300, method='multi')
                    pyjin.print_logging("dummy table 생성 완료")
            pyjin.print_logging("dummy table 업데이트 완료")
            ##  join update
            pyjin.execute_query(con,join_query)

        except Exception as e:
            pyjin.print_logging("{} update failed, {}".format(table, e))

    pyjin.print_logging('update completed')
