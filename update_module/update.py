from pyjin import pyjin    
from x_x import account_info as ai
import pandas as pd

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

def update_rows(acc,         
                db,         
                table,
                list_dict_input,
                list_dict_condition):        

    with pyjin.connectDB(**acc) as con:                               
        for dict_input, dict_condition in zip(list_dict_input, list_dict_condition):
            query, kwargs_set_conditions, kwargs_where_conditions = update_query_row(db, table, dict_input, dict_condition)
            
            try:
                pyjin.execute_query(con, query, **kwargs_set_conditions, **kwargs_where_conditions, is_return=False)
            except BaseException as e:
                print(e)
            

            
def update_bulk_row(acc,
                db,
                table,
                list_dict_input,
                list_dict_condition):
    
    

    set_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_input[0].items()])
    join_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_condition[0].items()])
    
		# join table for update
    df = pd.concat([pd.DataFrame(list_dict_condition),pd.DataFrame(list_dict_input)],axis=1)
    join_query = """
            update {db}.{table} A
            inner join {db}.temp_table B 
            on {join_conditions}
            set {set_conditions}
            """.format(db=db,
                        table=table,
                        join_conditions=join_conditions,
                        set_conditions=set_conditions)

    with pyjin.connectDB(**acc) as con:
        try:
            # transaction
            # create temporary table
            df.to_sql('temp_table', 
                    schema=db, 
                    con=con, 
                    if_exists='replace', 
                    index=False, 
                    chunksize=1000, 
                    method='multi') ## 큰 query 처리 불가능한 경우를 위해 chunksize 추가

            print('create temp table finished')
            
            #  join table
            pyjin.execute_query(con,join_query)
            print('join table finished')

            # 만약 temp_table이 남아있으면 drop으로 삭제
            if  pyjin.check_is_table(acc=acc, 
                                        schema_name = db,
                                        table_name = 'temp_table'
                                        ):

            # delete temporary table
                pyjin.execute_query(con, 'drop table {}.{}'.format(db,'temp_table'))
                print('delete table finished')

        except Exception as e:
            pyjin.print_logging("{} update failed, {}".format(table,e))

    pyjin.print_logging('update completed')

def bulk_insert_table(insert_json_list, table_name, to_conn):
    # dummy table 생성
    pd.DataFrame(insert_json_list).to_sql(table_name, index=False, if_exists='replace', con=to_conn ,method='multi', chunksize=300)

# update할 테이블과 dummy_table이 있을 시
def bulk_update_table(table_name, to_conn):
    set_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_input[0].items()])
    join_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_condition[0].items()])
    
		# join table for update
    df = pd.concat([pd.DataFrame(list_dict_condition),pd.DataFrame(list_dict_input)],axis=1)
    join_query = """
            update {db}.{table} A
            inner join {db}.temp_table B 
            on {join_conditions}
            set {set_conditions}
            """.format(db=db,
                        table=table,
                        join_conditions=join_conditions,
                        set_conditions=set_conditions)


