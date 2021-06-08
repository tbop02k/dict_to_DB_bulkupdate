import pandas as pd
from x_x import account_info as ai
from pyjin import pyjin

# json 형태의 자료를 table에 insert하는 함수
def bulk_insert_table(insert_json_list, table_name, to_conn):
    pd.DataFrame(insert_json_list).to_sql(table_name, index=False, if_exists='replace', con=to_conn ,method='multi', chunksize=300)

def bulk_update(acc,
                db,
                table,
                list_dict_input,
                list_dict_condition):

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
            

def bulk_join_update(acc,
                     db,
                     table,
                     list_dict_input,
                     list_dict_condition):

    set_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_input[0].items()])
    join_conditions = ",".join(["A.{key} = B.{key}".format(key=key) for key, value in list_dict_condition[0].items()])
    
	# dummy table
    df = pd.concat([pd.DataFrame(list_dict_condition),pd.DataFrame(list_dict_input)],axis=1)
    print(df)
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
            # dummy table이 없으면 새로 create하고, 있으면 drop create insert. (if_exists='replace')
            df.to_sql(table+'_dummy', schema=db, con=con, if_exists='replace', index=False, chunksize=300, method='multi')
            pyjin.print_logging("dummy table 업뎃 완료")
            ##  join update
            pyjin.execute_query(con,join_query)

        except Exception as e:
            pyjin.print_logging("{} update failed, {}".format(table, e))

    pyjin.print_logging('update completed')
