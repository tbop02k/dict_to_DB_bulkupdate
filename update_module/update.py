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
