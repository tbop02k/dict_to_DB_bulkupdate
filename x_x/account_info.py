def servicedb_sql_info(**kwars):
    return {
        'host' :'ople-db.koreacentral.cloudapp.azure.com',
        'user' : 'ople',
        'password' : 'ohmysql@x_x.co.kr',
        'db' : 'ople',
        'port' : 5000,
    }

def analdb_sql_info(db='ople'):
    return {
        'host' :'datatest.koreacentral.cloudapp.azure.com',
        'user' : 'ople',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 5000,
    }

def dw_sql_info(**kwars):
    return {
        'host' :'warehouse.cyil3yszr2rs.ap-northeast-2.rds.amazonaws.com',
        'user' : 'x_x',
        'password' : 'orderplus11',
        'db' : 'ople',
        'port' : 3306,
    }

def vmdb_sql_info(db='ople'):
    return {
        'host' :'222.107.129.210',
        'user' : 'ople',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 3306,
    }

# dbtype default : mysql
def vmdb2_sql_info(db='postgres'):
    return {
        'host' :'222.107.129.210',
        'user' : 'postgres',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 5432,
        'dbtype' : 'postgresql+psycopg2'
    }
