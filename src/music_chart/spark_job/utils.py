import mysql.connector


def insert_partition(partition, mysql_conf, dbname: str, dbtable: str, cols: list, replace: bool=False):
    '''
    Insert records to mysql for each partition
    '''
    mysql_conn = mysql.connector.connect(**mysql_conf, database=dbname)
    cursor = mysql_conn.cursor()
    command = 'INSERT'
    if replace:
        command = 'REPLACE'
    sql = f'''
    {command} INTO {dbtable}
    ({','.join(cols)})
    VALUES ({','.join(['%s']*len(cols))})
    '''
    for row in partition:
        cursor.execute(sql, row)
    mysql_conn.commit()
    mysql_conn.close()