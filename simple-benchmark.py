
import pymysqlpool
import pymysql
import time
import sys

config = {'host': '192.168.1.111', 'user': 'user', 'password': 'pass', 'database': 'test'}


def test_with_pymysqlpool(num):
    pool = pymysqlpool.ConnectionPool(size=10, maxsize=10, pre_create_num=10, name='poo', **config)
    t1 = time.time()
    for _ in range(num):
        con = pool.get_connection()
        cur = con.cursor()
        cur.execute('select 1+1')
        cur.close()
        con.close()
    t2 = time.time()
    print('total {} finish within {}s.\n{} queries per second, avg {} ms per query'.format(num, round(t2-t1, 3), round(num/(t2-t1), 2), round((t2-t1)*1000/num, 2)))


def test_within_only_one_con(num):
    con = pymysql.Connection(**config)
    t1 = time.time()
    for _ in range(num):
        cur = con.cursor()
        cur.execute('select 1+1')
        cur.close()
    t2 = time.time()
    print('total {} finish within {}s.\n{} queries per second, avg {} ms per query'.format(num, round(t2-t1, 3), round(num/(t2-t1), 2), round((t2-t1)*1000/num, 2)))


def make_conn_everytime(num):
    t1 = time.time()
    for i in range(num):
        con = pymysql.Connection(**config)
        cur = con.cursor()
        cur.execute('select 1+1')
        cur.close()
        con.close()
    t2 = time.time()
    print('total {} finish within {}s.\n{} queries per second, avg {} ms per query'.format(num, round(t2-t1, 3), round(num/(t2-t1), 2), round((t2-t1)*1000/num, 2)))


# test use sqlalchemy
# def test_sqlalchemy_connection_pool(num):
#     from sqlalchemy import create_engine, text
#     from sqlalchemy.orm import sessionmaker
#     db_url = 'mysql+pymysql://root:ljk.404@192.168.1.218:3306/test'
#     engine = create_engine(db_url, pool_size=10, max_overflow=20)
#     Session = sessionmaker(bind=engine)
#     t1 = time.time()
#     for _ in range(num):
#         with Session() as session:
#             result = session.execute(text('SELECT 1 + 1'))
#             # print(result.scalar())
#     t2 = time.time()
#     print(t2-t1)


def main():
    usage = "\
Usage: {} [test_name] [test_num]\n\
    test_name: [pymysql-pool | pymysql-one-conn | pymysql-new-con-everytime]\n\
    test_num: a integer\
    ".format(sys.argv[0])
    if len(sys.argv) != 3:
        print(usage)
    else:
        test = sys.argv[1]
        num = int(sys.argv[2])

        if test == 'pymysql-pool' and num:
            test_with_pymysqlpool(num)
        elif test == 'pymysql-one-conn' and num:
            # This is the best performing scenario, native pymysql, and all queries are done within a single connection
            test_within_only_one_con(num)
        elif test == 'pymysql-new-con-everytime' and num:
            # This is the worst-performing scenario, native pymysql, but a new connection is established with each query
            make_conn_everytime(num)
        else:
            print(usage)


if __name__ == "__main__":
    main()
