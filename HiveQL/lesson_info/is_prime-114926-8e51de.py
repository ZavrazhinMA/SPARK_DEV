"""
https://florianwilhelm.info/2016/10/python_udf_in_hive/
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Transform

host:
docker cp "..\22. HiveQL\is_prime.py" docker-hive-hive-server-1:/opt
docker-compose exec hive-server bash

sh> 
apt-get update
apt-get install -y --force-yes python3

hdfs dfs -put -f is_prime.py /user/hive/


hive>

create database pycheck;
create table pycheck.prime(
    x int,
    y int
);

insert overwrite table pycheck.prime values
(1, 5), (2, 7), (49, 25), (13, 42);

add FILE hdfs:///user/hive/is_prime.py;

set hive.cli.print.header=true;

select transform(x, y) 
using 'python3 is_prime.py' as (sum, dif)
from (select x, y from pycheck.prime) t;


drop table if exists pycheck.sum_dif;
create table pycheck.sum_dif as
select transform(x, y) 
using 'python3 is_prime.py' as (sum int, dif int)
from (select x, y from pycheck.prime) t; -- run in mapper
--from (select x, y from pycheck.prime distribute by y) t; -- run in reducer
--DISTRIBUTE BY and CLUSTER BY = Distribute BY + SORT BY;

sum     dif
6       -4
9       -5
55      -29

# col_name              data_type
sum                     string
dif                     string
"""

def is_prime(x):
    """x is natural number"""
    if x == 1:
        return False
    if x == 2:
        return True
    if x % 2 == 0:
        return False
    for i in range(3, int(x ** 0.5) + 1, 2):
        if x % i == 0:
            return False
    return True

if __name__ == '__main__':
    import sys
    for line in sys.stdin:
        sys.stderr.write('>>>> Read a line: ' + line)
        line = line.strip("\r\n")
        x, y = tuple(map(int, line.split("\t")))
        if is_prime(x) or is_prime(y):
            print("\t".join([str(x + y), str(x - y)]))
        # else:
        #     print("\t".join([r"\N", str(x)]))
    
if __name__ == '__main__1':
    assert not is_prime(1)
    assert is_prime(2)
    assert is_prime(3)
    assert not is_prime(4)
    assert is_prime(5)
    assert not is_prime(6)
    assert not is_prime(9)
    assert is_prime(13)
    assert not is_prime(25)
    assert not is_prime(35)
    print("OK")
    
    