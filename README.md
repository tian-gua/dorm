<p align="center"><h1 style="font-size: 50px" align="center">pydorm</h1></p>
<p align="center">
    <em>一个轻量级的python orm框架</em>
</p>
<p align="center">
<a href="https://pypi.org/project/pydorm" target="_blank">
    <img src="https://img.shields.io/pypi/v/pydorm?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/py-seal" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/fastapi.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

## 安装
```shell
pip install pydorm
```

## 依赖
- PyYAML
- loguru
- PyMySQL

## 特性
- [x] 支持多数据源
- [x] 支持链式查询
- [x] 动态数据结构，同时支持字典和根据表结构动态生成的dataclass

## 使用方法
### 1.添加数据库配置文件
项目根目录添加`dorm.yaml`文件
```yaml
datasource:
  default: # 默认数据源
    dialect: 'mysql' # mysql or sqlite
    host: localhost # db_host
    port: 3306 # db_port
    user: test # db_user
    password: test # db_password
    database: database # db_name

  another_datasource: # 多数据源
    dialect: 'mysql' # mysql or sqlite
    host: localhost # db_host
    port: 3307 # db_port
    user: test # db_user
    password: test # db_password
    database: database # db_name
```
### 2.CURD示例
```python
from pydorm import init, dorm, dict_query, query, update, insert, insert_bulk, upsert, upsert_bulk

if __name__ == '__main__':
    # 初始化
    init('./dorm.yaml')
    print(dorm.is_initialized())

    # 链式查询单条数据，返回字典
    record: dict = dict_query('test_table').eq('id', '1').one()
    print(record['name'])

    # 链式查询单条数据，返回动态对象
    record_obj = query('test_table').eq('id', '1').one()
    print(record_obj.name)

    # 链式查询批量数据
    dict_query('test_table').eq('type', 1).list()

    # 跨库查询
    dict_query('test_table', 'database2').list()

    # 删除数据，返回影响行数（这里会报错，有安全校验，不允许全量删除）
    update('test_table').delete()

    # 更新数据，返回影响行数
    update('test_table').set(name='abc', type=1).eq('id', 1).update()

    # 插入数据，返回影响行数和主键
    insert('test_table', {'name': 'new_record', 'type': 2})

    # 批量插入
    insert_bulk('user', [{'nick_name': 'test'}, {'nick_name': 'test'}])
    
    # 分页查询
    query('test_table').page(1, 10)

    # 插入更新（on duplicate key update）
    upsert('test_table', {'nick_name': 'guest', 'username': 'guest'})
    # username冲突时更新nick_name
    upsert('test_table', {'nick_name': 'guest', 'username': 'guest'}, ['nick_name'])

    # 批量插入更新
    upsert_bulk('test_table', [{'nick_name': 'guest', 'username': 'guest'},{'nick_name': 'admin', 'username': 'admin'}])
    upsert_bulk('test_table', [{'nick_name': 'guest', 'username': 'guest'},{'nick_name': 'admin', 'username': 'admin'}], ['nick_name'])
```
