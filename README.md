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
from pydorm import init, dorm, dict_query, query, update, insert

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
    print(dict_query('test_table').eq('type', 1).list())

    # 跨库查询
    print(dict_query('test_table', 'database2').list())

    # 删除数据，返回影响行数（这里会报错，有安全校验，不允许全量删除）
    print(update('test_table').delete())

    # 更新数据，返回影响行数
    print(update('test_table').set(name='abc', type=1).eq('id', 1).update())

    # 插入数据，返回影响行数和主键
    print(insert('test_table', {'name': 'new_record', 'type': 2}))
```
