[English](README.md)

# reydb

**reydb** 是一个基于 **SQLAlchemy**、**SQLModel** 和 **Pydantic** 构建的 Python 数据库引擎池连接包。

提供数据库引擎、连接池、数据库连接、SQL 执行、ORM 映射、数据库元数据、数据库配置及表结构管理等功能。

基于 **SQLAlchemy** 实现数据库引擎及连接池管理，基于 **SQLModel** 和 **Pydantic** 实现 ORM 映射模型及数据验证，并提供统一的 SQL 语法构建、执行和结果处理接口。

各执行模块同时支持**同步方法**和**基于协程的异步方法**，适用于异步及同步 Python 后端服务中的数据库操作。

> **数据库支持**
>
> * `reydb <= 1.3.12`：支持 MySQL
> * `reydb >= 1.3.13`：支持 PostgreSQL

## 特性

* 基于 SQLAlchemy 实现数据库引擎及连接池管理
* 基于 SQLModel 实现 ORM 映射模型
* 基于 Pydantic 实现数据模型及字段验证
* 支持 PostgreSQL 数据库
* 支持同步数据库操作
* 支持基于协程的异步数据库操作
* 提供数据库引擎连接池管理
* 提供数据库连接及连接生命周期管理
* 提供数据库事件周期管理
* 提供 SQL 语法构建与执行
* 支持 SQL 参数和值注入
* 提供 SQL 执行结果扩展处理
* 支持将查询结果转换为 `list[dict]` 表结构
* 支持将查询结果转换为 JSON 结构
* 提供数据库 ORM 操作
* 提供 ORM 模型、字段及语法对象
* 提供 ORM 数据验证
* 提供 ORM 数据库会话及事件管理
* 提供数据库表创建和删除
* 支持通过 ORM 模型或 Python 基本数据类型构建数据库表
* 提供数据库配置数据的增、删、改操作
* 提供数据库异常数据存储
* 提供数据库元数据查询和修改
* 支持 PostgreSQL `pg_catalog`、`information_schema` 等 Schema 的元数据操作

---

## 安装

要求 **Python 3.12 或更高版本**。

```bash
pip install reydb
```

---

## 快速开始

创建数据库对象：

```python
from reydb import Database

db = Database()

db(**engine1_args)
db(**engine2_args)
```

其中，每次调用 `Database` 对象可以添加一个数据库引擎及其连接池配置。

### 同步执行

```python
result = db[engine_name].execute(sql)

result.to_table()
```

### 异步执行

```python
model = db[engine_name].orm.get(model)

db[engine_name].orm.insert(model).values(data).execute()
```

---

# 模块

reydb 按数据库功能划分为多个模块，各模块负责不同的数据库操作能力。

## `rall` — All import methods

**统一导出模块。**

提供 reydb 所有模块方法和对象的便捷导出，可以通过该模块集中导入数据库相关功能，减少从多个模块分别导入的代码。

---

## `rbase` — Base methods

**基础方法模块。**

提供其它模块使用的基础依赖方法和公共功能。

用于支持 reydb 各模块之间的基础功能调用。

---

## `rbuild` — Database build methods

**数据库构建模块。**

提供数据库表结构创建和删除相关方法。

主要功能：

* 创建数据库表
* 删除数据库表
* 支持通过 ORM 映射模型创建表
* 支持通过 Python 基本数据类型构建数据库表

---

## `rconfig` — Database config methods

**数据库配置模块。**

提供数据库配置参数的数据存储和管理方法。

主要功能：

* 数据库配置参数新增
* 数据库配置参数修改
* 数据库配置参数删除

可用于将数据库相关配置参数存储到数据库表中，并进行统一管理。

---

## `rconn` — Database connection methods

**数据库连接模块。**

提供数据库连接对象和数据库事件对象。

主要包括：

* 数据库连接对象

  * 管理单个数据库连接资源
* 数据库事件对象

  * 管理单个数据库事件周期

用于管理数据库连接及其生命周期。

---

## `rdb` — Database methods

**顶层数据库模块。**

提供顶层封装的数据库对象，用于统一管理多个数据库引擎及其连接池对象。

主要功能：

* 创建数据库对象
* 添加数据库引擎
* 管理多个数据库连接池
* 根据引擎名称获取对应数据库引擎对象

---

## `rengine` — Database engine methods

**数据库引擎模块。**

提供数据库引擎及连接池对象。

主要用于管理数据库连接资源，并为上层数据库操作提供统一的引擎访问入口。

主要功能：

* 数据库引擎管理
* 数据库连接池管理
* 数据库连接对象管理
* 数据库连接生命周期管理

---

## `rerror` — Database error methods

**数据库异常模块。**

提供 Python 异常数据存储到数据库表中的相关方法。

可以通过装饰器等方式封装目标函数，在函数执行发生异常时，将异常相关数据记录到数据库中。

---

## `rexec` — Database execute methods

**数据库执行模块。**

提供数据库 SQL 语法构建、执行及参数和值注入相关功能。

主要功能：

* SQL 语法构建
* SQL 语句执行
* SQL 参数注入
* SQL 值注入
* 规范化数据库操作流程
* 提供扩展的 `Result` 结果对象

`Result` 对象提供多种结果处理方法，例如：

* 转换为表结构 `list[dict]`
* 转换为 JSON 结构
* 其它数据库查询结果处理

---

## `rinfo` — Database information methods

**数据库信息模块。**

提供数据库元数据信息的查询和修改方法。

主要涉及 PostgreSQL 的系统 Schema，包括：

* `pg_catalog`
* `information_schema`
* 其它数据库元数据信息

可用于获取和管理数据库、Schema、表、字段及其它数据库对象的相关元数据。

---

## `rorm` — Database ORM methods

**数据库 ORM 模块。**

提供数据库 ORM 映射及相关操作功能。

主要包括：

* ORM 映射模型基类
* ORM 映射模型字段对象
* ORM 各类 SQL 语法抽象对象
* ORM 字段值验证类
* ORM 数据库会话对象
* ORM 数据库事件对象

其中：

* **ORM 数据库会话对象**：管理 ORM 操作中的单个数据库连接资源
* **ORM 数据库事件对象**：管理 ORM 操作中的单个数据库事件周期

通过 ORM 模型可以对数据库表进行结构映射，并执行相应的数据操作。

---

# 模块概览

| 模块        | 功能             |
| --------- | -------------- |
| `rall`    | 所有方法的统一导出      |
| `rbase`   | 基础方法及模块公共依赖    |
| `rbuild`  | 数据库表创建和删除      |
| `rconfig` | 数据库配置参数管理      |
| `rconn`   | 数据库连接及事件管理     |
| `rdb`     | 顶层数据库对象及引擎管理   |
| `rengine` | 数据库引擎及连接池管理    |
| `rerror`  | Python 异常数据存储  |
| `rexec`   | SQL 构建、执行及结果处理 |
| `rinfo`   | 数据库元数据管理       |
| `rorm`    | ORM 映射及数据库操作   |

---

# 数据库支持

reydb 的数据库支持版本如下：

| reydb 版本    | 数据库        |
| ----------- | ---------- |
| `<= 1.3.12` | MySQL      |
| `>= 1.3.13` | PostgreSQL |

当前版本 `1.3.71` 使用 **PostgreSQL**。

---

# 依赖

主要依赖：

* `asyncpg`
* `psycopg[binary]`
* `pydantic[email]`
* `reykit`
* `sqlalchemy==2.0.42`
* `sqlmodel`

---

# 项目信息

| 项目         | 信息                                                        |
| ---------- | --------------------------------------------------------- |
| 名称         | `reydb`                                                   |
| 版本         | `1.3.71`                                                  |
| Python     | `>=3.12`                                                  |
| 数据库        | `PostgreSQL`                                              |
| 作者         | `Rey`                                                     |
| 邮箱         | `reyxbo@163.com`                                          |
| Homepage   | [reyxbo.com](https://www.reyxbo.com/release/python/reydb) |
| Repository | [reydb-py](https://github.com/reyxbo/reydb-py.git)        |

## 关键词

`rey` · `reyxbo` · `PostgreSQL` · `postgres` · `db` · `database` · `orm` · `async` · `asynchronous` · `build`
