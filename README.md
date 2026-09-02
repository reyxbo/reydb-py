[中文](README_zh.md)

# reydb

**reydb** is a Python database engine and connection pool package built on **SQLAlchemy**, **SQLModel**, and **Pydantic**.

It provides database engine, connection pool, database connection, SQL execution, ORM mapping, database metadata, database configuration, and table structure management capabilities.

It uses **SQLAlchemy** for database engine and connection pool management, and **SQLModel** and **Pydantic** for ORM mapping models and data validation. It also provides unified interfaces for SQL statement construction, execution, and result processing.

All execution modules support both **synchronous methods** and **coroutine-based asynchronous methods**, making reydb suitable for database operations in both asynchronous and synchronous Python backend services.

> **Database Support**
>
> * `reydb <= 1.3.12`: MySQL
> * `reydb >= 1.3.13`: PostgreSQL

## Features

* Database engine and connection pool management based on SQLAlchemy
* ORM mapping models based on SQLModel
* Data models and field validation based on Pydantic
* PostgreSQL database support
* Synchronous database operations
* Coroutine-based asynchronous database operations
* Database engine connection pool management
* Database connection and connection lifecycle management
* Database event lifecycle management
* SQL statement construction and execution
* SQL parameter and value injection
* Extended SQL execution result processing
* Convert query results to `list[dict]` table structures
* Convert query results to JSON structures
* Database ORM operations
* ORM models, fields, and SQL syntax objects
* ORM data validation
* ORM database session and event management
* Database table creation and deletion
* Build database tables from ORM models or Python basic data types
* Database configuration data creation, modification, and deletion
* Python exception data storage in database tables
* Database metadata querying and modification
* PostgreSQL metadata operations for Schemas such as `pg_catalog` and `information_schema`

---

## Installation

Requires **Python 3.12 or higher**.

```bash
pip install reydb

```

---

## Quick Start

Create a database object:

```python
from reydb import Database

db = Database()

db(**engine1_args)
db(**engine2_args)

```

Each call to the `Database` object can be used to add a database engine and its connection pool configuration.

### Synchronous Execution

```python
result = db[engine_name].execute(sql)

result.to_table()

```

### Asynchronous Execution

```python
model = db[engine_name].orm.get(model)

db[engine_name].orm.insert(model).values(data).execute()

```

---

# Modules

reydb is divided into multiple database-related modules, with each module providing different database operation capabilities.

## `rall` — All import methods

**Unified import module.**
Provides convenient exports for all reydb modules, methods, and objects. It allows database-related functionality to be imported from a centralized module, reducing the need to import components separately from multiple modules.

---

## `rbase` — Base methods

**Base methods module.**
Provides basic dependency methods used by other modules.
Supports common functionality shared between reydb modules.

---

## `rbuild` — Database build methods

**Database build module.**
Provides methods for creating and deleting database table structures.
Main features:

* Create database tables
* Delete database tables
* Create tables from ORM mapping models
* Build tables from Python basic data types

---

## `rconfig` — Database config methods

**Database configuration module.**
Provides methods for storing and managing database configuration parameters.
Main features:

* Add database configuration parameters
* Modify database configuration parameters
* Delete database configuration parameters

It can be used to store database-related configuration parameters in database tables and manage them centrally.

---

## `rconn` — Database connection methods

**Database connection module.**
Provides database connection objects and database event objects.
Mainly includes:

* Database connection objects

  * Manage individual database connection resources
* Database event objects

  * Manage individual database event lifecycles

Used to manage database connections and their lifecycles.

---

## `rdb` — Database methods

**Top-level database module.**
Provides a top-level database object for centrally managing multiple database engines and their connection pool objects.
Main features:

* Create database objects
* Add database engines
* Manage multiple database connection pools
* Get the corresponding database engine object by engine name

---

## `rengine` — Database engine methods

**Database engine module.**
Provides database engine and connection pool objects.
It is mainly used to manage database connection resources and provide a unified engine access interface for upper-level database operations.
Main features:

* Database engine management
* Database connection pool management
* Database connection object management
* Database connection lifecycle management

---

## `rerror` — Database error methods

**Database error module.**
Provides methods for storing Python exception data in database tables.
Target functions can be wrapped using decorators or other mechanisms so that exception-related data is recorded in the database when an exception occurs during function execution.

---

## `rexec` — Database execute methods

**Database execution module.**
Provides functionality for SQL statement construction, execution, parameter injection, and value injection.
Main features:

* SQL statement construction
* SQL statement execution
* SQL parameter injection
* SQL value injection
* Standardized database operation workflows
* Extended `Result` result object

The `Result` object provides various result processing methods, such as:

* Convert results to table structures such as `list[dict]`
* Convert results to JSON structures
* Other database query result processing

---

## `rinfo` — Database information methods

**Database information module.**
Provides methods for querying and modifying database metadata.
It mainly works with PostgreSQL system Schemas, including:

* `pg_catalog`
* `information_schema`
* Other database metadata

It can be used to retrieve and manage metadata related to databases, Schemas, tables, fields, and other database objects.

---

## `rorm` — Database ORM methods

**Database ORM module.**
Provides database ORM mapping and related operations.
Mainly includes:

* ORM mapping model base classes
* ORM mapping model field objects
* ORM abstractions for various SQL syntax objects
* ORM field value validation classes
* ORM database session objects
* ORM database event objects

Among them:

* **ORM database session objects**: Manage individual database connection resources during ORM operations
* **ORM database event objects**: Manage individual database event lifecycles during ORM operations

ORM models can be used to map database table structures and perform corresponding database operations.

---

# Module Overview

| Module    |                                                    |
| --------- | -------------------------------------------------- |
| `rall`    | Unified exports for all methods                    |
| `rbase`   | Base methods and shared module dependencies        |
| `rbuild`  | Database table creation and deletion               |
| `rconfig` | Database configuration parameter management        |
| `rconn`   | Database connection and event management           |
| `rdb`     | Top-level database object and engine management    |
| `rengine` | Database engine and connection pool management     |
| `rerror`  | Python exception data storage                      |
| `rexec`   | SQL construction, execution, and result processing |
| `rinfo`   | Database metadata management                       |
| `rorm`    | ORM mapping and database operations                |

---

# Database Support

reydb supports the following database versions:

| reydb Version |            |
| ------------- | ---------- |
| `<= 1.3.12`   | MySQL      |
| `>= 1.3.13`   | PostgreSQL |

The current version `1.3.71` uses **PostgreSQL**.

---

# Dependencies

Main dependencies:

* `asyncpg`
* `psycopg[binary]`
* `pydantic[email]`
* `reykit`
* `sqlalchemy==2.0.42`
* `sqlmodel`

---

# Project Information

| Project Information |                                                           |
| ------------------- | --------------------------------------------------------- |
| Name                | `reydb`                                                   |
| Version             | `1.3.71`                                                  |
| Python              | `>=3.12`                                                  |
| Database            | `PostgreSQL`                                              |
| Author              | `Rey`                                                     |
| Email               | `reyxbo@163.com`                                          |
| Homepage            | [REYXBO](https://www.reyxbo.com/release/python/reydb)     |
| Repository          | [reydb-py](https://github.com/reyxbo/reydb-py.git)        |

## Keywords

`rey` · `reyxbo` · `PostgreSQL` · `postgres` · `db` · `database` · `orm` · `async` · `asynchronous` · `build`
