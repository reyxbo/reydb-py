#!/usr/bin/env python3

"""
@Time    : 2024-01-07
@Author  : Rey
@Contact : reyxbo@163.com
@Explain : Database engine and connection pool package.
    It provides database engine, connection pool, database connection, SQL execution, ORM mapping, database metadata, database configuration, and table structure management capabilities.

Modules
-------
rall : Unified export module.
    Provides convenient exports for all reydb modules, methods, and objects.
    It allows database framework functionality to be imported from a centralized module, reducing the need to import components separately from multiple modules.
rbase : Base methods module.
    Provides basic dependency methods used by other modules.
    Supports common functionality shared between reydb modules.
rbuild : Database build module.
    Provides methods for creating and deleting database table structures.
rconfig : Database configuration module.
    Provides methods for storing and managing database configuration parameters.
rconn : Database connection module.
    Provides database connection objects and database event objects.
rdb : Top-level database module.
    Provides a top-level database object for centrally managing multiple database engines and their connection pool objects.
rengine : Database engine module.
    Provides database engine and connection pool objects.
    It is mainly used to manage database connection resources and provide a unified engine access interface for upper-level database operations.
rerror : Database error module.
    Provides methods for storing Python exception data in database tables.
    Target functions can be wrapped using decorators or other mechanisms so that
    exception-related data is recorded in the database when an exception occurs during function execution.
rexec : Database execution module.
    Provides functionality for SQL statement construction, execution, parameter injection, and value injection.
rinfo : Database information module.
    Provides methods for querying and modifying database metadata.
rorm : Database ORM module.
    Provides database ORM mapping and related operations.
"""

from .rdb import Database as Database, DatabaseAsync as DatabaseAsync
from .rengine import DatabaseEngine as DatabaseEngine, DatabaseEngineAsync as DatabaseEngineAsync
