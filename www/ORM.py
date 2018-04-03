import logging, asyncio


import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓数据库↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

# 创建一个连接池
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 定义一个全局变量连接池
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minseze=kw.get('minsize', 1),
        loop=loop
    )

# 封装SELECT语句
async def select(sql, args, size=None):
    # 打印SQL语句
    log(sql, args)
    # 导入全局变量连接池
    global __pool
    # 连接mysql数据库
    with (await __pool) as conn:
        # 返回该连接的游标
        cur = await conn.cursor(aiomysql.DictCursor)
        # 把'?'替换成'%s'，因为SQL语句的占位符是?,而mysql语句的占位符是'%s'
        await cur.execute(sql.replace('?', '%s'), args or ())
        # 如果存在size值就只取size数量的结果，如果不存在就全取
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        # 关闭cur游标
        await cur.close()
        # 打印返回多少行数
        logging.info('rows returned: %s' % len(rs))
        return rs

# 封装Insert, Update, Delete 语句
async def execute(sql, args):
    # 打印SQL语句
    log(sql)
    # 连接数据库
    with (await __pool) as conn:
        try:
            # 返回该连接的游标
            cur = await conn.cursor()
            # 替换占位符
            await cur.execute(sql.replace('?', '%s'), args)
            # 返回查询或更新所发生行数
            affected = cur.rowcount
            # 关闭游标
            await cur.close()
        except BaseException as e:
            raise
        return affected

# ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑数据库↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

# ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓表格↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

class ModelMetaclass(type):
    # __new__在实例的创建前调用
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Pield和主键名:
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    raise RuntimeError('Duplicate primary key for field: %s' % k)
                primaryKey = k
            else:
                fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?'% (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    # 调用父类的__init__初始化
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    # 特殊函数__getattr__可以像引用普通字段一样 user.id
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
            return value
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        # find objects by where clause.
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        # find number by select and where.
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args =  list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)

# ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑表格↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

# ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓数据类型↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

class Field(object):
    # name=变量名, column_type=列名, primary_key=主键, default=初始值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    # 被打印时显示格式为： 类名， 列名（即数据类型）：变量名
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', None, default)

# ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑数据类型↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
