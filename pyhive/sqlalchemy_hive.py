"""Integration between SQLAlchemy and Hive.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import, unicode_literals

import ast
import datetime as dt
import itertools
import json
import re
from collections import UserDict

from sqlalchemy import (Integer, String, exc, literal, literal_column, null,
                        types, util)
from sqlalchemy.databases import mysql
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import array as pg_array
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
# TODO shouldn't use mysql type
from sqlalchemy.schema import ColumnCollectionMixin, SchemaItem
from sqlalchemy.sql import (Alias, FromClause, compiler, crud, elements,
                            operators)
from sqlalchemy.sql.base import DialectKWArgs, _generative
from sqlalchemy.sql.compiler import DDLCompiler, SQLCompiler
from sqlalchemy.sql.dml import Insert as StandardInsert
from sqlalchemy.sql.elements import (ColumnClause, _anonymous_label, _clone,
                                     _literal_as_binds)
from sqlalchemy.sql.expression import (ClauseElement, ColumnElement,
                                       Executable, FunctionElement,
                                       _literal_as_text)
from sqlalchemy.sql.functions import FunctionElement, GenericFunction
from sqlalchemy.sql.selectable import _interpret_as_from
from sqlalchemy.sql.sqltypes import Date, Indexable
from sqlalchemy.types import TypeDecorator, UserDefinedType, to_instance

from pyhive import hive
from pyhive.common import UniversalSet

# @compiles(dt.date)
# def compile_lateral_view(date, compiler, **kwargs):
#     date_str = date.strftime('%Y-%m-%d')
#     return f"DATE '{date_str}'"

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable


class CreateTableAs(Executable, ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs)
def _create_table_as(element, compiler, **kw):
    return "CREATE TABLE %s AS %s" % (
        element.name,
        compiler.process(element.query)
    )



class Insert(StandardInsert):
    @_generative
    def overwrite(self):
        self._overwrite = True
        return self


class Explain(Executable, ClauseElement):
    def __init__(self, stmt, mode=None):
        '''EXPLAIN [EXTENDED|DEPENDENCY|AUTHORIZATION|LOCKS|VECTORIZATION] query
        '''
        self.statement = stmt
        self.mode = mode


@compiles(Explain)
def compile_explain(explain, compiler, **kwargs):
    statement_str = compiler.process(explain.statement, **kwargs)
    if explain.mode is None:
        mode = ''
    else:
        mode = explain.mode
    return (f'EXPLAIN {mode} {statement_str}')


class HiveResultParseError(Exception):
    pass


class array(pg_array):
    def __init__(self, clauses, **kw):
        super(array, self).__init__(clauses, **kw)
        self.type = ARRAY(self.type.item_type)


def identity(_): return _


class ARRAY(PG_ARRAY):
    def _proc_array(self, arr, itemproc, dim, collection):
        if isinstance(arr, str):
            arr = ast.literal_eval(arr)
        return super(ARRAY, self)._proc_array(arr, itemproc, dim, collection)

    def literal_processor(self, dialect):
        item_proc = self.item_type.dialect_impl(
            dialect).literal_processor(dialect)

        if not item_proc:
            item_proc = identity

        def process(value):
            value_str = ', '.join(item_proc(v) for v in value)
            return f'array({value_str})'
        return process

    def adapt(self, impltype):
        return ARRAY(self.item_type)


def convert_to_columnelement(value, type_):
    if value is None:
        return null().cast(type_)
    elif isinstance(value, (str, int)):
        return literal(value)
#     elif isinstance(value, dt.date):
#         return literal(value, type_=DATE())
    elif isinstance(value, Struct):
        return Struct(value.names(), value.values(), value.types())
    elif isinstance(value, dt.date):
        return literal_column(f'DATE "{value:%Y-%m-%d}"')
    elif isinstance(value, list):
        return array(value)
    elif isinstance(value, Map):
        return value
    else:
        print(type(value))
        assert False


class Struct(GenericFunction):
    __slots__ = ('_col_names', '_col_values', '_col_value_map')
    name = 'named_struct'

    def __init__(self,
                 names=None, values=None, types=None, type_=None):
        self._col_names = tuple(names)
        self._col_values = tuple(values)
        self._col_value_map = dict(zip(names, values))

        if types is None and type_ is None:
            types = [_literal_as_binds(v).type for v in values]

        if type_ is not None and types is None:
            names_from_type_, types = zip(
                *[(n, t) for n, t in type_.name_type_pairs])
            if names_from_type_ != tuple(names):
                raise ValueError('`names` differ from column names in `type_`')
        if type_ is None:
            type_ = STRUCT(list(zip(names, types)))

        self._col_types = tuple(types)
        self._col_type_map = dict(zip(names, types))

        col_eles = [convert_to_columnelement(
            v, t) for v, t in zip(values, types)]

        args = itertools.chain.from_iterable(zip(names, col_eles))
        super().__init__(*args, type_=type_)

    def values(self):
        return self._col_values

    def names(self):
        return self._col_names

    def types(self):
        return self._col_types

    def type_of(self, name):
        return self._col_type_map[name]

    def value_of(self, name):
        return self._col_value_map[name]

    def __iter__(self):
        for value in self._col_values:
            yield value

    def __len__(self):
        return len(self._col_values)

    def __getitem__(self, name):
        self._col_value_map[name]

    def __getattr__(self, name):
        try:
            # for fixing pickle.load
            col_dict = object.__getattribute__(self, '_col_value_map')
            return col_dict[name]
        except KeyError as e:
            raise AttributeError(e.args[0])

    def __hash__(self):
        return hash((self._col_names, self._col_values))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._col_names == other._col_names and
                    self._col_values == other._col_values)
        else:
            return False

    def __repr__(self):
        # kwargs_str = ', '.join(f'{k}={v!r}' for k, v in self._col_value_map.items())

        return f'{self.__class__.__name__}(names={self._col_names}, values={self._col_values})'


try:
    from collections.abc import Sequence
    Sequence.register(Struct)
except ImportError:
    pass


class StructElement(ColumnElement):
    '''
    Instances of this class wrap a Hive struct type.
    '''
    __visit_name__ = 'struct_element'

    def __init__(self, base, col, type_):
        self.name = col
        self.key = col
        self.type = to_instance(type_)
        self.is_literal = True
        self.base = base

        ColumnElement.__init__(self)


@compiles(StructElement)
def compile_struct_element(element, compiler, **kw):
    return '%s.%s' % (
        compiler.process(element.base, **kw), element.name
    )


class UDTF(FunctionElement):
    __visit_name__ = 'function'
    col_type_pairs = []


class stack(UDTF):
    name = 'stack'

    def __init__(self, n, *args, names=None):
        self.n = n
        column_len = len(args) // n
        if names is None:
            names = [f'col{i}'for i in range(column_len)]
        self.col_type_pairs = [
            (name, _literal_as_binds(arg).type)
            for name, arg in zip(names, args[:column_len])]
        UDTF.__init__(self, n, *args)


class inline(UDTF):
    name = 'inline'

    def __init__(self, arr):
        self.arr = arr
        struct_type = arr.type.item_type
        self.col_type_pairs = struct_type.name_type_pairs
        UDTF.__init__(self, self.arr)

class posexplode(UDTF):
    name = 'posexplode'

    def __init__(self, clause, *, names, **kw):
        self.clause = _literal_as_binds(clause)
        self.col_type_pairs = [(names[0], Integer),
                               (names[1], clause.type.item_type)]
        super(posexplode, self).__init__(clause, **kw)

class explode(UDTF):
    name = 'explode'

    def __init__(self, clause, *, names, **kw):
        self.clause = _literal_as_binds(clause)
        if isinstance(clause.type, ARRAY):
            self.col_type_pairs = [(names[0], clause.type.item_type)]
        elif isinstance(clause.type, MAP):
            self.col_type_pairs = [(names[0], clause.type.key_type),
                                   (names[1], clause.type.value_type)]
        super(explode, self).__init__(clause, **kw)


class parse_url_tuple(UDTF):
    name = 'parse_url_tuple'

    def __init__(self, url, *parts, names=None, **kw):
        if names is None:
            names = [part.replace(':', '_') for part in parts]

        self.col_type_pairs = [(name, String())
                               for name,  part in zip(names, parts)]

        super(parse_url_tuple, self).__init__(url, *parts, **kw)


class LateralView(FromClause):
    __visit_name__ = 'lateral_view'

    named_with_column = True
    _is_from_container = True

    def __init__(self, from_table, udtf_expr, name=None):
        baseselectable = from_table
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.element
        while isinstance(baseselectable, LateralView):
            baseselectable = baseselectable.from_table
        self.original = baseselectable
        self.from_table = _interpret_as_from(from_table)
        self.udtf_expr = udtf_expr
        if name is None:
            if self.original.named_with_column:
                name = getattr(self.original, 'name', None)
            name = _anonymous_label('%%(%d %s)s' %
                                    (id(self), name or 'anon'))
        self.name = name

    def _populate_column_collection(self):
        columns = self.from_table.columns

        self.primary_key.extend(c for c in columns if c.primary_key)
        self._columns.update((col._label, col) for col in columns)

        for col_name, col_type in self.udtf_expr.col_type_pairs:
            co = ColumnClause(col_name, type_=col_type, _selectable=self)
            self._columns[col_name] = co

    def _copy_internals(self, clone=_clone, **kw):
        self._reset_exported()
        self.from_table = clone(self.from_table, **kw)
        self.udtf_expr = clone(self.udtf_expr, **kw)
        self.original = clone(self.original, **kw)

    def _refresh_for_new_column(self, column):
        raise NotImplementedError

    def select(self, whereclause=None, **kwargs):
        raise NotImplementedError

    def self_group(self, against=None):
        # TODO
        raise NotImplementedError

    def get_children(self, **kwargs):
        return self.from_table.get_children(**kwargs)
        raise NotImplementedError

    @property
    def _hide_froms(self):
        return itertools.chain(*[x.from_table._from_objects
                                 for x in self._cloned_set])

    @property
    def _from_objects(self):
        return [self] + self.from_table._from_objects


@compiles(LateralView)
def compile_lateral_view(lateral_view, compiler, asfrom=False, **kwargs):
    from_table_str = lateral_view.from_table._compiler_dispatch(
        compiler, asfrom=True, **kwargs)
    udtf_expr_str = lateral_view.udtf_expr._compiler_dispatch(
        compiler, **kwargs)
    # udtf_columns_str = ', '.join(compiler.preparer.quote(c)
    #                              for c in lateral_view.udtf_column_names)
    column_names = [
        compiler.preparer.quote(col_name)
        for col_name, col_type in lateral_view.udtf_expr.col_type_pairs]
    column_names_str = ','.join(column_names)
    if isinstance(lateral_view.name, elements._truncated_label):
        udtf_table_name = compiler._truncated_identifier(
            "lateral_view", lateral_view.name)
    else:
        udtf_table_name = lateral_view.name

    udtf_table_name = compiler.preparer.quote(udtf_table_name)

    return (f'{from_table_str} LATERAL VIEW {udtf_expr_str} '
            f'{udtf_table_name} AS {column_names_str}')


class DATE(TypeDecorator):
    impl = types.DATE
    __visit_name__ = 'DATE'

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            elif isinstance(value, dt.date):
                return f"DATE '{value:%Y-%m-%d}'"
            else:
                raise TypeError(
                    'Hive Date type only accepts Python date objects as input.')
        return process

    def get_dbapi_type(self, dbapi):
        from IPython.core.debugger import set_trace
        set_trace()
        return dbapi.DATE


class TIMESTAMP(TypeDecorator):
    impl = types.TIMESTAMP

    def process_literal_param(self, value, dialect):
        if value is None:
            return None
        elif isinstance(value, dt.date):
            return f"'{value:%Y-%m-%d %H:%M:%S.%f}'"
        else:
            raise TypeError(
                'Hive Timestamp type only accepts Python datetime objects as input.')

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        elif isinstance(value, dt.date):
            return f"'{value:%Y-%m-%d %H:%M:%S.%f}'"
        else:
            raise TypeError(
                'Hive Timestamp type only accepts Python datetime objects as input.')

    def get_dbapi_type(self, dbapi):
        from IPython.core.debugger import set_trace
        set_trace()
        return dbapi.DATE


class STRUCT(UserDefinedType):
    __visit_name__ = 'STRUCT'
    python_type = Struct
    shoulde_evaluate_none = True
    hashable = True

    def get_col_spec(self):
        from IPython.core.debugger import set_trace
        set_trace()
        type_strs = ['{}:{}'.format(col_name, col_type._compiler_dispatch())
                     for col_name, col_type in self.name_type_pairs]
        return 'STRUCT<{}>'.format(', '.join(type_strs))

    def __init__(self, name_type_pairs):
        self.name_type_pairs = name_type_pairs
        self.name_type_map = {n: t for n, t in name_type_pairs}

    def bind_processor(self, dialect):
        from IPython.core.debugger import set_trace
        set_trace()
        name_proc = String().dialect_impl(dialect).bind_processor(dialect)

        if name_proc is None:
            name_proc = identity

        # TODO: bind for complex type
        # def process(value):
        #     return repr(value)

        def process(value):
            from IPython.core.debugger import set_trace
            set_trace()
            from sqlalchemy import func, literal
            return func.named_struct(value.items(), type_=self).compile(dialect=dialect)
            args = itertools.chain.from_iterable(
                (name_proc(n),
                 self.name_type_map[n].dialect_impl(
                     dialect).bind_processor(dialect)(v),
                 )
                for n, v in value.items())
            arg_str = ', '.join(args)
            return f'named_struct({arg_str})'

        return process

    # def bind_expression(self, bindvalue):
    #     from IPython.core.debugger import set_trace
    #     set_trace()
    #     from sqlalchemy import func, literal
    #     return func.named_struct(bindvalue.value.items(), type_=bindvalue.type)

    def literal_processor(self, dialect):
        name_proc = String().dialect_impl(dialect).literal_processor(dialect)

        def process(value):
            from IPython.core.debugger import set_trace
            set_trace()
            args = itertools.chain.from_iterable(
                (name_proc(n),
                 self.name_type_map[n].dialect_impl(
                     dialect).literal_processor(dialect)(v),
                 )
                for n, v in value.items())
            arg_str = ', '.join(args)
            return f'named_struct({arg_str})'

        return process

    def result_processor(self, dialect, coltype):

        def get_type_proc(t):
            proc = t.dialect_impl(dialect).result_processor(dialect, coltype)
            if proc is None:
                return identity
            else:
                return proc

        col_procs_map = {
            n: get_type_proc(t)
            for n, t in self.name_type_pairs}

        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                d = json.loads(value)
            elif isinstance(value, dict):
                d = value
            elif isinstance(value, Struct):
                return value
            else:
                raise HiveResultParseError()

            if not isinstance(d, dict):
                raise HiveResultParseError()

            d_proced_v = {k: col_procs_map[k](v)
                          for k, v in d.items()}

            names = d_proced_v.keys()
            values = [col_procs_map[k](v)
                      for k, v in d_proced_v.items()]

            return Struct(names=names, values=values, type_=self)

        return process

    class comparator_factory(UserDefinedType.Comparator):
        """Define comparison operations for :class:`STRUCT`."""

        def __getattr__(self, key):
            try:
                type_ = self.type.name_type_map[key]
            except KeyError:
                raise AttributeError(
                    'Type %r doesn\'t have an attribute: %s' % (
                        self.type, key)
                )
            return StructElement(self.expr, key, type_)

        # def __getitem__(self, name):
        #     return self.operate(struct_getcol, name, self.type.name_type_map[name])

        # def _setup_getitem(self, index):
        #     return operators.getitem, index, self.type.name_type_map[index]

        # def __getattr__(self, name):
        #     return self.operate(struct_getcol, name, self.type.name_type_map[name])


class MAP(Indexable, UserDefinedType):
    __visit_name__ = 'MAP'
    python_type = dict
    should_evaluate_none = True
    hashable = False

    def __init__(self, key_type, value_type):

        # key_type should be primitive type
        self.key_type = (key_type()
                         if isinstance(key_type, type) else key_type)
        self.value_type = (value_type()
                           if isinstance(value_type, type) else value_type)

    # def literal_processor(self, dialect):
    #     def process(value):

    def bind_processor(self, dialect):
        # TODO: bind for complex type
        key_proc = self.key_type.dialect_impl(dialect).\
            bind_processor(dialect)
        value_proc = self.value_type.dialect_impl(dialect).\
            bind_processor(dialect)

        def process(value):
            return repr(value)
        return process

    def result_processor(self, dialect, coltype):
        key_proc = self.key_type.dialect_impl(dialect).\
            result_processor(dialect, coltype)
        value_proc = self.value_type.dialect_impl(dialect).\
            result_processor(dialect, coltype)

        if not key_proc:
            def key_proc(x): return x

        if not value_proc:
            def value_proc(x): return x

        def process(value):
            if value is None:
                return None
            if isinstance(value, Map):
                return value
            elif isinstance(value, dict):
                return Map(value, type_=self)
            elif isinstance(value, str):
                evaluated = ast.literal_eval(value)
                if not isinstance(evaluated, dict):
                    raise HiveResultParseError()
                evaluated = {key_proc(k): value_proc(v)
                             for k, v in evaluated.items()}

                return Map(evaluated, type_=self)
            else:
                raise HiveResultParseError()
        return process

    class Comparator(Indexable.Comparator):
        """Define comparison operations for :class:`MAP`."""

        def __getitem__(self, index):
            return self.operate(
                operators.getitem,
                index,
                result_type=self.type.value_type
            )

    comparator_factory = Comparator


class HiveIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()

    def __init__(self, dialect):
        super(HiveIdentifierPreparer, self).__init__(
            dialect,
            initial_quote='`',
        )


_type_map = {
    'boolean': types.Boolean,
    'tinyint': mysql.MSTinyInteger,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': types.BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'date': DATE,
    'timestamp': TIMESTAMP,
    'binary': types.String,
    'array': ARRAY,
    'map': MAP,
    'struct': types.String,
    'uniontype': types.String,
    'decimal': types.DECIMAL,
}


class HiveSQLCompiler(SQLCompiler):

    def render_literal_value(self, value, type_):
        if isinstance(value, dt.date):

            type_ = DATE()
            # from IPython.core.debugger import set_trace
            # set_trace()
        return super(HiveSQLCompiler, self).render_literal_value(value, type_)

    def visit_getitem_binary(self, binary, operator, **kw):
        return "%s[%s]" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    # def visit_struct_element(self, element, **kw):
    #     return '%s.%s' % (
    #         self.process(element.clauses, **kw), element.name
    #     )
    def visit_DATE(self, element, **kw):
        from IPython.core.debugger import set_trace
        set_trace()
        return 'DATE %s' % (self.process(element))

    def visit_array(self, element, **kw):
        return 'array({})'.format(self.visit_clauselist(element, **kw))

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_lateral_view(self, lateral_view, asfrom=False, **kwargs):
        from_table_str = lateral_view.from_table._compiler_dispatch(
            self, asfrom=True, **kwargs)
        udtf_expr_str = lateral_view.udtf_expr._compiler_dispatch(
            self, **kwargs)
        # udtf_columns_str = ', '.join(self.preparer.quote(c)
        #                              for c in lateral_view.udtf_column_names)
        column_names = [
            self.preparer.quote(col_name)
            for col_name, col_type in lateral_view.udtf_expr.col_type_pairs]
        column_names_str = ','.join(column_names)
        if isinstance(lateral_view.name, elements._truncated_label):
            udtf_table_name = self._truncated_identifier(
                "lateral_view", lateral_view.name)
        else:
            udtf_table_name = lateral_view.name

        udtf_table_name = self.preparer.quote(udtf_table_name)

        return (f'{from_table_str} LATERAL VIEW {udtf_expr_str} '
                f'{udtf_table_name} AS {column_names_str}')

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        toplevel = not self.stack

        self.stack.append(
            {'correlate_froms': set(),
             "asfrom_froms": set(),
             "selectable": insert_stmt})

        crud_params = crud._setup_crud_params(
            self, insert_stmt, crud.ISINSERT, **kw)

        if not crud_params and \
                not self.dialect.supports_default_values and \
                not self.dialect.supports_empty_insert:
            raise exc.CompileError("The '%s' dialect with current database "
                                   "version settings does not support empty "
                                   "inserts." %
                                   self.dialect.name)

        if insert_stmt._has_multi_parameters:
            if not self.dialect.supports_multivalues_insert:
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support "
                    "in-place multirow inserts." %
                    self.dialect.name)
            crud_params_single = crud_params[0]
        else:
            crud_params_single = crud_params

        preparer = self.preparer
        supports_default_values = self.dialect.supports_default_values

        text = "INSERT "

        if insert_stmt._prefixes:
            text += self._generate_prefixes(insert_stmt,
                                            insert_stmt._prefixes, **kw)

        if getattr(insert_stmt, '_overwrite', False):
            text += "OVERWRITE "
        else:
            text += "INTO "

        table_text = preparer.format_table(insert_stmt.table)

        # Add PARTITION(partitions) hint
        partitioned_by = getattr(insert_stmt.table, 'partitioned_by', False)
        if partitioned_by:
            pt_columns_str = ', '.join(self.preparer.format_column(c)
                                       for c in partitioned_by.columns)
            insert_stmt = insert_stmt.with_hint(
                f'PARTITION ({pt_columns_str})')

        if insert_stmt._hints:
            dialect_hints, table_text = self._setup_crud_hints(
                insert_stmt, table_text)
        else:
            dialect_hints = None

        text += "TABLE " + table_text

        # if crud_params_single or not supports_default_values:
        #     text += " (%s)" % ', '.join([preparer.format_column(c[0])
        #                                  for c in crud_params_single])

        if self.returning or insert_stmt._returning:
            returning_clause = self.returning_clause(
                insert_stmt, self.returning or insert_stmt._returning)

            if self.returning_precedes_values:
                text += " " + returning_clause
        else:
            returning_clause = None

        if insert_stmt.select is not None:
            text += " %s" % self.process(self._insert_from_select, **kw)
        elif not crud_params and supports_default_values:
            text += " DEFAULT VALUES"
        elif insert_stmt._has_multi_parameters:
            text += " VALUES %s" % (
                ", ".join(
                    "(%s)" % (
                        ', '.join(c[1] for c in crud_param_set)
                    )
                    for crud_param_set in crud_params
                )
            )
        else:
            text += " VALUES (%s)" % \
                ', '.join([c[1] for c in crud_params])

        if insert_stmt._post_values_clause is not None:
            post_values_clause = self.process(
                insert_stmt._post_values_clause, **kw)
            if post_values_clause:
                text += " " + post_values_clause

        if returning_clause and not self.returning_precedes_values:
            text += " " + returning_clause

        if self.ctes and toplevel:
            text = self._render_cte_clause() + text

        self.stack.pop(-1)

        if asfrom:
            return "(" + text + ")"
        else:
            return text

    def visit_column(self, *args, **kwargs):
        result = super(HiveSQLCompiler, self).visit_column(*args, **kwargs)
        return result
        dot_count = result.count('.')
        assert dot_count in (
            0, 1, 2), "Unexpected visit_column result {}".format(result)
        if dot_count == 2:
            # we have something of the form schema.table.column
            # hive doesn't like the schema in front, so chop it out
            result = result[result.index('.') + 1:]
        return result

    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))

    def get_select_hint_text(self, byfroms):
        return None

    def get_from_hint_text(self, table, text):
        # TODO:
        return text

    def get_crud_hint_text(self, table, text):
        return None

    def get_statement_hint_text(self, hint_texts):
        return " ".join(hint_texts)


class HiveTypeCompiler(compiler.GenericTypeCompiler):
    def visit_INTEGER(self, type_):
        return 'INT'

    def visit_NUMERIC(self, type_):
        return 'DECIMAL'

    def visit_CHAR(self, type_):
        return 'STRING'

    def visit_VARCHAR(self, type_):
        return 'STRING'

    def visit_NCHAR(self, type_):
        return 'STRING'

    def visit_TEXT(self, type_):
        return 'STRING'

    def visit_CLOB(self, type_):
        return 'STRING'

    def visit_BLOB(self, type_):
        return 'BINARY'

    def visit_TIME(self, type_):
        return 'TIMESTAMP'

    def visit_DATE(self, type_):
        return 'DATE'

    def visit_DATETIME(self, type_):
        return 'TIMESTAMP'

    def visit_ARRAY(self, type_):
        return 'ARRAY<{}>'.format(self.process(type_.item_type))

    def visit_MAP(self, type_):
        return 'MAP<{}, {}>'.format(
            self.process(type_.key_type),
            self.process(type_.value_type))

    def visit_STRUCT(self, type_):
        type_strs = ['{}:{}'.format(col_name, self.process(col_type))
                     for col_name, col_type in type_.name_type_pairs]
        return 'STRUCT<{}>'.format(', '.join(type_strs))


class HiveExecutionContext(default.DefaultExecutionContext):
    """This is pretty much the same as SQLiteExecutionContext to work around the same issue.

    http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#dotted-column-names

    engine = create_engine('hive://...', execution_options={'hive_raw_colnames': True})
    """

    @util.memoized_property
    def _preserve_raw_colnames(self):
        # Ideally, this would also gate on hive.resultset.use.unique.column.names
        return self.execution_options.get('hive_raw_colnames', False)

    def _translate_colname(self, colname):
        # Adjust for dotted column names.
        # When hive.resultset.use.unique.column.names is true (the default), Hive returns column
        # names as "tablename.colname" in cursor.description.
        if not self._preserve_raw_colnames and '.' in colname:
            return colname.split('.')[-1], colname
        else:
            return colname, None


class HiveDDLCompiler(DDLCompiler):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + \
            self.dialect.type_compiler.process(
                column.type, type_expression=column)
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        # if not column.nullable:
        #     colspec += " NOT NULL"
        return colspec

    def create_table_constraints(
        self, table,
            _include_foreign_key_constraints=None):
        # no constraints
        return ''

    def visit_create_column(self, create, first_pk=False):
        # ignore partition keys
        column = create.element
        if hasattr(column.table, 'partitioned_by'):
            if column.name in column.table.partitioned_by.columns:
                return None
        return DDLCompiler.visit_create_column(self, create, first_pk=False)

    def create_table_suffix(self, table):
        '''
        CREATE TABLE <create_tabel_suffix> (...)
        '''
        return ''

    def visit_partitioned_by(self, partitioned_by):

        partition_by_str = ', '.join(self.get_column_specification(c)
                                     for c in partitioned_by.columns)
        partition_by_str = f'PARTITIONED BY ({partition_by_str})'
        return partition_by_str

    def post_create_table(self, table):
        '''
        CREATE TABLE (...) <post_create_table>
        '''
        table_opts = []

        if hasattr(table, 'partitioned_by'):
            table_opts.append(self.process(table.partitioned_by))

        dialect_name = self.dialect.name
        if isinstance(dialect_name, bytes):
            dialect_name = dialect_name.decode()

        opts = dict(
            (
                k[len(dialect_name) + 1:].upper(),
                v
            )
            for k, v in table.kwargs.items()
            if k.startswith('%s_' % dialect_name)
        )

        # Example
        # PARTITIONED BY (YYYY INT, MM INT, DD INT)
        # CLUSTERED BY (beacon) INTO 2 BUCKETS
        # STORED AS ORC
        # TBLPROPERTIES ('transactional'='true');

        for opt in util.topological.sort([('PARTITIONED_BY', 'STORED_AS')], opts):
            arg = opts[opt]
            # if opt in _reflection._options_of_type_string:
            #     arg = "'%s'" % arg.replace("\\", "\\\\").replace("'", "''")

            if opt in ('DATA_DIRECTORY', 'INDEX_DIRECTORY',
                       'DEFAULT_CHARACTER_SET', 'CHARACTER_SET',
                       'DEFAULT_CHARSET',
                       'DEFAULT_COLLATE', 'PARTITIONED_BY',
                       'CLUSTERED_BY',
                       'STORED_AS'
                       ):
                opt = opt.replace('_', ' ')

            joiner = '='
            if opt in ('TABLESPACE', 'DEFAULT CHARACTER SET',
                       'CHARACTER SET', 'COLLATE',
                       'PARTITIONED BY', 'PARTITIONS',
                       'STORED AS',
                       ):
                joiner = ' '

            table_opts.append(joiner.join((opt, arg)))
        return '\n' + '\n'.join(table_opts)


class PartitionedBy(ColumnCollectionMixin, DialectKWArgs, SchemaItem):
    __visit_name__ = 'partitioned_by'

    def __init__(self, *columns, **kw):
        SchemaItem.__init__(self, **kw)
        DialectKWArgs.__init__(self, **kw)
        ColumnCollectionMixin.__init__(self, *columns, **kw)

    def _set_parent(self, table):
        ColumnCollectionMixin._set_parent(self, table)
        table.partitioned_by = self


class HiveDialect(default.DefaultDialect):
    name = b'hive'
    driver = b'thrift'
    execution_ctx_cls = HiveExecutionContext
    preparer = HiveIdentifierPreparer
    statement_compiler = HiveSQLCompiler
    ddl_compiler = HiveDDLCompiler
    supports_views = True
    supports_alter = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_multivalues_insert = True
    # dbapi_type_map = {
    #     'DATE_TYPE': DATE()
    # }
    # dbapi_type_map = {
    #     'DATE_TYPE': HiveDate(),
    #     'TIMESTAMP_TYPE': HiveTimestamp(),
    #     'DECIMAL_TYPE': types.DECIMAL()
    # }
    colspecs = {
        types.TIMESTAMP: TIMESTAMP,
        types.DATE: DATE
    }
    type_compiler = HiveTypeCompiler
    _json_deserializer = None
    _json_serializer = None

    @classmethod
    def dbapi(cls):
        return hive

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 10000,
            'username': url.username,
            'password': url.password,
            'database': url.database or 'default',
        }
        kwargs.update(url.query)
        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES
        return [row[0] for row in connection.execute('SHOW SCHEMAS')]

    def get_view_names(self, connection, schema=None, **kw):
        # Hive does not provide functionality to query tableType
        # This allows reflection to not crash at the cost of being inaccurate
        return self.get_table_names(connection, schema, **kw)

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # This needs the table name to be unescaped (no backticks).
            rows = connection.execute(
                'DESCRIBE {}'.format(full_table)).fetchall()
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(full_table))
            if re.search(regex, e.args[0]):
                raise exc.NoSuchTableError(full_table)
            else:
                raise
        else:
            # Hive is stupid: this is what I get from DESCRIBE some_schema.does_not_exist
            regex = r'Table .* does not exist'
            if len(rows) == 1 and re.match(regex, rows[0].col_name):
                raise exc.NoSuchTableError(full_table)
            return rows

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            if col_name == '# Partition Information':
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            col_type = re.search(r'^\w+', col_type).group(0)
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (col_type, col_name))
                coltype = types.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        for i, (col_name, _col_type, _comment) in enumerate(rows):
            if col_name == '# Partition Information':
                break
        # Handle partition columns
        col_names = []
        for col_name, _col_type, _comment in rows[i + 1:]:
            col_names.append(col_name)
        if col_names:
            return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        else:
            return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return [row[0] for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions for Hive
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True


class str_to_map(GenericFunction):
    type = MAP(String(), String())
    name = 'str_to_map'


class map_keys(GenericFunction):
    type = ARRAY(String())
    name = 'map_keys'


class instr(GenericFunction):
    type = Integer()
    name = 'instr'


class parse_url(GenericFunction):
    type = String()
    name = 'parse_url'


class length(GenericFunction):
    type = Integer()
    name = 'length'


class to_date(GenericFunction):
    type = Date()
    name = 'to_date'


class concat_ws(GenericFunction):
    type = String()
    name = 'concat_ws'


class named_struct(GenericFunction):
    name = 'named_struct'

    def __init__(self, name_value_pairs, type_=None):
        if isinstance(name_value_pairs, dict):
            name_value_pairs = [(name, value)
                                for name, value in name_value_pairs.items()]

        args = itertools.chain.from_iterable(name_value_pairs)
        if type_ is None:
            type_ = STRUCT([(name, _literal_as_binds(value).type)
                            for name, value in name_value_pairs])
        super().__init__(*args, type_=type_)


class collect_list(GenericFunction):
    name = 'collect_list'

    def __init__(self, col):
        type_ = ARRAY(_literal_as_binds(col).type)
        super().__init__(col, type_=type_)


class collect(GenericFunction):
    '''
    collect() from brickhouse
    '''

    name = 'collect'

    def __init__(self, col):
        type_ = ARRAY(_literal_as_binds(col).type)
        super().__init__(col, type_=type_)


class collect_set(GenericFunction):
    name = 'collect_set'

    def __init__(self, col):
        type_ = ARRAY(_literal_as_binds(col).type)
        super().__init__(col, type_=type_)


class Map(GenericFunction):
    '''
    >>> func.map({1: 'B', 2: 'A'})
    '''

    name = 'map'

    def __init__(self, data, type_=None):
        kwargs = {}
        if type_ is not None:
            kwargs['type_'] = type_
        else:
            if not data:
                raise ValueError('')
            key, value = next(iter(data.items()))
            key_type = _literal_as_binds(value).type
            value_type = _literal_as_binds(key).type
            kwargs['type_'] = MAP(key_type, value_type)

        args = []
        for k, v in data.items():
            args.append(k)
            args.append(v)

        self.data = data

        GenericFunction.__init__(self, *args, **kwargs)

    def values(self):
        return self.data.values()

    def keys(self):
        return self.data.keys()

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.data)})'
