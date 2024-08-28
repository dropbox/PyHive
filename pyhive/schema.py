"""
This module attempts to reconstruct an Arrow schema from the info dumped at the beginning of a Hive query log.

SUPPORTS:
 * All primitive types _except_ INTERVAL.
 * STRUCT and ARRAY types.
 * Composition of any combination of previous types.

LIMITATIONS:
 * PyHive does not support INTERVAL types yet. A converter needs to be implemented.
 * Hive sends complex types always as strings as something _similar_ to JSON.
 * Arrow can parse most of this pseudo-JSON excluding:
   * MAP and INTERVAL types
   * A custom parser would be needed to implement support for all types and their composition.
"""

import pyparsing as pp
import pyarrow as pa

def a_type(s, loc, toks):   
    m_basic = {
        'tinyint' : pa.int8(),
        'smallint' : pa.int16(),
        'int' : pa.int32(),
        'bigint' : pa.int64(),
        'float' : pa.float32(),
        'double' : pa.float64(),
        'boolean' : pa.bool_(),
        'string' : pa.string(),
        'char' : pa.string(),
        'varchar' : pa.string(),
        'binary' : pa.binary(),
        'timestamp' : pa.timestamp('ns'),
        'date' : pa.date32(),
        #'interval_year_month' : pa.month_day_nano_interval(),
        #'interval_day_time' : pa.month_day_nano_interval(),
    }
   
    typ, args = toks[0], toks[1:]

    if typ in m_basic:
        return m_basic[typ]
    if typ == 'decimal':
        return pa.decimal128(*map(int, args))
    if typ == 'array':
        return pa.list_(args[0])
    #if typ == 'map':
    #    return pa.map_(args[0], args[1])
    if typ == 'struct':
        return pa.struct(args)
    raise NotImplementedError(f"Type {typ} is not supported")

def a_field(s, loc, toks):
    return pa.field(toks[0], toks[1])

LB, RB, LP, RP, LT, RT, COMMA, COLON = map(pp.Suppress, "[]()<>,:")

def t_args(n):
    return LP + pp.delimitedList(pp.Word(pp.nums), ",", min=n, max=n) + RP

t_basic = pp.one_of(
    "tinyint smallint int bigint float double boolean string binary timestamp date decimal",
    caseless=True, as_keyword=True
)
t_interval = pp.one_of(
    "interval_year_month interval_day_time",
    caseless=True, as_keyword=True
)
t_char = pp.one_of("char varchar", caseless=True, as_keyword=True) + t_args(1)
t_decimal = pp.CaselessKeyword("decimal") + t_args(2)
t_primitive = (t_basic ^ t_char ^ t_decimal).set_parse_action(a_type)

t_type = pp.Forward()

t_label = pp.Word(pp.alphas + "_", pp.alphanums + "_")
t_array = pp.CaselessKeyword('array') + LT + t_type + RT
t_map = pp.CaselessKeyword('map') + LT + t_primitive + COMMA + t_type + RT
t_struct = pp.CaselessKeyword('struct') + LT + pp.delimitedList((t_label + COLON + t_type).set_parse_action(a_field), ",") + RT
t_complex = (t_array ^ t_map ^ t_struct).set_parse_action(a_type)

t_type <<= t_primitive ^ t_complex
t_top_type = t_type ^ t_interval

l_schema, l_fieldschemas, l_fieldschema, l_name, l_type, l_comment, l_properties, l_null = map(
    lambda x: pp.Keyword(x).suppress(), "Schema fieldSchemas FieldSchema name type comment properties null".split(' ')
)
t_fieldschema = l_fieldschema + LP + l_name + COLON + t_label.suppress() + COMMA + l_type + COLON + t_top_type + COMMA + l_comment + COLON + l_null + RP
t_schema = l_schema + LP + l_fieldschemas + COLON + LB + pp.delimitedList(t_fieldschema, ',') + RB + COMMA + l_properties + COLON + l_null + RP

def parse_schema(logs):
    prefix = 'INFO  : Returning Hive schema: '

    for l in logs:
        if l.startswith(prefix):
            str_schema = l[len(prefix):]

            return t_schema.parse_string(str_schema).as_list()
