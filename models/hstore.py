# Extracted from http://groups.google.com/group/sqlalchemy/browse_thread/thread/22075a4c71e0422d
# Kyle Schaffrick

import re
import sqlalchemy.types as satypes
import sqlalchemy.schema as saschema
import sqlalchemy.sql as sasql
import sqlalchemy.sql.expression as saexp
import sqlalchemy.sql.functions as safunc
import sqlalchemy.util as sautil
import sqlalchemy.dialects.postgresql as pgdialect
from sqlalchemy.exc import SQLAlchemyError

__all__ = [ 'HStoreSyntaxError', 'HStore', 'HStoreElement', 'pair',
        'HStoreColumn' ]

# My best guess at the parsing rules of hstore literals, since no formal
# grammar is given. This may be overkill since the docs say that current output
# implementation always quotes keys and values, but gives no explicit guarantee
# that this behavior is dependable. This is mostly reverse engineered from PG's
# input parser behavior.
HSTORE_PAIR_RE = re.compile(r"""
    (
        (?P<key> [^" ] [^= ]* )            # Unquoted keys
      | " (?P<key_q> ([^"] | \\ . )* ) "   # Quoted keys
    )
    [ ]* => [ ]*    # Pair operator, optional adjoining whitespace
    (
        (?P<value> [^" ] [^, ]* )          # Unquoted values
      | " (?P<value_q> ([^"] | \\ . )* ) " # Quoted values
    )
    """, re.VERBOSE)

HSTORE_DELIMITER_RE = re.compile(r"""
    [ ]* , [ ]*
    """, re.VERBOSE)

class HStoreSyntaxError(SQLAlchemyError):
    """Indicates an error unmarshalling an hstore value."""
    def __init__(self, hstore_str, pos):
        self.hstore_str = hstore_str
        self.pos = pos

        CTX = 20
        hslen = len(hstore_str)

        parsed_tail = hstore_str[ max(pos - CTX - 1, 0) : min(pos, hslen) ]
        residual = hstore_str[ min(pos, hslen) : min(pos + CTX + 1, hslen) ]

        if len(parsed_tail) > CTX:
            parsed_tail = '[...]' + parsed_tail[ 1 : ]
        if len(residual) > CTX:
            residual = residual[ : -1 ] + '[...]'

        super(HStoreSyntaxError, self).__init__(
                "After %r, could not parse residual at position %d: %r" %
                (parsed_tail, pos, residual))

def _parse_hstore(hstore_str):
    """
    Parse an hstore from it's literal string representation.
    
    Attempts to approximate PG's hstore input parsing rules as closely as
    possible. Although currently this is not strictly necessary, since the
    current implementation of hstore's output syntax is stricter than what it
    accepts as input, the documentation makes no guarantees that will always
    be the case.

    Throws HStoreSyntaxError if parsing fails.
    """
    result = {}
    pos = 0
    pair_match = HSTORE_PAIR_RE.match(hstore_str)

    while pair_match is not None:
        key = pair_match.group('key') or pair_match.group('key_q')
        key = key.decode('string_escape')
        value = pair_match.group('value') or pair_match.group('value_q')
        value = value.decode('string_escape')
        result[key] = value

        pos += pair_match.end()
                
        delim_match = HSTORE_DELIMITER_RE.match(hstore_str[ pos : ])
        if delim_match is not None:
            pos += delim_match.end()

        pair_match = HSTORE_PAIR_RE.match(hstore_str[ pos : ])

    if pos != len(hstore_str):
        raise HStoreSyntaxError(hstore_str, pos)

    return result

def _serialize_hstore(val):
    """
    Serialize a dictionary into an hstore literal. Keys and values must both be
    strings.
    """
    def esc(s, position):
        try:
            return s.encode('string_escape').replace('"', r'\"')
        except AttributeError:
            raise ValueError("%r in %s position is not a string." %
                    (s, position))
    return ', '.join( '"%s"=>"%s"' % (esc(k, 'key'), esc(v, 'value'))
            for k, v in val.iteritems() )


class HStore(satypes.MutableType, satypes.Concatenable, satypes.TypeEngine):
    """
    The column type for representing PostgreSQL's contrib/hstore type. This
    type is a miniature key-value store in a column. It supports query
    operators for all the usual operations on a map-like data structure.
    """

    name = 'hstore'

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return _serialize_hstore(value)
            else:
                return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return _parse_hstore(value)
            else:
                return value
        return process

    def _adapt_expression(self, op, other_type):
        if op in ['?', '@>', '<@']:
            return op, satypes.Boolean
        elif op == '->':
            return op, other_type
        else:
            return super(HStore, self)._adapt_expression(op, other_type)

    def copy_value(self, value):
        #added an existence check, otherwise it bails if there are no hstore pairs
        return dict(value) if value else dict()


class HStoreElement(saexp.ColumnElement):
    """
    An expression element that evaluates to an hstore object. This is where the
    expression language extensions for hstore types are implemented.
    """
    def has_key(self, other):
        """Boolean expression. Test for presence of a key. Note that the key
        may be a SQLA expression."""
        return self.op('?')(other)

    def contains(self, other):
        """Boolean expression. Test if keys are a superset of the keys of
        the argument hstore expression."""
        return self.op('@>')(other)

    def contained_by(self, other):
        """Boolean expression. Test if keys are a proper subset of the keys of
        the argument hstore expression."""
        return self.op('<@')(other)

    def __getitem__(self, other):
        """Text expression. Get the value at a given key. Note that the key may
        be a SQLA expression."""
        return self.op('->')(other)

    def concat(self, other):
        """HStore expression. Merge this hstore with the argument hstore, with
        duplicate keys taking the value from the argument."""
        return _HStoreBinaryExpression(self, other, '||', type_=HStore)

    def __add__(self, other):
        """HStore expression. Merge the left and right hstore expressions, with
        duplicate keys taking the value from the right expression."""
        return self.concat(other)

    def assoc(self, key, val):
        """HStore expression. Returns the contents of this hstore updating the
        given key with the given value. Note that the key, value, or both may
        be SQLA expressions."""
        return self.concat(pair(key, val))

    def dissoc(self, key):
        """HStore expression. Returns the contents of this hstore with the
        given key deleted. Note that the key may be a SQLA expression."""
        return _HStoreDeleteFunction(self, key)

    def keys(self):
        """Text array expression. Return array of keys."""
        return _HStoreKeysFunction(self)

    def vals(self):
        """Text array expression. Return array of values."""
        return _HStoreValsFunction(self)


class _HStoreBinaryExpression(HStoreElement, saexp._BinaryExpression):
    pass


class pair(_HStoreBinaryExpression):
    """
    Construct an hstore on the server side using the pair operator.

    This is different from a one-member hstore literal because the key and
    value are evaluated as SQLAlchemy expressions, so the key, value, or both
    may contain columns, function calls, or any other valid SQL expressions which
    evaluate to text.
    """
    def __init__(self, key, val):
        # HACK: We're borrowing this function from _BinaryExpression and
        # something in it's call graph blows up if the type is set, for reasons
        # I do not yet understand.
        self.type = None
        key = self._check_literal('=>', key)
        val = self._check_literal('=>', val)
        saexp._BinaryExpression.__init__(self, key, val, '=>', type_=HStore)


class _HStoreDeleteFunction(HStoreElement, safunc.GenericFunction):
    __return_type__ = HStore
    def __init__(self, store, key, **kwargs):
        safunc.GenericFunction.__init__(self, args=[store, key], **kwargs)
        self.name = 'delete'


class _HStoreKeysFunction(safunc.GenericFunction):
    __return_type__ = pgdialect.ARRAY(satypes.Text)
    def __init__(self, store, **kwargs):
        safunc.GenericFunction.__init__(self, args=[store], **kwargs)
        self.name = 'akeys'


class _HStoreValsFunction(safunc.GenericFunction):
    __return_type__ = pgdialect.ARRAY(satypes.Text)
    def __init__(self, store, **kwargs):
        safunc.GenericFunction.__init__(self, args=[store], **kwargs)
        self.name = 'avals'


class HStoreColumn(HStoreElement, saschema.Column):
    """Same as a regular Column, except it augments the SQL expression language
    with hstore features."""
    pass


if __name__ == '__main__':
    from sqlalchemy import create_engine, MetaData
    from sqlalchemy.schema import Column, Table
    from sqlalchemy.types import Integer, Text
    import sqlalchemy.sql as sql
    import sqlalchemy.orm as orm

    engine = create_engine('postgresql://test:test@gsr-db.local/sandbox')
    meta = MetaData()

    test_table = Table('test', meta,
            Column('id', Integer(), primary_key=True),
            HStoreColumn('hash', HStore()))

    conn = engine.connect()

    hashcol = test_table.c.hash
    where_tests = [
            hashcol.has_key('foo'),
            hashcol.contains({'foo': '1'}),
            hashcol.contained_by({'foo': '1'}) ]
    select_tests = [
            hashcol['foo'],
            hashcol.dissoc('foo'),
            pair('foo', '3')['foo'],
            hashcol.assoc(sql.cast(test_table.c.id, Text), '3'),
            hashcol + hashcol,
            (hashcol + hashcol)['foo'],
            hashcol.keys() ]

    for wt in where_tests:
        a = sql.select([test_table], whereclause=wt)
        a.bind = conn
        print str(a)
        print str(list(a.execute()))

    for st in select_tests:
        a = sql.select([st])
        a.bind = conn
        print str(a)
        print str(list(a.execute()))

    conn.close()

    class TestObj(object):
        def __init__(self, id_, hash_):
            self.id = id_
            self.hash = hash_
        def __repr__(self):
            return "TestObj(%r, %r)" % (self.id, self.hash)

    orm.mapper(TestObj, test_table)
    Session = orm.sessionmaker(bind=engine)
    ses = Session()

    print list(ses.query(TestObj).all())

    ses.close()

