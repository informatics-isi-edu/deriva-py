
from . import ermrest_config as _ec

def _kwargs(**kwargs):
    """Helper for extending ermrest_config with sub-types for the whole model tree."""
    kwargs2 = {
        'schema_class': Schema,
        'table_class': Table,
        'column_class': Column,
        'key_class': Key,
        'foreign_key_class': ForeignKey,
    }
    kwargs2.update(kwargs)
    return kwargs2

class Model (_ec.CatalogConfig):
    """Top-level catalog model.
    """
    def __init__(self, model_doc, **kwargs):
        super(Model, self).__init__(model_doc, **_kwargs(**kwargs))

class Schema (_ec.CatalogSchema):
    """Named schema.
    """
    def __init__(self, sname, schema_doc, **kwargs):
        super(Schema, self).__init__(sname, schema_doc, **_kwargs(**kwargs))
        self.comment = schema_doc.get('comment')

    def prejson(self, prune=True):
        d = super(Schema, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    def create_table(self, catalog, table_def):
        """Add a new table to this schema in the remote database based on table_def.

           Returns a new Table instance based on the server-supplied
           representation of the newly created table.

           The returned Table is also added to self.tables.
        """
        tname = table_def['table_name']
        if tname in self.tables:
            raise ValueError('Table %s already exists.' % tname)
        r = catalog.post(
            '%s/table' % self.uri_path,
            json=table_def,
        )
        r.raise_for_status()
        newtable = Table(self.name, tname, r.json())
        self.tables[tname] = newtable
        return newtable

class Table (_ec.CatalogTable):
    """Named table.
    """
    def __init__(self, sname, tname, table_doc, **kwargs):
        super(Table, self).__init__(sname, tname, table_doc, **_kwargs(**kwargs))
        self.comment = table_doc.get('comment')

    @classmethod
    def system_column_defs(cls, custom=[]):
        """Build standard system column definitions, merging optional custom definitions."""
        return [
            Column.define(cname, builtin_types[ctype], nullok)
            for cname, ctype, nullok in [
                    ('RID', 'ermrest_rid', False),
                    ('RCT', 'ermrest_rct', False),
                    ('RMT', 'ermrest_rmt', False),
                    ('RCB', 'ermrest_rcb', True),
                    ('RMB', 'ermrest_rmb', True),
            ]
            if cname not in { c['name']: c for c in custom }
        ] + custom

    @classmethod
    def system_key_defs(cls, custom=[]):
        """Build standard system key definitions, merging optional custom definitions."""
        def ktup(k):
            return tuple(k['unique_columns'])
        return [
            kdef for kdef in [
                Key.define(['RID'])
            ]
            if ktup(kdef) not in { ktup(kdef): kdef for kdef in custom }
        ] + custom

    @classmethod
    def define(cls, tname, column_defs=[], key_defs=[], fkey_defs=[], comment=None, acls={}, acl_bindings={}, annotations={}, provide_system=True):
        """Build a table definition.

           If provide_system == True (default) then standard sytem
           column and key definitions are injected.

        """
        if provide_system:
            column_defs = cls.system_column_defs(column_defs)
            key_defs = cls.system_key_defs(key_defs)

        return {
            'table_name': tname,
            'column_definitions': column_defs,
            'keys': key_defs,
            'foreign_keys': fkey_defs,
            'comment': comment,
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        }

    def prejson(self, prune=True):
        d = super(Table, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    def _create_table_part(self, catalog, subapi, registerfunc, constructor, doc):
        r = catalog.post(
            '%s/%s' % (self.uri_path, subapi),
            json=doc,
        )
        r.raise_for_status()
        created = r.json()
        if isinstance(created, list):
            # handle fkey case where POST returns a list
            assert len(created) == 1
            created = created[0]
        return registerfunc(constructor(self.sname, self.name, created))

    def create_column(self, catalog, column_def):
        """Add a new column to this table in the remote database based on column_def.

           Returns a new Column instance based on the server-supplied
           representation of the new column, and adds it to
           self.column_definitions too.
        """
        cname = column_def['name']
        if cname in self.column_definitions.elements:
            raise ValueError('Column %s already exists.' % cname)
        def add_column(col):
            self.column_definitions.append(col)
            return col
        return self._create_table_part(catalog, 'column', add_column, Column, column_def)

    def create_key(self, catalog, key_def):
        """Add a new key to this table in the remote database based on key_def.

           Returns a new Key instance based on the server-supplied
           representation of the new key, and adds it to self.keys
           too.

        """
        def add_key(key):
            self.keys.append(key)
            return key
        return self._create_table_part(catalog, 'key', add_key, Key, key_def)

    def create_fkey(self, catalog, fkey_def):
        """Add a new foreign key to this table in the remote database based on fkey_def.

           Returns a new ForeignKey instance based on the
           server-supplied representation of the new foreign key, and
           adds it to self.fkeys too.

        """
        def add_fkey(fkey):
            self.foreign_keys.append(fkey)
            return fkey
        return self._create_table_part(catalog, 'foreignkey', add_fkey, ForeignKey, fkey_def)

class Column (_ec.CatalogColumn):
    """Named column.
    """
    def __init__(self, sname, tname, column_doc, **kwargs):
        super(Column, self).__init__(sname, tname, column_doc, **_kwargs(**kwargs))
        self.type = make_type(column_doc['type'], **_kwargs(**kwargs))
        self.nullok = bool(column_doc.get('nullok', True))
        self.default = column_doc.get('default')
        self.comment = column_doc.get('comment')

    @classmethod
    def define(cls, cname, ctype, nullok=True, default=None, comment=None, acls={}, acl_bindings={}, annotations={}):
        """Build a column definition."""
        if not isinstance(ctype, Type):
            raise TypeError('Ctype %s should be an instance of Type.' % ctype)
        if not isinstance(nullok, bool):
            raise TypeError('Nullok %s should be an instance of bool.' % nullok)
        return {
            'name': cname,
            'type': ctype.prejson(),
            'nullok': nullok,
            'default': default,
            'comment': comment,
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        }

    def prejson(self, prune=True):
        d = super(Column, self).prejson(prune)
        d.update({
            'type': self.type.prejson(prune),
            'nullok': self.nullok,
            'default': self.default,
            'comment': self.comment,
        })
        return d
        
class Key (_ec.CatalogKey):
    """Named key.
    """
    def __init__(self, sname, tname, key_doc, **kwargs):
        super(Key, self).__init__(sname, tname, key_doc, **_kwargs(**kwargs))
        self.comment = key_doc.get('comment')

    @classmethod
    def define(cls, colnames, constraint_names=[], comment=None, annotations={}):
        """Build a key definition."""
        if not isinstance(colnames, list):
            raise TypeError('Colnames should be a list.')
        return {
            'unique_columns': list(colnames),
            'names': constraint_names,
            'comment': comment,
            'annotations': annotations,
        }

    def prejson(self, prune=True):
        d = super(Key, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

class ForeignKey (_ec.CatalogForeignKey):
    """Named foreign key.
    """
    def __init__(self, sname, tname, fkey_doc, **kwargs):
        super(ForeignKey, self).__init__(sname, tname, fkey_doc, **_kwargs(**kwargs))
        self.comment = fkey_doc.get('comment')

    @classmethod
    def define(cls, fk_colnames, pk_sname, pk_tname, pk_colnames, on_update='NO ACTION', on_delete='NO ACTION', constraint_names=[], comment=None, acls={}, acl_bindings={}, annotations={}):
        if len(fk_colnames) != len(pk_colnames):
            raise ValueError('The fk_colnames and pk_colnames lists must have the same length.')
        return {
            'foreign_key_columns': [
                {
                    'column_name': fk_colname
                }
                for fk_colname in fk_colnames
            ],
            'referenced_columns': [
                {
                    'schema_name': pk_sname,
                    'table_name': pk_tname,
                    'column_name': pk_colname,
                }
                for pk_colname in pk_colnames
            ],
            'on_update': on_update,
            'on_delete': on_delete,
            'names': constraint_names,
            'comment': comment,
        }

    def prejson(self, prune=True):
        d = super(ForeignKey, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

def make_type(type_doc, **kwargs):
    """Create instance of Type, DomainType, or ArrayType as appropriate for type_doc."""
    if type_doc.get('is_domain', False):
        return DomainType(type_doc, **kwargs)
    elif type_doc.get('is_array', False):
        return ArrayType(type_doc, **kwargs)
    else:
        return Type(type_doc, **kwargs)

class Type (object):
    """Named type.
    """
    def __init__(self, type_doc, **kwargs):
        self.typename = type_doc['typename']
        self.is_domain = False
        self.is_array = False

    def prejson(self, prune=True):
        d = {
            'typename': self.typename,
        }
        return d

class DomainType (Type):
    """Named domain type.
    """
    def __init__(self, type_doc, **kwargs):
        super(DomainType, self).__init__(type_doc, **kwargs)
        self.is_domain = True
        self.base_type = make_type(type_doc['base_type'], **kwargs)
        
    def prejson(self, prune=True):
        d = super(DomainType, self).prejson(prune)
        d.update({
            'is_domain': True,
            'base_type': self.base_type.prejson(prune)
        })
        return d

class ArrayType (Type):
    """Named domain type.
    """
    def __init__(self, type_doc, **kwargs):
        super(ArrayType, self).__init__(type_doc, **kwargs)
        is_array = True
        self.base_type = make_type(type_doc['base_type'], **kwargs)

    def prejson(self, prune=True):
        d = super(ArrayType, self).prejson(prune)
        d.update({
            'is_array': True,
            'base_type': self.base_type.prejson(prune)
        })
        return d

builtin_types = _ec.AttrDict(
    # first define standard scalar types
    {
        typename: Type({'typename': typename})
        for typename in {
                'date',
                'float4', 'float8',
                'json', 'jsonb',
                'int2', 'int4', 'int8',
                'text',
                'timestamptz', 'timestamp',
        }
    }
)
builtin_types.update(
    # define some typical array types
    {
        '%s[]' % typename: ArrayType({
            'typename': '%s[]' % typename,
            'is_array': True,
            'base_type': typedoc.prejson()
        })
        for typename, typedoc in builtin_types.items()
    }
)
builtin_types.update(
    # define standard domain types
    {
        domain: DomainType({
            'typename': domain,
            'is_domain': True,
            'base_type': builtin_types[basetypename].prejson(),
        })
        for domain, basetypename in {
                'ermrest_rid': 'text',
                'ermrest_rcb': 'text',
                'ermrest_rmb': 'text',
                'ermrest_rct': 'timestamptz',
                'ermrest_rmt': 'timestamptz',
                'markdown': 'text',
        }.items()
    }
)

