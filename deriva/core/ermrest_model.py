
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

    def create_schema(self, catalog, schema_def):
        """Add a new schema to this model in the remote database based on schema_def.

           Returns a new Schema instance based on the server-supplied
           representation of the newly created schema.

           The returned Schema is also added to self.schemas.
        """
        sname = schema_def['schema_name']
        if sname in self.schemas:
            raise ValueError('Schema %s already exists.' % sname)
        r = catalog.post(
            self.uri_path,
            json=[schema_def],
        )
        r.raise_for_status()
        d = r.json()
        assert len(d) == 1
        newschema = Schema(sname, d[0])
        self.schemas[sname] = newschema
        return newschema

class Schema (_ec.CatalogSchema):
    """Named schema.
    """
    def __init__(self, sname, schema_doc, **kwargs):
        super(Schema, self).__init__(sname, schema_doc, **_kwargs(**kwargs))
        self.comment = schema_doc.get('comment')

    @classmethod
    def define(cls, sname, comment=None, acls={}, annotations={}):
        """Build a schema definition.
        """
        return {
            "schema_name": sname,
            "acls": acls,
            "annotations": annotations,
        }

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

    def delete(self, catalog, model=None):
        """Remove this schema from the remote database.

        Also remove this schema from the local model object (if provided).

        :param catalog: an ErmrestCatalog object
        :param schema: a Schema object or None

        """
        if model is not None:
            if self.name not in model.schemas:
                raise ValueError('Schema %s does not appear to belong to model.' % (self,))
        catalog.delete(self.update_uri_path).raise_for_status()
        if model is not None:
            del model.schemas[self.name]

class Table (_ec.CatalogTable):
    """Named table.
    """
    def __init__(self, sname, tname, table_doc, **kwargs):
        super(Table, self).__init__(sname, tname, table_doc, **_kwargs(**kwargs))
        self.comment = table_doc.get('comment')
        self.kind = table_doc.get('kind')

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

    def delete(self, catalog, schema=None):
        """Remove this table from the remote database.

        Also remove this table from the local schema object (if provided).

        :param catalog: an ErmrestCatalog object
        :param schema: a Schema object or None

        """
        if schema is not None:
            if self.name not in schema.tables:
                raise ValueError('Table %s does not appear to belong to schema %s.' % (self, schema))
        catalog.delete(self.update_uri_path).raise_for_status()
        if schema is not None:
            del schema.tables[self.name]

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
        
    def delete(self, catalog, table=None):
        """Remove this column from the remote database.

        Also remove this column from the local table object (if provided).

        :param catalog: an ErmrestCatalog object
        :param table: a Table object or None

        """
        if table is not None:
            if self.name not in table.column_definitions.elements:
                raise ValueError('Column %s does not appear to belong to table %s.' % (self, table))
        catalog.delete(self.update_uri_path).raise_for_status()
        if table is not None:
            del table.column_definitions[self.name]

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

    def delete(self, catalog, table=None):
        """Remove this key from the remote database.

        Also remove this key from the local table object (if provided).

        :param catalog: an ErmrestCatalog object
        :param table: a Table object or None

        """
        if table is not None:
            if self.names[0] not in table.keys.elements:
                raise ValueError('Key %s does not appear to belong to table %s.' % (self, table))
        catalog.delete(self.update_uri_path).raise_for_status()
        if table is not None:
            del table.keys[self.names[0]]

class ForeignKey (_ec.CatalogForeignKey):
    """Named foreign key.
    """
    def __init__(self, sname, tname, fkey_doc, **kwargs):
        super(ForeignKey, self).__init__(sname, tname, fkey_doc, **_kwargs(**kwargs))
        self.comment = fkey_doc.get('comment')
        self.on_delete = fkey_doc.get('on_delete')
        self.on_update = fkey_doc.get('on_update')

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
            'on_delete': self.on_delete,
            'on_update': self.on_update,
        })
        return d

    def delete(self, catalog, table=None):
        """Remove this foreign key from the remote database.

        Also remove this foreign key from the local table object (if provided).

        :param catalog: an ErmrestCatalog object
        :param table: a Table object or None

        """
        if table is not None:
            if self.names[0] not in table.foreign_keys.elements:
                raise ValueError('Foreign key %s does not appear to belong to table %s.' % (self, table))
        catalog.delete(self.update_uri_path).raise_for_status()
        if table is not None:
            del table.foreign_keys[self.names[0]]

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
                'boolean'
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

