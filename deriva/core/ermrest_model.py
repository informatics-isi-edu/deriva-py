
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

class Table (_ec.CatalogTable):
    """Named table.
    """
    def __init__(self, sname, tname, table_doc, **kwargs):
        super(Table, self).__init__(sname, tname, table_doc, **_kwargs(**kwargs))
        self.comment = table_doc.get('comment')

    def prejson(self, prune=True):
        d = super(Table, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    @classmethod
    def skeleton_table(cls, sname, tname, **kwargs):
        """Generate a table with 5 standard system columns and constraints.

           This returns a client-side table definition. It DOES NOT
           modify the remote database.
        """
        return kwargs.get('table_class', cls)(
            sname,
            tname,
            {
                'column_definitions': [
                    Column(
                        sname,
                        tname,
                        {
                            'name': cname,
                            'type': builtin_types[ctype].prejson(),
                            'nullok': nok,
                        }
                    ).prejson()
                    for cname, ctype, nok in [
                            ('RID', 'ermrest_rid', False),
                            ('RCT', 'ermrest_rct', False),
                            ('RMT', 'ermrest_rmt', False),
                            ('RCB', 'ermrest_rcb', True),
                            ('RMB', 'ermrest_rmb', True),
                    ]
                ],
                'keys': [
                    Key(
                        sname,
                        tname,
                        {
                            'names': [[sname, '%s_RID_key' % tname]],
                            'unique_columns': ['RID'],
                        }
                    ).prejson()
                ],
            },
            **kwargs
        )

    def create_column(self, catalog, cname, ctype, nullok=True, default=None, comment=None, acls={}, acl_bindings={}, annotations={}):
        """Create a new column in this table and return its representation.

           This method modifies the table definition in the remote catalog.
        """
        if cname in self.column_definitions.elements:
            raise ValueError('Column %s already exists.' % cname)
        if not isinstance(ctype, Type):
            raise TypeError('Ctype %s should be an instance of Type.' % ctype)
        if not isinstance(nullok, bool):
            raise TypeError('Nullok %s should be an instance of bool.' % nullok)
        r = catalog.post(
            '%s/column' % self.uri_path,
            json={
                'name': cname,
                'type': ctype.prejson(),
                'nullok': nullok,
                'default': default,
                'comment': comment,
                'acls': acls,
                'acl_bindings': acl_bindings,
                'annotations': annotations,
            }
        )
        r.raise_for_status()
        newcol = Column(self.sname, self.name, r.json())
        self.column_definitions.append(newcol)
        return newcol

class Column (_ec.CatalogColumn):
    """Named column.
    """
    def __init__(self, sname, tname, column_doc, **kwargs):
        super(Column, self).__init__(sname, tname, column_doc, **_kwargs(**kwargs))
        self.type = make_type(column_doc['type'], **_kwargs(**kwargs))
        self.nullok = bool(column_doc.get('nullok', True))
        self.default = column_doc.get('default')
        self.comment = column_doc.get('comment')

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

