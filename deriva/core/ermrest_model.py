
from . import ermrest_config as _ec
import re

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

class NoChange (object):
    """Special class used to distinguish no-change default arguments to methods.

       Values for no-change are distinct from all valid values for
    these arguments.

    """
    pass

# singletone to use in APIs below
nochange = NoChange()

class Model (_ec.CatalogConfig):
    """Top-level catalog model.
    """
    def __init__(self, catalog, model_doc, **kwargs):
        super(Model, self).__init__(catalog, model_doc, **_kwargs(**kwargs))

    def create_schema(self, schema_def):
        """Add a new schema to this model in the remote database based on schema_def.

           Returns a new Schema instance based on the server-supplied
           representation of the newly created schema.

           The returned Schema is also added to self.schemas.
        """
        sname = schema_def['schema_name']
        if sname in self.schemas:
            raise ValueError('Schema %s already exists.' % sname)
        r = self.catalog.post(
            self.uri_path,
            json=[schema_def],
        )
        r.raise_for_status()
        d = r.json()
        assert len(d) == 1
        newschema = Schema(self, sname, d[0])
        self.schemas[sname] = newschema
        self.digest_fkeys()
        return newschema

def strip_nochange(d):
    return {
        k: v
        for k, v in d.items()
        if v is not nochange
    }

class Schema (_ec.CatalogSchema):
    """Named schema.
    """
    def __init__(self, model, sname, schema_doc, **kwargs):
        super(Schema, self).__init__(model, sname, schema_doc, **_kwargs(**kwargs))
        self.comment = schema_doc.get('comment')

    @classmethod
    def define(cls, sname, comment=None, acls={}, annotations={}):
        """Build a schema definition.
        """
        return {
            "schema_name": sname,
            "acls": acls,
            "annotations": annotations,
            "comment": comment,
        }

    def prejson(self, prune=True):
        d = super(Schema, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    def alter(self, schema_name=nochange, comment=nochange, acls=nochange, annotations=nochange):
        """Alter existing schema definition.

        :param schema_name: Replacement schema name (default nochange)
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param annotations: Replacement annotations (default nochange)

        Returns self (to allow for optional chained access).

        """
        changes = strip_nochange({
            'schema_name': schema_name,
            'comment': comment,
            'acls': acls,
            'annotations': annotations,
        })

        r = self.catalog.put(self.uri_path, json=changes)
        r.raise_for_status()
        changed = r.json() # use changed vs changes to get server-digested values

        if 'schema_name' in changes:
            del self.model.schemas[self.name]
            self.name = changed['schema_name']
            self.model.schemas[self.name] = self

        if 'comment' in changes:
            self._comment = changed['comment']

        if 'acls' in changes:
            self.acls.clear()
            self.acls.update(changed['acls'])

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def create_table(self, table_def):
        """Add a new table to this schema in the remote database based on table_def.

           Returns a new Table instance based on the server-supplied
           representation of the newly created table.

           The returned Table is also added to self.tables.
        """
        tname = table_def['table_name']
        if tname in self.tables:
            raise ValueError('Table %s already exists.' % tname)
        r = self.catalog.post(
            '%s/table' % self.uri_path,
            json=table_def,
        )
        r.raise_for_status()
        newtable = Table(self, tname, r.json())
        self.tables[tname] = newtable
        self.model.digest_fkeys()
        return newtable

    def delete(self):
        """Remove this schema from the remote database.
        """
        if self.name not in self.model.schemas:
            raise ValueError('Schema %s does not appear to belong to model.' % (self,))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.model.schemas[self.name]

class Table (_ec.CatalogTable):
    """Named table.
    """
    def __init__(self, schema, tname, table_doc, **kwargs):
        super(Table, self).__init__(schema, tname, table_doc, **_kwargs(**kwargs))
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

        :param tname: the name of the newly defined table
        :param column_defs: a list of Column.define() results for extra or overridden column definitions
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param provide_system: whether to inject standard system column definitions when missing from column_defs

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

    @classmethod
    def define_vocabulary(cls, tname, curie_template, uri_template='/id/{RID}', column_defs=[], key_defs=[], fkey_defs=[], comment=None, acls={}, acl_bindings={}, annotations={}, provide_system=True):
        """Build a vocabulary table definition.

        :param tname: the name of the newly defined table
        :param curie_template: the RID-based template for the CURIE of locally-defined terms, e.g. 'MYPROJECT:{RID}'
        :param uri_template: the RID-based template for the URI of locally-defined terms, e.g. 'https://server.example.org/id/{RID}'
        :param column_defs: a list of Column.define() results for extra or overridden column definitions
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param provide_system: whether to inject standard system column definitions when missing from column_defs

        These core vocabulary columns are generated automatically if
        absent from the input column_defs.

        - ID: ermrest_curie, unique not null, default curie template "%s:{RID}" % curie_prefix
        - URI: ermrest_uri, unique not null, default URI template "/id/{RID}"
        - Name: text, unique not null
        - Description: markdown, not null
        - Synonyms: text[]

        However, caller-supplied definitions override the default.

        """
        if not re.match("^[A-Za-z][-_A-Za-z0-9]*:[{]RID[}]$", curie_template):
            raise ValueError("The curie_template '%s' is invalid." % curie_template)

        if not re.match("^[-_.~?%#=&!*()@:;/+$A-Za-z0-9]+[{]RID[}][-_.~?%#=&!*()@:;/+$A-Za-z0-9]*$", uri_template):
            raise ValueError("The uri_template '%s' is invalid." % uri_template)

        def add_vocab_columns(custom):
            return [
                col_def
                for col_def in [
                        Column.define(
                            'ID',
                            builtin_types['ermrest_curie'],
                            nullok=False,
                            default=curie_template,
                            comment='The preferred Compact URI (CURIE) for this term.'
                        ),
                        Column.define(
                            'URI',
                            builtin_types['ermrest_uri'],
                            nullok=False,
                            default=uri_template,
                            comment='The preferred URI for this term.'
                        ),
                        Column.define(
                            'Name',
                            builtin_types['text'],
                            nullok=False,
                            comment='The preferred human-readable name for this term.'
                        ),
                        Column.define(
                            'Description',
                            builtin_types['markdown'],
                            nullok=False,
                            comment='A longer human-readable description of this term.'
                        ),
                        Column.define(
                            'Synonyms',
                            builtin_types['text[]'],
                            comment='Alternate human-readable names for this term.'
                        ),
                ]
                if col_def['name'] not in { c['name']: c for c in custom }
            ] + custom

        def add_vocab_keys(custom):
            def ktup(k):
                return tuple(k['unique_columns'])
            return [
                key_def
                for key_def in [
                        Key.define(['ID']),
                        Key.define(['URI']),
                ]
                if ktup(key_def) not in { ktup(kdef): kdef for kdef in custom }
            ] + custom

        return cls.define(
            tname,
            add_vocab_columns(column_defs),
            add_vocab_keys(key_defs),
            fkey_defs,
            comment if comment is not None else 'A set of controlled vocabular terms.',
            acls,
            acl_bindings,
            annotations,
            provide_system
        )

    @classmethod
    def define_asset(cls,
                     sname,
                     tname,
                     hatrac_template=None,
                     column_defs=[],
                     key_defs=[],
                     fkey_defs=[],
                     comment=None,
                     acls={},
                     acl_bindings={},
                     annotations={},
                     provide_system=True):
        """Build an asset  table definition.

          :param sname: the name of the schema for the asset table
          :param tname: the name of the newly defined table
          :param hatrac_template: template for the hatrac URL.  Will undergo substitution to template can include
                 elmenents such at {{{MD5}}} or {{{Filename}}}. The default template puts files in
                     /hatrac/schema_name/table_name/md5.filename
                 where the filename and md5 value is computed on upload and the schema_name and table_name are the
                 values of the provided arguments.  If value is set to False, no hatrac_template is used.
          :param column_defs: a list of Column.define() results for extra or overridden column definitions
          :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
          :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
          :param comment: a comment string for the table
          :param acls: a dictionary of ACLs for specific access modes
          :param acl_bindings: a dictionary of dynamic ACL bindings
          :param annotations: a dictionary of annotations
          :param provide_system: whether to inject standard system column definitions when missing from column_defs

          These core asset table columns are generated automatically if
          absent from the input column_defs.

          - Filename: ermrest_curie, unique not null, default curie template "%s:{RID}" % curie_prefix
          - URL: Location of the asset, unique not null.  Default template is:
                    /hatrac/sname/tname/{{{MD5}}}.{{{Filename}}} where tname is the name of the asset table.
          - Length: Length of the asset.
          - MD5: text
          - Description: markdown, not null

          However, caller-supplied definitions override the default.

          In addition to creating the columns, this function also creates an asset annotation on the URL column to
          facilitate use of the table by Chaise.
          """

        if hatrac_template is None:
            hatrac_template = '/hatrac/%s/%s/{{{MD5}}}.{{#encode}}{{{Filename}}}{{/encode}}' % (sname, tname)

        def add_asset_annotations(custom):
            annotations.update(custom)
            return annotations

        def add_asset_columns(custom):
            asset_annotation = {
                _ec.tag.asset: {
                    'filename_column': 'Filename',
                    'byte_count_column': 'Length',
                    'md5': 'MD5',
                }
            }
            if hatrac_template:
                asset_annotation[_ec.tag.asset]['url_pattern'] = hatrac_template
            return [
                col_def
                for col_def in [
                        Column.define(
                            'URL', builtin_types['text'],
                            nullok=False,
                            annotations=asset_annotation,
                            comment='URL to the asset',
                        ),
                        Column.define('Filename', builtin_types['text'], comment='Filename of the asset that was uploaded'),
                        Column.define('Description', builtin_types['markdown'], comment='Description of the asset'),
                        Column.define('Length', builtin_types['int8'], nullok=False, comment='Asset length (bytes)'),
                        Column.define('MD5', builtin_types['text'], nullok=False, comment='Asset content MD5 checksum'),
                ]
                if col_def['name'] not in {c['name']: c for c in custom}
            ] + custom

        def add_asset_keys(custom):
            def ktup(k):
                return tuple(k['unique_columns'])

            return [
                key_def
                for key_def in [
                    Key.define(['URL']),
                ]
                if ktup(key_def) not in {ktup(kdef): kdef for kdef in custom}
            ] + custom

        return cls.define(
            tname,
            add_asset_columns(column_defs),
            add_asset_keys(key_defs),
            fkey_defs,
            comment if comment is not None else 'Asset table.',
            acls,
            acl_bindings,
            add_asset_annotations(annotations),
            provide_system
        )

    def prejson(self, prune=True):
        d = super(Table, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    def alter(
            self,
            schema_name=nochange,
            table_name=nochange,
            comment=nochange,
            acls=nochange,
            acl_bindings=nochange,
            annotations=nochange
    ):
        """Alter existing schema definition.

        :param schema_name: Destination schema name (default nochange)
        :param table_name: Replacement table name (default nochange)
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)

        A change of schema name is a transfer of the existing table to
        an existing destination schema (not a rename of the current
        containing schema).

        Returns self (to allow for optional chained access).

        """
        changes = strip_nochange({
            'schema_name': schema_name,
            'table_name': table_name,
            'comment': comment,
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        })

        r = self.catalog.put(self.uri_path, json=changes)
        r.raise_for_status()
        changed = r.json() # use changed vs changes to get server-digested values

        if 'table_name' in changes:
            del self.schema.tables[self.name]
            self.name = changed['table_name']
            self.schema.tables[self.name] = self

        if 'schema_name' in changes:
            del self.schema.tables[self.name]
            self.schema = self.schema.model.schemas[changed['schema_name']]
            for fkey in self.foreign_keys:
                if fkey.constraint_schema:
                    del fkey.constraint_schema._fkeys[fkey.constraint_name]
                    fkey.constraint_schema = self.schema
                    fkey.constraint_schema._fkeys[fkey.constraint_name] = fkey
            self.schema.tables[self.name] = self

        if 'comment' in changes:
            self._comment = changed['comment']

        if 'acls' in changes:
            self.acls.clear()
            self.acls.update(changed['acls'])

        if 'acls_bindings' in changes:
            self.acl_bindings.clear()
            self.acl_bindings.update(changed['acls'])

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def _create_table_part(self, subapi, registerfunc, constructor, doc):
        r = self.catalog.post(
            '%s/%s' % (self.uri_path, subapi),
            json=doc,
        )
        r.raise_for_status()
        created = r.json()
        if isinstance(created, list):
            # handle fkey case where POST returns a list
            assert len(created) == 1
            created = created[0]
        return registerfunc(constructor(self, created))

    def create_column(self, column_def):
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
        return self._create_table_part('column', add_column, Column, column_def)

    def create_key(self, key_def):
        """Add a new key to this table in the remote database based on key_def.

           Returns a new Key instance based on the server-supplied
           representation of the new key, and adds it to self.keys
           too.

        """
        def add_key(key):
            self.keys.append(key)
            return key
        return self._create_table_part('key', add_key, Key, key_def)

    def create_fkey(self, fkey_def):
        """Add a new foreign key to this table in the remote database based on fkey_def.

           Returns a new ForeignKey instance based on the
           server-supplied representation of the new foreign key, and
           adds it to self.fkeys too.

        """
        def add_fkey(fkey):
            self.foreign_keys.append(fkey)
            fkey.digest_referenced_columns(self.schema.model)
            return fkey
        return self._create_table_part('foreignkey', add_fkey, ForeignKey, fkey_def)

    def delete(self):
        """Remove this table from the remote database.
        """
        if self.name not in self.schema.tables:
            raise ValueError('Table %s does not appear to belong to schema %s.' % (self, self.schema))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.schema.tables[self.name]

class Column (_ec.CatalogColumn):
    """Named column.
    """
    def __init__(self, table, column_doc, **kwargs):
        super(Column, self).__init__(table, column_doc, **_kwargs(**kwargs))
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

    def alter(
            self,
            name=nochange,
            type=nochange,
            nullok=nochange,
            default=nochange,
            comment=nochange,
            acls=nochange,
            acl_bindings=nochange,
            annotations=nochange
    ):
        """Alter existing schema definition.

        :param name: Replacement column name (default nochange)
        :param type: Replacement Type instance (default nochange)
        :param nullok: Replacement nullok value (default nochange)
        :param default: Replacement default value (default nochange)
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)

        Returns self (to allow for optional chained access).

        """
        if type is not nochange and not isinstance(type, Type):
            raise TypeError('Parameter "type" %s should be an instance of Type.' % (type,))
        changes = strip_nochange({
            'name': name,
            'type': type,
            'nullok': nullok,
            'default': default,
            'comment': comment,
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        })

        r = self.catalog.put(self.uri_path, json=changes)
        r.raise_for_status()
        changed = r.json() # use changed vs changes to get server-digested values

        if 'name' in changes:
            del self.table.column_definitions.elements[self.name]
            self.name = changed['name']
            self.table.column_definitions.elements[self.name] = self

        if 'type' in changes:
            self.type = make_type(changed['type'])

        if 'nullok' in changes:
            self.nullok = changed['nullok']

        if 'default' in changes:
            self.default = changed['default']

        if 'comment' in changes:
            self._comment = changed['comment']

        if 'acls' in changes:
            self.acls.clear()
            self.acls.update(changed['acls'])

        if 'acls_bindings' in changes:
            self.acl_bindings.clear()
            self.acl_bindings.update(changed['acls'])

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def prejson(self, prune=True):
        d = super(Column, self).prejson(prune)
        d.update({
            'type': self.type.prejson(prune),
            'nullok': self.nullok,
            'default': self.default,
            'comment': self.comment,
        })
        return d
        
    def delete(self):
        """Remove this column from the remote database.
        """
        if self.name not in self.table.column_definitions.elements:
            raise ValueError('Column %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.column_definitions[self.name]

class Key (_ec.CatalogKey):
    """Named key.
    """
    def __init__(self, table, key_doc, **kwargs):
        super(Key, self).__init__(table, key_doc, **_kwargs(**kwargs))
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

    def alter(
            self,
            constraint_name=nochange,
            comment=nochange,
            annotations=nochange
    ):
        """Alter existing schema definition.

        :param constraint_name: Unqualified constraint name string
        :param comment: Replacement comment (default nochange)
        :param annotations: Replacement annotations (default nochange)

        Returns self (to allow for optional chained access).

        """
        changes = strip_nochange({
            'comment': comment,
            'annotations': annotations,
        })
        if constraint_name is not nochange:
            changes['names'] = [[
                self.constraint_schema.name if self.constraint_schema else '',
                constraint_name
            ]]

        r = self.catalog.put(self.uri_path, json=changes)
        r.raise_for_status()
        changed = r.json() # use changed vs changes to get server-digested values

        if 'names' in changes:
            self.constraint_name = changed['names'][0][1]

        if 'comment' in changes:
            self._comment = changed['comment']

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def prejson(self, prune=True):
        d = super(Key, self).prejson(prune)
        d.update({
            'comment': self.comment,
        })
        return d

    def delete(self):
        """Remove this key from the remote database.
        """
        if self.names[0] not in self.table.keys.elements:
            raise ValueError('Key %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.update_uri_path).raise_for_status()
        del self.table.keys[self.names[0]]

class ForeignKey (_ec.CatalogForeignKey):
    """Named foreign key.
    """
    def __init__(self, table, fkey_doc, **kwargs):
        super(ForeignKey, self).__init__(table, fkey_doc, **_kwargs(**kwargs))
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
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        }

    def alter(
            self,
            constraint_name=nochange,
            comment=nochange,
            acls=nochange,
            acl_bindings=nochange,
            annotations=nochange
    ):
        """Alter existing schema definition.

        :param constraint_name: Replacement constraint name string
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)

        Returns self (to allow for optional chained access).

        """
        changes = strip_nochange({
            'comment': comment,
            'acls': acls,
            'acl_bindings': acl_bindings,
            'annotations': annotations,
        })
        if constraint_name is not nochange:
            changes['names'] = [[
                self.constraint_schema.name if self.constraint_schema else '',
                constraint_name
            ]]

        r = self.catalog.put(self.uri_path, json=changes)
        r.raise_for_status()
        changed = r.json() # use changed vs changes to get server-digested values

        if 'names' in changes:
            if self.constraint_schema:
                del self.constraint_schema._fkeys[self.constraint_name]
            else:
                del self.table.schema.model._pseudo_fkeys[self.constraint_name]
            self.constraint_name = changed['names'][0][1]
            if self.constraint_schema:
                self.constraint_schema._fkeys[self.constraint_name] = self
            else:
                self.table.schema.model._pseudo_fkeys[self.constraint_name] = self

        if 'comment' in changes:
            self._comment = changed['comment']

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        if 'acls' in changes:
            self.acls.clear()
            self.acls.update(changed['acls'])

        if 'acls_bindings' in changes:
            self.acl_bindings.clear()
            self.acl_bindings.update(changed['acls'])

        return self

    def prejson(self, prune=True):
        d = super(ForeignKey, self).prejson(prune)
        d.update({
            'comment': self.comment,
            'on_delete': self.on_delete,
            'on_update': self.on_update,
        })
        return d

    def delete(self):
        """Remove this foreign key from the remote database.
        """
        if self.names[0] not in self.table.foreign_keys.elements:
            raise ValueError('Foreign key %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.update_uri_path).raise_for_status()
        del self.table.foreign_keys[self.names[0]]
        del self.pk_table.foreign_keys[self.names[0]]

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
                'ermrest_curie': 'text',
                'ermrest_uri': 'text',
        }.items()
    }
)
builtin_types.update(
    # define standard serial types which don't have array types
    {
        typename: Type({'typename': typename})
        for typename in [ 'serial2', 'serial4', 'serial8' ]
    }
)
