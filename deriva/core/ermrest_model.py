
from collections import OrderedDict
import json
import re

from . import AttrDict, tag, urlquote

class NoChange (object):
    """Special class used to distinguish no-change default arguments to methods.

       Values for no-change are distinct from all valid values for
    these arguments.

    """
    pass

# singletone to use in APIs below
nochange = NoChange()

def presence_annotation(tag_uri):
    """Decorator to establish property getter/setter/deleter for presence annotations.

       Usage example:

          @presence_annotation(tag.generated)
          def generated(self): pass

       The stub method will be discarded.
    """
    def helper(ignore):
        docstr = "Convenience property for managing presence of annotation %s" % tag_uri

        def getter(self):
            return tag_uri in self.annotations

        def setter(self, present):
            if present:
                self.annotations[tag_uri] = None
            else:
                self.annotations.pop(tag_uri, None)

        def deleter(self):
            self.annotations.pop(tag_uri, None)

        return property(getter, setter, deleter, docstr)

    return helper

def object_annotation(tag_uri):
    """Decorator to establish property getter/setter/deleter for object annotations.

       Usage example:

          @presence_annotation(tag.display)
          def display(self): pass

       The stub method will be discarded.
    """
    def helper(ignore):
        docstr = "Convenience property for managing content of object annotation %s" % tag_uri

        def getter(self):
            if tag_uri not in self.annotations:
                self.annotations[tag_uri] = AttrDict({})
            return self.annotations[tag_uri]

        def setter(self, value):
            if not isinstance(value, (dict, AttrDict)):
                raise TypeError('Unexpected object type %s for annotation %s' % (type(value), tag_uri))
            self.annotations[tag_uri] = AttrDict(value)

        def deleter(self):
            self.annotations.pop(tag_uri, None)

        return property(getter, setter, deleter, docstr)

    return helper

def equivalent(doc1, doc2, method=None):
    """Determine whether two dict/array/literal documents are structurally equivalent."""
    if method == 'acl_binding':
        # fill in defaults to avoid some false negatives on acl binding comparison
        if not isinstance(doc1, dict):
            return False
        def canonicalize(d):
            if not isinstance(d, dict):
                return d
            def helper(b):
                if not isinstance(b, dict):
                    return b
                return {
                    'projection': b['projection'],
                    'projection_type': b.get('projection_type'), # we can't provide default w/o type inference!
                    'types': b['types'],
                    'scope_acl': b.get('scope_acl', ['*']), # this is a common omission...
                }
            return {
                binding_name: helper(binding)
                for binding_name, binding in d.items()
            }
        return equivalent(canonicalize(doc1), canonicalize(doc2))
    elif isinstance(doc1, dict) and isinstance(doc2, dict):
        return equivalent(sorted(doc1.items()), sorted(doc2.items()))
    elif isinstance(doc1, (list, tuple)) and isinstance(doc2, (list, tuple)):
        if len(doc1) != len(doc2):
            return False
        for e1, e2 in zip(doc1, doc2):
            if not equivalent(e1, e2):
                return False
        return True
    return doc1 == doc2

class Model (object):
    """Top-level catalog model.
    """
    def __init__(self, catalog, model_doc):
        self._catalog = catalog
        self._pseudo_fkeys = {}
        self.acls = AttrDict(model_doc.get('acls', {}))
        self.annotations = dict(model_doc.get('annotations', {}))
        self.schemas = {
            sname: Schema(self, sname, sdoc)
            for sname, sdoc in model_doc.get('schemas', {}).items()
        }
        self.digest_fkeys()

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        return {
            "acls": self.acls,
            "annotations": self.annotations,
            "schemas": {
                sname: schema.prejson()
                for sname, schema in self.schemas.items()
            }
        }

    def digest_fkeys(self):
        """Finish second-pass digestion of foreign key definitions using full model w/ all schemas and tables.
        """
        for schema in self.schemas.values():
            for referer in schema.tables.values():
                for fkey in list(referer.foreign_keys):
                    try:
                        fkey.digest_referenced_columns(self)
                    except KeyError:
                        del referer.foreign_keys[fkey.name]

    @property
    def catalog(self):
        return self._catalog

    @property
    def uri_path(self):
        """URI to this model resource."""
        return "/schema"

    @classmethod
    def fromcatalog(cls, catalog):
        """Retrieve catalog config as a Model management object."""
        return cls(catalog, catalog.get("/schema").json())

    @classmethod
    def fromfile(cls, catalog, schema_file):
        """Deserialize a JSON schema file as a Model management object."""
        with open(schema_file) as sf:
            schema = sf.read()
        return cls(catalog, json.loads(schema, object_pairs_hook=OrderedDict))

    def clear(self, clear_comment=False):
        """Clear all configuration in catalog and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.annotations.clear()
        self.acls.clear()
        for schema in self.schemas.values():
            schema.clear(clear_comment=clear_comment)

    def apply(self, existing=None):
        """Apply catalog configuration to catalog unless existing already matches.

        :param existing: An instance comparable to self.

        The configuration in self will be applied recursively to the
        corresponding model nodes in schema.

        If existing is not provided (default), the current whole
        configuration will be retrieved from the catalog and used
        automatically to determine whether the configuration goals
        under this Model tree are already met or need to be remotely
        applied.

        """
        if existing is None:
            existing = self.fromcatalog(self.catalog)
        if not equivalent(self.annotations, existing.annotations):
            self.catalog.put('/annotation', json=self.annotations)
        if not equivalent(self.acls, existing.acls):
            self.catalog.put('/acl', json=self.acls)
        for sname, schema in self.schemas.items():
            schema.apply(existing.schemas[sname])

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

    def table(self, sname, tname):
        """Return table configuration for table with given name."""
        return self.schemas[sname].tables[tname]

    def column(self, sname, tname, cname):
        """Return column configuration for column with given name."""
        return self.table(sname, tname).column_definitions[cname]

    def fkey(self, constraint_name_pair):
        """Return configuration for foreign key with given name pair.

        Accepts (schema_name, constraint_name) pairs as found in many
        faceting annotations and (schema_obj, constraint_name) pairs
        as found in fkey.name fields.

        """
        sname, cname = constraint_name_pair
        if isinstance(sname, Schema):
            if self.schemas[sname.name] is sname:
                return sname._fkeys[cname]
            else:
                raise ValueError('schema object %s is not from same model tree' % (sname,))
        elif sname is None or sname == '':
            return self._pseudo_fkeys[cname]
        else:
            return self.schemas[sname]._fkeys[cname]

    @object_annotation(tag.bulk_upload)
    def bulk_upload(self): pass

    @object_annotation(tag.display)
    def display(self): pass

def strip_nochange(d):
    return {
        k: v
        for k, v in d.items()
        if v is not nochange
    }

class Schema (object):
    """Named schema.
    """
    def __init__(self, model, sname, schema_doc):
        self.model = model
        self.name = sname
        self.acls = AttrDict(schema_doc.get('acls', {}))
        self.annotations = dict(schema_doc.get('annotations', {}))
        self.comment = schema_doc.get('comment')
        self._fkeys = {}
        self.tables = {
            tname: Table(self, tname, tdoc)
            for tname, tdoc in schema_doc.get('tables', {}).items()
        }

    @property
    def catalog(self):
        return self.model.catalog

    @property
    def uri_path(self):
        """URI to this model resource."""
        return "/schema/%s" % urlquote(self.name)

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
        """Produce native Python representation of schema, suitable for JSON serialization."""
        return {
            "schema_name": self.name,
            "acls": self.acls,
            "annotations": self.annotations,
            "comment": self.comment,
            "tables": {
                tname: table.prejson()
                for tname, table in self.tables.items()
            }
        }

    def clear(self, clear_comment=False):
        """Clear all configuration in schema and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.acls.clear()
        self.annotations.clear()
        if clear_comment:
            self.comment = None
        for table in self.tables.values():
            table.clear(clear_comment=clear_comment)

    def apply(self, existing=None):
        """Apply configuration to corresponding schema in catalog unless existing already matches.

        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The state of self.comment, self.annotations, and self.acls
        will be applied to the server unless they match their
        corresponding state in existing.
        """
        changes = {}
        if existing is None or not equivalent(self.comment, existing.comment):
            changes['comment'] = self.comment
        if existing is None or not equivalent(self.annotations, existing.annotations):
            changes['annotations'] = self.annotations
        if existing is None or not equivalent(self.acls, existing.acls):
            changes['acls'] = self.acls
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)
        for tname, table in self.tables.items():
            table.apply(existing.tables[tname] if existing else None)

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
            self.comment = changed['comment']

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

    def drop(self):
        """Remove this schema from the remote database.
        """
        if self.name not in self.model.schemas:
            raise ValueError('Schema %s does not appear to belong to model.' % (self,))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.model.schemas[self.name]

    @object_annotation(tag.display)
    def display(self): pass

class KeyedList (list):
    """Keyed list."""
    def __init__(self, l):
        list.__init__(self, l)
        self.elements = {
            e.name: e
            for e in l
        }

    def __getitem__(self, idx):
        """Get element by key or by list index or slice."""
        if isinstance(idx, (int, slice)):
            return list.__getitem__(self, idx)
        else:
            return self.elements[idx]

    def __delitem__(self, idx):
        """Delete element by key or by list index or slice."""
        if isinstance(idx, int):
            victim = list.__getitem__(self, idx)
            list.__delitem__(self, idx)
            del self.elements[victim.name]
        elif isinstance(idx, slice):
            victims = [list.__getitem__(self, idx)]
            list.__delslice__(self, idx)
            for victim in victims:
                del self.elements[victim.name]
        else:
            victim = self.elements[idx]
            list.__delitem__(self, self.index(victim))
            del self.elements[victim.name]

    def append(self, e):
        """Append element to list and record its key."""
        if e.name in self.elements:
            raise ValueError('Element name %s already exists.' % (e.name,))
        list.append(self, e)
        self.elements[e.name] = e

class Table (object):
    """Named table.
    """
    def __init__(self, schema, tname, table_doc):
        self.schema = schema
        self.name = tname
        self.acls = AttrDict(table_doc.get('acls', {}))
        self.acl_bindings = AttrDict(table_doc.get('acl_bindings', {}))
        self.annotations = dict(table_doc.get('annotations', {}))
        self.comment = table_doc.get('comment')
        self.kind = table_doc.get('kind')
        self.column_definitions = KeyedList([
            Column(self, cdoc)
            for cdoc in table_doc.get('column_definitions', [])
        ])
        self.keys = KeyedList([
            Key(self, kdoc)
            for kdoc in table_doc.get('keys', [])
        ])
        self.foreign_keys = KeyedList([
            ForeignKey(self, fkdoc)
            for fkdoc in table_doc.get('foreign_keys', [])
        ])
        self.referenced_by = KeyedList([])

    @property
    def columns(self):
        """Sugared access to self.column_definitions"""
        return self.column_definitions

    @property
    def catalog(self):
        return self.schema.model.catalog

    @property
    def uri_path(self):
        """URI to this model element."""
        return "%s/table/%s" % (self.schema.uri_path, urlquote(self.name))

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
            comment,
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
                tag.asset: {
                    'filename_column': 'Filename',
                    'byte_count_column': 'Length',
                    'md5': 'MD5',
                }
            }
            if hatrac_template:
                asset_annotation[tag.asset]['url_pattern'] = hatrac_template
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
        return {
            "schema_name": self.schema.name,
            "table_name": self.name,
            "acls": self.acls,
            "acl_bindings": self.acl_bindings,
            "annotations": self.annotations,
            "comment": self.comment,

            "column_definitions": [
                c.prejson()
                for c in self.column_definitions
            ],
            "keys": [
                key.prejson()
                for key in self.keys
            ],
            "foreign_keys": [
                fkey.prejson()
                for fkey in self.foreign_keys
            ]
        }

    def clear(self, clear_comment=False):
        """Clear all configuration in table and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.acls.clear()
        self.acl_bindings.clear()
        self.annotations.clear()
        if clear_comment:
            self.comment = None
        for col in self.column_definitions:
            col.clear(clear_comment=clear_comment)
        for key in self.keys:
            key.clear(clear_comment=clear_comment)
        for fkey in self.foreign_keys:
            fkey.clear(clear_comment=clear_comment)

    def apply(self, existing=None):
        """Apply configuration to corresponding table in catalog unless existing already matches.

        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The state of self.comment, self.annotations, self.acls, and
        self.acl_bindings will be applied to the server unless they
        match their corresponding state in existing.
        """
        changes = {}
        if existing is None or not equivalent(self.comment, existing.comment):
            changes['comment'] = self.comment
        if existing is None or not equivalent(self.annotations, existing.annotations):
            changes['annotations'] = self.annotations
        if existing is None or not equivalent(self.acls, existing.acls):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings):
            changes['acl_bindings'] = self.acl_bindings
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)
        for col in self.column_definitions:
            col.apply(existing.column_definitions[col.name] if existing else None)
        for key in self.keys:
            key.apply(existing.keys[key.name_in_model(existing.schema.model)] if existing else None)
        for fkey in self.foreign_keys:
            fkey.apply(existing.foreign_keys[fkey.name_in_model(existing.schema.model)] if existing else None)

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
            self.comment = changed['comment']

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

    def _own_column(self, column):
        if isinstance(column, Column):
            if self.column_definitions[column.name] is column:
                return column
            raise ValueError('column %s object is not from this table object' % (column,))
        elif column in self.column_definitions.elements:
            return self.column_definitions[column]
        raise ValueError('value %s does not name a defined column in this table' % (column,))

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

    def drop(self):
        """Remove this table from the remote database.
        """
        if self.name not in self.schema.tables:
            raise ValueError('Table %s does not appear to belong to schema %s.' % (self, self.schema))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.schema.tables[self.name]

    def key_by_columns(self, unique_columns, raise_nomatch=True):
        """Return key from self.keys with matching unique columns.

        unique_columns: iterable of column instances or column names
        raise_nomatch: for True, raise KeyError on non-match, else return None
        """
        cset = { self._own_column(c) for c in unique_columns }
        for key in self.keys:
            if cset == { c for c in key.unique_columns }:
                return key
        if raise_nomatch:
            raise KeyError(cset)

    def fkeys_by_columns(self, from_columns, partial=False, raise_nomatch=True):
        """Iterable of fkeys from self.foreign_keys with matching columns.

        from_columns: iterable of referencing column instances or column names
        partial: include fkeys which cover a superset of from_columns
        raise_nomatch: for True, raise KeyError on empty iterable
        """
        cset = { self._own_column(c) for c in from_columns }
        if not cset:
            raise ValueError('from_columns must be non-empty')
        to_table = None
        for fkey in self.foreign_keys:
            fkey_cset = set(fkey.foreign_key_columns)
            if cset == fkey_cset or partial and cset.issubset(fkey_cset):
                raise_nomatch = False
                yield fkey
        if raise_nomatch:
            raise KeyError(cset)

    def fkey_by_column_map(self, from_to_map, raise_nomatch=True):
        """Return fkey from self.foreign_keys with matching {referencing: referenced} column mapping.

        from_to_map: dict-like mapping with items() method yielding (from_col, to_col) pairs
        raise_nomatch: for True, raise KeyError on non-match, else return None
        """
        colmap = {
            self._own_column(from_col): to_col
            for from_col, to_col in from_to_map.items()
        }
        if not colmap:
            raise ValueError('column mapping must be non-empty')
        to_table = None
        for c in colmap.values():
            if to_table is None:
                to_table = c.table
            elif to_table is not c.table:
                raise ValueError('to-columns must all be part of same table')
        for fkey in self.foreign_keys:
            if colmap == fkey.column_map:
                return fkey
        if raise_nomatch:
            raise KeyError(from_to_map)

    def is_association(self, min_arity=2, max_arity=2, unqualified=True, pure=True, no_overlap=True):
        """Return (truthy) integer arity if self is a matching association, else False.

        min_arity: minimum number of associated fkeys (default 2)
        max_arity: maximum number of associated fkeys (default 2) or None
        unqualified: reject qualified associations when True (default True)
        pure: reject impure assocations when True (default True)
        no_overlap: reject overlapping associations when True (default True)

        The default behavior with no arguments is to test for pure,
        unqualified, non-overlapping, binary assocations.

        An association is comprised of several foreign keys which are
        covered by a non-nullable composite row key. This allows
        specific combinations of foreign keys to appear at most once.

        The arity of an association is the number of foreign keys
        being associated. A typical binary association has arity=2.

        An unqualified association contains *only* the foreign key
        material in its row key. Conversely, a qualified association
        mixes in other material which means that a specific
        combination of foreign keys may repeat with different
        qualifiers.

        A pure association contains *only* row key
        material. Conversely, an impure association includes
        additional metadata columns not covered by the row key. Unlike
        qualifiers, impure metadata merely decorates an association
        without augmenting its identifying characteristics.

        A non-overlapping association does not share any columns
        between multiple foreign keys. This means that all
        combinations of foreign keys are possible. Conversely, an
        overlapping association shares some columns between multiple
        foreign keys, potentially limiting the combinations which can
        be represented in an association row.

        These tests ignore the five ERMrest system columns and any
        corresponding constraints.

        """
        if min_arity < 2:
            raise ValueError('An assocation cannot have arity < 2')
        if max_arity is not None and max_arity < min_arity:
            raise ValueError('max_arity cannot be less than min_arity')

        # TODO: revisit whether there are any other cases we might
        # care about where system columns are involved?
        non_sys_cols = {
            col
            for col in self.column_definitions
            if col.name not in {'RID', 'RCT', 'RMT', 'RCB', 'RMB'}
        }
        non_sys_key_colsets = {
            frozenset(key.unique_columns)
            for key in self.keys
            if set(key.unique_columns).issubset(non_sys_cols)
            and len(key.unique_columns) > 1
        }

        if not non_sys_key_colsets:
            # reject: not association
            return False

        # choose longest compound key (arbitrary choice with ties!)
        row_key = sorted(non_sys_key_colsets, key=lambda s: len(s), reverse=True)[0]
        covered_fkeys = {
            fkey
            for fkey in self.foreign_keys
            if set(fkey.foreign_key_columns).issubset(row_key)
        }
        covered_fkey_cols = set()

        if len(covered_fkeys) < min_arity:
            # reject: not enough fkeys in association
            return False
        elif max_arity is not None and len(covered_fkeys) > max_arity:
            # reject: too many fkeys in association
            return False

        for fkey in covered_fkeys:
            fkcols = set(fkey.foreign_key_columns)
            if no_overlap and fkcols.intersection(covered_fkey_cols):
                # reject: overlapping fkeys in association
                return False
            covered_fkey_cols.update(fkcols)

        if unqualified and row_key.difference(covered_fkey_cols):
            # reject: qualified association
            return False

        if pure and non_sys_cols.difference(row_key):
            # reject: impure association
            return False

        # return (truthy) arity
        return len(covered_fkeys)

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @object_annotation(tag.display)
    def display(self): pass

    @object_annotation(tag.table_alternatives)
    def alternatives(self): pass

    @object_annotation(tag.table_display)
    def table_display(self): pass

    @object_annotation(tag.visible_columns)
    def visible_columns(self): pass

    @object_annotation(tag.visible_foreign_keys)
    def visible_foreign_keys(self): pass

class Column (object):
    """Named column.
    """
    def __init__(self, table, column_doc):
        self.table = table
        self.name = column_doc['name']
        self.acls = AttrDict(column_doc.get('acls', {}))
        self.acl_bindings = AttrDict(column_doc.get('acl_bindings', {}))
        self.annotations = dict(column_doc.get('annotations', {}))
        self.comment = column_doc.get('comment')
        self.type = make_type(column_doc['type'])
        self.nullok = bool(column_doc.get('nullok', True))
        self.default = column_doc.get('default')
        self.comment = column_doc.get('comment')

    @property
    def catalog(self):
        return self.table.schema.model.catalog

    @property
    def uri_path(self):
        """URI to this model resource."""
        return "%s/column/%s" % (self.table.uri_path, urlquote(self.name))

    def prejson_colref(self):
        return {
            "schema_name": self.table.schema.name,
            "table_name": self.table.name,
            "column_name": self.name,
        }

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        return {
            "name": self.name,
            "acls": self.acls,
            "acl_bindings": self.acl_bindings,
            "annotations": self.annotations,
            "comment": self.comment,
            "type": self.type.prejson(prune),
            "nullok": self.nullok,
            "default": self.default,
        }

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

    def clear(self, clear_comment=False):
        """Clear all configuration in column

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.acls.clear()
        self.acl_bindings.clear()
        self.annotations.clear()
        if clear_comment:
            self.comment = None

    def apply(self, existing=None):
        """Apply configuration to corresponding column in catalog unless existing already matches.

        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The state of self.comment, self.annotations, self.acls, and
        self.acl_bindings will be applied to the server unless they
        match their corresponding state in existing.
        """
        changes = {}
        if existing is None or not equivalent(self.comment, existing.comment):
            changes['comment'] = self.comment
        if existing is None or not equivalent(self.annotations, existing.annotations):
            changes['annotations'] = self.annotations
        if existing is None or not equivalent(self.acls, existing.acls):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings):
            changes['acl_bindings'] = self.acl_bindings
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)

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
        if type is not nochange:
            if not isinstance(type, Type):
                raise TypeError('Parameter "type" %s should be an instance of Type.' % (type,))
            type = type.prejson()

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
            self.comment = changed['comment']

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

    def drop(self):
        """Remove this column from the remote database.
        """
        if self.name not in self.table.column_definitions.elements:
            raise ValueError('Column %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.column_definitions[self.name]

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @object_annotation(tag.display)
    def display(self): pass

    @object_annotation(tag.asset)
    def asset(self): pass

    @object_annotation(tag.column_display)
    def column_display(self): pass

def _constraint_name_parts(constraint, doc):
    # modern systems should have 0 or 1 names here
    names = doc.get('names', [])[0:1]
    if not names:
        raise ValueError('Unexpected constraint without any name.')
    if names[0][0] == '':
        constraint_schema = None
    elif names[0][0] == constraint.table.schema.name:
        constraint_schema = constraint.table.schema
    else:
        raise ValueError('Unexpected schema name in constraint %s' % (names[0],))
    constraint_name = names[0][1]
    return (constraint_schema, constraint_name)

class Key (object):
    """Named key.
    """
    def __init__(self, table, key_doc):
        self.table = table
        self.annotations = dict(key_doc.get('annotations', {}))
        self.comment = key_doc.get('comment')
        try:
            self.constraint_schema, self.constraint_name = _constraint_name_parts(self, key_doc)
        except ValueError:
            self.constraint_schema, self.constraint_name = None, str(hash(self))
        self.unique_columns = KeyedList([
            table.column_definitions[cname]
            for cname in key_doc['unique_columns']
        ])

    @property
    def columns(self):
        """Sugared access to self.unique_columns"""
        return self.unique_columns

    @property
    def catalog(self):
        return self.table.schema.model.catalog

    @property
    def uri_path(self):
        """URI to this model resource."""
        return '%s/key/%s' % (
            self.table.uri_path,
            ','.join([ urlquote(c.name) for c in self.unique_columns ])
        )

    @property
    def name(self):
        """Constraint name (schemaobj, name_str) used in API dictionaries."""
        return (self.constraint_schema, self.constraint_name)

    def name_in_model(self, model):
        """Constraint name (schemaobj, name_str) used in API dictionaries fetching schema from model.

        While self.name works as a key within the same model tree,
        self.name_in_model(dstmodel) works in dstmodel tree by finding
        the equivalent schemaobj in that model via schema name lookup.

        """
        return (
            model.schemas[self.constraint_schema.name] if self.constraint_schema else None,
            self.constraint_name
        )

    @property
    def names(self):
        """Constraint names field as seen in JSON document."""
        return [ [self.constraint_schema.name if self.constraint_schema else '', self.constraint_name] ]

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        return {
            'annotations': self.annotations,
            'comment': self.comment,
            'unique_columns': [
                c.name
                for c in self.unique_columns
            ],
            'names': self.names,
        }

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

    def clear(self, clear_comment=False):
        """Clear all configuration in key

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.annotations.clear()
        if clear_comment:
            self.comment = None

    def apply(self, existing=None):
        """Apply configuration to corresponding table in catalog unless existing already matches.

        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The state of self.comment and self.annotations will be applied
        to the server unless they match their corresponding state in
        existing.
        """
        changes = {}
        if existing is None or not equivalent(self.comment, existing.comment):
            changes['comment'] = self.comment
        if existing is None or not equivalent(self.annotations, existing.annotations):
            changes['annotations'] = self.annotations
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)

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
            self.comment = changed['comment']

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def drop(self):
        """Remove this key from the remote database.
        """
        if self.name not in self.table.keys.elements:
            raise ValueError('Key %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.keys[self.name]

class ForeignKey (object):
    """Named foreign key.
    """
    def __init__(self, table, fkey_doc):
        self.table = table
        self.pk_table = None
        self.acls = AttrDict(fkey_doc.get('acls', {}))
        self.acl_bindings = AttrDict(fkey_doc.get('acl_bindings', {}))
        self.annotations = dict(fkey_doc.get('annotations', {}))
        self.comment = fkey_doc.get('comment')
        self.on_delete = fkey_doc.get('on_delete')
        self.on_update = fkey_doc.get('on_update')
        try:
            self.constraint_schema, self.constraint_name = _constraint_name_parts(self, fkey_doc)
        except ValueError:
            self.constraint_schema, self.constraint_name = None, str(hash(self))
        if self.constraint_schema:
            self.constraint_schema._fkeys[self.constraint_name] = self
        else:
            self.table.schema.model._pseudo_fkeys[self.constraint_name] = self
        self.foreign_key_columns = KeyedList([
            table.column_definitions[coldoc['column_name']]
            for coldoc in fkey_doc['foreign_key_columns']
        ])
        self._referenced_columns_doc = fkey_doc['referenced_columns']
        self.referenced_columns = None

    def digest_referenced_columns(self, model):
        """Finish construction deferred until model is known with all tables."""
        if self.referenced_columns is None:
            pk_sname = self._referenced_columns_doc[0]['schema_name']
            pk_tname = self._referenced_columns_doc[0]['table_name']
            self.pk_table = model.schemas[pk_sname].tables[pk_tname]
            self.referenced_columns = KeyedList([
                self.pk_table.column_definitions[coldoc['column_name']]
                for coldoc in self._referenced_columns_doc
            ])
            self._referenced_columns_doc = None
            self.pk_table.referenced_by.append(self)

    @property
    def column_map(self):
        """Mapping of foreign_key_columns elements to referenced_columns elements."""
        return {
            fk_col: pk_col
            for fk_col, pk_col in zip(self.foreign_key_columns, self.referenced_columns)
        }

    @property
    def columns(self):
        """Sugared access to self.column_definitions"""
        return self.foreign_key_columns

    @property
    def catalog(self):
        return self.table.schema.model.catalog

    @property
    def uri_path(self):
        """URI to this model resource."""
        return '%s/foreignkey/%s/reference/%s:%s/%s' % (
            self.table.uri_path,
            ','.join([ urlquote(c.name) for c in self.foreign_key_columns ]),
            urlquote(self.pk_table.schema.name),
            urlquote(self.pk_table.name),
            ','.join([ urlquote(c.name) for c in self.referenced_columns ]),
        )

    @property
    def name(self):
        """Constraint name (schemaobj, name_str) used in API dictionaries."""
        return (self.constraint_schema, self.constraint_name)

    def name_in_model(self, model):
        """Constraint name (schemaobj, name_str) used in API dictionaries fetching schema from model.

        While self.name works as a key within the same model tree,
        self.name_in_model(dstmodel) works in dstmodel tree by finding
        the equivalent schemaobj in that model via schema name lookup.

        """
        return (
            model.schemas[self.constraint_schema.name] if self.constraint_schema else None,
            self.constraint_name
        )

    @property
    def names(self):
        """Constraint names field as seen in JSON document."""
        return [ [self.constraint_schema.name if self.constraint_schema else '', self.constraint_name] ]

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        return {
            'acls': self.acls,
            'acl_bindings': self.acl_bindings,
            'annotations': self.annotations,
            'comment': self.comment,
            'foreign_key_columns': [
                c.prejson_colref()
                for c in self.foreign_key_columns
            ],
            'referenced_columns': [
                c.prejson_colref()
                for c in self.referenced_columns
            ],
            'names': self.names,
            'on_delete': self.on_delete,
            'on_update': self.on_update,
        }

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

    def clear(self, clear_comment=False):
        """Clear all configuration in foreign key

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        self.acls.clear()
        self.acl_bindings.clear()
        self.annotations.clear()
        if clear_comment:
            self.comment = None

    def apply(self, existing=None):
        """Apply configuration to corresponding table in catalog unless existing already matches.

        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The state of self.comment, self.annotations, self.acls, and
        self.acl_bindings will be applied to the server unless they
        match their corresponding state in existing.
        """
        changes = {}
        if existing is None or not equivalent(self.comment, existing.comment):
            changes['comment'] = self.comment
        if existing is None or not equivalent(self.annotations, existing.annotations):
            changes['annotations'] = self.annotations
        if existing is None or not equivalent(self.acls, existing.acls):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings):
            changes['acl_bindings'] = self.acl_bindings
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)

    def alter(
            self,
            constraint_name=nochange,
            on_update=nochange,
            on_delete=nochange,
            comment=nochange,
            acls=nochange,
            acl_bindings=nochange,
            annotations=nochange
    ):
        """Alter existing schema definition.

        :param constraint_name: Replacement constraint name string
        :param on_update: Replacement on-update action string
        :param on_delete: Replacement on-delete action string
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)

        Returns self (to allow for optional chained access).

        """
        changes = strip_nochange({
            'on_update': on_update,
            'on_delete': on_delete,
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

        if 'on_update' in changes:
            self.on_update = changed['on_update']

        if 'on_delete' in changes:
            self.on_delete = changed['on_delete']

        if 'comment' in changes:
            self.comment = changed['comment']

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

    def drop(self):
        """Remove this foreign key from the remote database.
        """
        if self.name not in self.table.foreign_keys.elements:
            raise ValueError('Foreign key %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.foreign_keys[self.name]
        del self.pk_table.referenced_by[self.name]

    @object_annotation(tag.foreign_key)
    def foreign_key(self): pass

def make_type(type_doc):
    """Create instance of Type, DomainType, or ArrayType as appropriate for type_doc."""
    if type_doc.get('is_domain', False):
        return DomainType(type_doc)
    elif type_doc.get('is_array', False):
        return ArrayType(type_doc)
    else:
        return Type(type_doc)

class Type (object):
    """Named type.
    """
    def __init__(self, type_doc):
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
    def __init__(self, type_doc):
        super(DomainType, self).__init__(type_doc)
        self.is_domain = True
        self.base_type = make_type(type_doc['base_type'])
        
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
    def __init__(self, type_doc):
        super(ArrayType, self).__init__(type_doc)
        is_array = True
        self.base_type = make_type(type_doc['base_type'])

    def prejson(self, prune=True):
        d = super(ArrayType, self).prejson(prune)
        d.update({
            'is_array': True,
            'base_type': self.base_type.prejson(prune)
        })
        return d

builtin_types = AttrDict(
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
                'color_rgb_hex': 'text',
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
