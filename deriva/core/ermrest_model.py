
from __future__ import annotations

import base64
import hashlib
import json
import re
from collections import OrderedDict
from collections.abc import Iterable
from enum import Enum

from . import AttrDict, tag, urlquote, stob, mmo


class NoChange (object):
    """Special class used to distinguish no-change default arguments to methods.

       Values for no-change are distinct from all valid values for
    these arguments.

    """
    pass

# singletone to use in APIs below
nochange = NoChange()

class UpdateMappings (str, Enum):
    """Update Mappings flag enum"""
    no_update = ''
    deferred = 'deferred'
    immediate = 'immediate'

def make_id(*components):
    """Build an identifier that will be OK for ERMrest and Postgres.

    Naively, append as '_'.join(components).

    Fallback to heuristics mixing truncation with short hashes.
    """
    # accept lists at top-level for convenience (compound keys, etc.)
    expanded = []
    for e in components:
        if isinstance(e, list):
            expanded.extend(e)
        else:
            expanded.append(e)

    # prefer to use naive name as requested
    naive_result = '_'.join(expanded)
    naive_len = len(naive_result.encode('utf8'))
    if naive_len <= 63:
        return naive_result

    # we'll need to truncate and hash in some way...
    def hash(s, nbytes):
        return base64.urlsafe_b64encode(hashlib.md5(s.encode('utf8')).digest()).decode()[0:nbytes]

    def truncate(s, maxlen):
        encoded_len = len(s.encode('utf8'))
        # we need to chop whole (unicode) chars but test encoded byte lengths!
        for i in range(max(1, len(s) - maxlen), len(s) - 1):
            result = s[0:-1 * i].rstrip()
            if len(result.encode('utf8')) <= (maxlen - 2):
                return result + '..'
        return s

    naive_hash = hash(naive_result, 5)
    parts = [
        (i, expanded[i])
        for i in range(len(expanded))
    ]

    # try to find a solution truncating individual fields
    for maxlen in [15, 12, 9]:
        parts.sort(key=lambda p: (len(p[1].encode('utf8')), p[0]), reverse=True)
        for i in range(len(parts)):
            idx, part = parts[i]
            if len(part.encode('utf8')) > maxlen:
                parts[i] = (idx, truncate(part, maxlen))
            candidate_result = '_'.join([
                p[1]
                for p in sorted(parts, key=lambda p: p[0])
            ] + [naive_hash])
            if len(candidate_result.encode('utf8')) < 63:
                return candidate_result

    # fallback to truncating original naive name
    # try to preserve suffix and trim in middle
    result = ''.join([
        truncate(naive_result, len(naive_result)//3),
        naive_result[-len(naive_result)//3:],
        '_',
        naive_hash
    ])
    if len(result.encode('utf8')) <= 63:
        return result

    # last-ditch (e.g. multibyte unicode suffix worst case)
    return truncate(naive_result, 55) + naive_hash

def sql_identifier(s):
    # double " to protect from SQL
    return '"%s"' % (s.replace('"', '""'))

def sql_literal(v):
    if v is None:
        return 'NULL'
    if type(v) is list:
        s = json.dumps(v)
    # double ' to protect from SQL
    s = '%s' % v
    return "'%s'" % (s.replace("'", "''"))

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
    # method is used to fill in defaults to avoid some false negatives
    if method == 'acls':
        if not isinstance(doc1, dict):
            return False
        def canon_acls(d):
            return {
                k: sorted(v)
                for k, v in d.items()
            }
        return equivalent(canon_acls(doc1), canon_acls(doc2))
    if method == 'catalog_acls':
        if not isinstance(doc1, dict):
            return False
        def canon_cat_acls(d):
            return {
                k: d.get(k, [])
                for k in {'owner', 'read', 'write', 'insert', 'update', 'delete'}
            }
        return equivalent(canon_cat_acls(doc1), canon_cat_acls(doc2), method='acls')
    elif method == 'foreign_key_acls':
        return equivalent(doc1, doc2, method='acls')
    elif method == 'acl_bindings':
        if not isinstance(doc1, dict):
            return False
        def canon_acl_bindings(d):
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
        return equivalent(canon_acl_bindings(doc1), canon_acl_bindings(doc2))
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

    def clear(self, clear_comment=False, clear_annotations=True, clear_acls=True, clear_acl_bindings=True):
        """Clear all configuration in catalog and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_annotations:
            self.annotations.clear()
        if clear_acls:
            self.acls.clear()
        for schema in self.schemas.values():
            schema.clear(clear_comment=clear_comment, clear_annotations=clear_annotations, clear_acls=clear_acls, clear_acl_bindings=clear_acl_bindings)

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
        if not equivalent(self.acls, existing.acls, method='catalog_acls'):
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

    @object_annotation(tag.chaise_config)
    def chaise_config(self): pass

    @object_annotation(tag.column_defaults)
    def column_defaults(self): pass

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @presence_annotation(tag.non_deletable)
    def non_deletable(self): pass

    @object_annotation(tag.export_2019)
    def export_2019(self): pass

    @object_annotation(tag.export_fragment_definitions)
    def export_fragment_definitions(self): pass

    def configure_baseline_ermrest_client(self, apply=True):
        """Baseline configuration of `ERMrest_Client` table.

        Set up `ERMrest_Client` table so that it has readable names and uses the display name of the user as the row
        name.

        :param apply: if true, apply configuration changes before returning.
        """
        ermrest_client = self.schemas['public'].tables['ERMrest_Client']

        # Set table and row name.
        ermrest_client.annotations.update({
            tag.display: {'name': 'Users'},
            tag.visible_columns: {'compact': ['Full_Name', 'Display_Name', 'Email', 'ID']},
            tag.table_display: {'row_name': {'row_markdown_pattern': '{{{Full_Name}}}'}}
        })

        column_annotations = {
            'RCT': {tag.display: {'name': 'Creation Time'}},
            'RMT': {tag.display: {'name': 'Modified Time'}},
            'RCB': {tag.display: {'name': 'Created By'}},
            'RMB': {tag.display: {'name': 'Modified By'}}
        }
        for k, v in column_annotations.items():
            ermrest_client.columns[k].annotations.update(v)

        if apply:
            # Apply model changes
            self.apply()

    def configure_baseline_ermrest_group(self, apply=True):
        """Baseline configuration of `ERMrest_Group` table.

        Set up `ERMrest_Group` table so that it has readable names and uses the display name of the group as the row
        name.

        :param apply: if true, apply configuration changes before returning.
        """
        ermrest_group = self.schemas['public'].tables['ERMrest_Group']

        # Set table and row name.
        ermrest_group.annotations.update({
            tag.display: {'name': 'Groups'},
            tag.visible_columns: {'compact': ['Display_Name', 'ID']},
            tag.table_display: {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}
        })

        column_annotations = {
            'RCT': {tag.display: {'name': 'Creation Time'}},
            'RMT': {tag.display: {'name': 'Modified Time'}},
            'RCB': {tag.display: {'name': 'Created By'}},
            'RMB': {tag.display: {'name': 'Modified By'}}
        }
        for k, v in column_annotations.items():
            ermrest_group.columns[k].annotations.update(v)

        if apply:
            # Apply model changes
            self.apply()

    def configure_baseline_catalog(self, apply=True, **kwargs):
        """A baseline catalog configuration.

        Update catalog to a baseline configuration:
        1. Set default display mode to turn underscores to spaces in model element names.
        2. Configure `ERMrest_Client` and `ERMrest_Group` to have readable names.
        3. Create a schema `WWW` with `Page` and `File` tables in that schema configured to display web-page like
           content.
        4. Configure a basic navbar with links to all tables.

        Afterwards, an ACL configuration should be applied to the catalog. See the `deriva.config.examples` package
        data for configuration templates.

        :param apply: if true, apply configuration changes before returning.
        :param kwargs: a set of name-value pairs used to override default settings.
        """
        # Configure baseline public schema
        if kwargs.get("publicSchemaDisplayName"):
            public_schema = self.schemas['public']
            public_schema.annotations.update({
                tag.display: {'name': 'User Info'}
            })
        self.configure_baseline_ermrest_client(apply=False)
        self.configure_baseline_ermrest_group(apply=False)

        # Create WWW schema
        if stob(kwargs.get("includeWWWSchema", True)):
            www_name = kwargs.get("wwwSchemaName", "WWW")
            if www_name not in self.schemas:
                self.create_schema(Schema.define_www(www_name))

        # Configure baseline annotations
        self.annotations.update({
            # Set up catalog-wide name style
            tag.display: {'name_style': {'underline_space': True}},
            # Set up default chaise config
            tag.chaise_config: {
                "headTitle": kwargs.get("headTitle", "DERIVA"),
                "navbarBrandText": kwargs.get("navbarBrandText", "DERIVA"),
                "navbarMenu": {
                    "newTab": False,
                    "children": [
                        {
                            "name": s.annotations.get(tag.display, {}).get('name', s.name.replace('_', ' ')),
                            "children": [
                                {
                                    "name": t.annotations.get(tag.display, {}).get('name', t.name.replace('_', ' ')),
                                    "url": f'/chaise/recordset/#{self.catalog.catalog_id}/{urlquote(s.name)}:{urlquote(t.name)}'
                                } for t in s.tables.values() if not t.is_association()
                            ]
                        } for s in self.schemas.values()
                    ]

                },
                "systemColumnsDisplayCompact": ["RID"],
                "systemColumnsDisplayEntry": ["RID"]
            }
        })

        if apply:
            # Apply model changes
            self.apply()

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

    def __repr__(self):
        cls = type(self)
        return "<%s.%s object %r at 0x%x>" % (
            cls.__module__,
            cls.__name__,
            self.name,
            id(self),
        )

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

    @classmethod
    def define_www(cls, sname, comment=None, acls={}, annotations={}):
        """Build a schema definition for wiki-like web content.

        Defines a schema with a "Page" wiki-like page table definition and a
        "File" asset table definition for attachments to the wiki pages.

        :param sname: schema name
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param annotations: a dictionary of annotations
        """
        www_schema = Schema.define(
            sname,
            comment=comment if comment is not None else "Schema for tables that will be displayed as web content",
            acls=acls,
            annotations=annotations
        )
        www_schema["tables"] = {
            "Page": Table.define_page("Page"),
            "File": Table.define_asset(
                sname,
                "File",
                column_defs=[
                    Column.define("Page", builtin_types.text, nullok=False, comment="Parent page of this asset")
                ],
                fkey_defs=[
                    ForeignKey.define(["Page"], sname, "Page", ["RID"])
                ]
            )
        }
        return www_schema

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

    def clear(self, clear_comment=False, clear_annotations=True, clear_acls=True, clear_acl_bindings=True):
        """Clear all configuration in schema and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_annotations:
            self.annotations.clear()
        if clear_acls:
            self.acls.clear()
        if clear_comment:
            self.comment = None
        for table in self.tables.values():
            table.clear(clear_comment=clear_comment, clear_annotations=clear_annotations, clear_acls=clear_acls, clear_acl_bindings=clear_acl_bindings)

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
        if existing is None or not equivalent(self.acls, existing.acls, method='acls'):
            changes['acls'] = self.acls
        if changes:
            # use alter method to reduce number of web requests
            self.alter(**changes)
        for tname, table in self.tables.items():
            table.apply(existing.tables[tname] if existing else None)

    def alter(self, schema_name=nochange, comment=nochange, acls=nochange, annotations=nochange, update_mappings=UpdateMappings.no_update):
        """Alter existing schema definition.

        :param schema_name: Replacement schema name (default nochange)
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param annotations: Replacement annotations (default nochange)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)

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
            old_schema_name = self.name
            del self.model.schemas[self.name]
            self.name = changed['schema_name']
            self.model.schemas[self.name] = self
            if update_mappings:
                mmo.replace(self.model, [old_schema_name, None], [self.name, None])
                if update_mappings == UpdateMappings.immediate:
                    self.model.apply()

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

    def drop(self, cascade=False, update_mappings=UpdateMappings.no_update):
        """Remove this schema from the remote database.

        :param cascade: drop dependent objects.
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)
        """
        if self.name not in self.model.schemas:
            raise ValueError('Schema %s does not appear to belong to model.' % (self,))

        if cascade:
            for table in list(self.tables.values()):
                table.drop(cascade=True, update_mappings=update_mappings)

        self.catalog.delete(self.uri_path).raise_for_status()
        del self.model.schemas[self.name]
        for table in self.tables.values():
            for fkey in table.foreign_keys:
                fkey._cleanup()

    @object_annotation(tag.display)
    def display(self): pass

    @object_annotation(tag.column_defaults)
    def column_defaults(self): pass    

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @presence_annotation(tag.non_deletable)
    def non_deletable(self): pass

    @object_annotation(tag.export_2019)
    def export_2019(self): pass

    @object_annotation(tag.export_fragment_definitions)
    def export_fragment_definitions(self): pass
    

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

class FindAssociationResult (object):
    """Wrapper for results of Table.find_associations()"""
    def __init__(self, table, self_fkey, other_fkeys):
        self.table = table
        self.name = table.name
        self.schema = table.schema
        self.self_fkey = self_fkey
        self.other_fkeys = other_fkeys
    
class Table (object):
    """Named table.
    """
    default_key_column_search_order = ["Name", "name", "ID", "id"]

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

    def __repr__(self):
        cls = type(self)
        return "<%s.%s object %r.%r at 0x%x>" % (
            cls.__module__,
            cls.__name__,
            self.schema.name if self.schema is not None else None,
            self.name,
            id(self),
        )

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
            Column.define(cname, builtin_types[ctype], nullok, annotations=annotations)
            for cname, ctype, nullok, annotations in [
                    ('RID', 'ermrest_rid', False, {tag.display: {'name': 'Record ID'}}),
                    ('RCT', 'ermrest_rct', False, {tag.display: {'name': 'Creation Time'}}),
                    ('RMT', 'ermrest_rmt', False, {tag.display: {'name': 'Modified Time'}}),
                    ('RCB', 'ermrest_rcb', True, {tag.display: {'name': 'Created By'}}),
                    ('RMB', 'ermrest_rmb', True, {tag.display: {'name': 'Modified By'}}),
            ]
            if cname not in { c['name']: c for c in custom }
        ] + custom

    @classmethod
    def system_key_defs(cls, custom=[]):
        """Build standard system key definitions, merging optional custom definitions."""
        def ktup(k):
            return frozenset(k['unique_columns'])
        customized = { ktup(kdef): kdef for kdef in custom }
        return [
            kdef for kdef in [
                Key.define(['RID'])
            ]
            if ktup(kdef) not in customized
        ] + custom

    @classmethod
    def system_fkey_defs(cls, tname, custom=[]):
        """Build standard system fkey definitions, merging optional custom definitions."""
        def fktup(fk):
            return (
                fk["referenced_columns"][0]["schema_name"],
                fk["referenced_columns"][0]["table_name"],
                frozenset(zip(
                    tuple( c["column_name"] for c in fk["foreign_key_columns"] ),
                    tuple( c["column_name"] for c in fk["referenced_columns"] ),
                ))
            )
        customized = { fktup(fkdef) for fkdef in custom }
        return [
            fkdef for fkdef in [
                ForeignKey.define(
                    ["RCB"], "public", "ERMrest_Client", ["ID"],
                    constraint_name=make_id(tname, "RCB", "fkey"),
                ),
                ForeignKey.define(
                    ["RMB"], "public", "ERMrest_Client", ["ID"],
                    constraint_name=make_id(tname, "RMB", "fkey"),
                ),
            ]
            if fktup(fkdef) not in customized
        ] + custom
    
    @classmethod
    def _expand_references(
        cls,
        table_name: str,
        column_defs: list[Key | Table | dict | tuple[str, bool, Key | Table] | tuple[str, Key | Table]],
        fkey_defs: Iterable[dict],
        used_names: set[str] =set(),
        key_column_search_order: Iterable[str] | None = None,
    ):
        """Expand implicit references in column_defs into actual column and fkey definitions.

        :param table_name: Name of table, needed to build fkey constraint names
        :param column_defs: List of column definitions and/or reference targets (see below)
        :param fkey_defs: List of foreign key definitions
        :param used_names: Set of reference base names to consider already in use

        Each reference target may be one of:
           - Key
           - Table
           - tuple (str, Key|Table)
           - tuple (str, bool, Key|Table)

        A target Key specifies the columns of the remote table to
        reference. A target Table instance specifies the remote table
        and relies on heuristics to choose the target Key. A target
        tuple specifies a string "base name" and optionally a boolean
        "nullok" for the implied referencing columns. When omitted, a
        default base name is constructed from target table
        information, and a default nullok=False is specified for
        referencing columns. The key_column_search_order parameter
        influences the heuristic for selecting a target Key for an
        input target Table.

        This method mutates the input column_defs and used_names
        containers. This can be abused to chain state through a
        sequence of calls.

        Returns the column_defs and fkey_defs which includes input
        definitions and implied definitions while removing the corresponding input
        reference targets.

        """
        out_column_defs = []
        out_fkey_defs = list(fkey_defs)

        # materialize for mutation and replay
        column_defs = list(column_defs)

        if key_column_search_order is not None:
            # materialize iterable for reuse
            key_column_search_order = list(key_column_search_order)
        else:
            key_column_search_order = cls.default_key_column_search_order

        def check_basename(basename):
            if not isinstance(base_name, str):
                raise TypeError('Base name %r is not of required type str' % (base_name,))
            if base_name in used_names:
                raise ValueError('Base name %r is not unique among inputs' % (base_name,))
            used_names.add(base_name)

        def choose_basename(key):
            base_name = key.table.name
            n = 2
            while base_name in used_names or any(used.startswith(base_name) for used in used_names):
                base_name = '%s%d' % (key.table.name, n)
                n += 1
            used_names.add(base_name)
            return base_name

        def check_key(key):
            if isinstance(key, Table):
                # opportunistic case: prefer (non-nullable) "name" or "id" keys, if found
                for cname in key_column_search_order:
                    try:
                        candidate = key.key_by_columns([cname])
                        if not candidate.unique_columns[0].nullok:
                            return candidate
                    except (KeyError, ValueError) as e:
                        continue

                # general case: try to use RID key
                try:
                    return key.key_by_columns(["RID"])
                except (KeyError, ValueError) as e:
                    raise ValueError('Could not determine default key for table %s' % (key,))
            elif isinstance(key, Key):
                return key
            raise TypeError('Expected Key or Table instance as target reference, not %s' % (key,))

        # check and normalize cdefs into list[(str, Key)] with distinct base names
        for i in range(len(column_defs)):
            if isinstance(column_defs[i], tuple):
                if len(column_defs[i]) == 2:
                    base_name, key = column_defs[i]
                    nullok = False
                elif len(column_defs[i]) == 3:
                    base_name, nullok, key = column_defs[i]
                else:
                    raise ValueError('Expected column definition tuple (str, Key|Table) or (str, bool, Key|Table), not %s' % (len(column_defs[i]),))
                check_basename(base_name)
                key = check_key(key)
                column_defs[i] = (base_name, nullok, key)
            elif isinstance(column_defs[i], (Key, Table)):
                key = check_key(column_defs[i])
                base_name = choose_basename(key)
                column_defs[i] = (base_name, False, key)
            elif isinstance(column_defs[i], dict):
                pass
            else:
                raise TypeError('Expected column definition dict, Key, or Table input, not %s' % (column_defs[i],))

        def simplify_type(ctype):
            if ctype.is_domain and ctype.typename.startswith('ermrest_'):
                return ctype.base_type
            return ctype

        def cdefs_for_key(base_name, nullok, key):
            return [
                Column.define(
                    '%s_%s' % (base_name, col.name) if len(key.unique_columns) > 1 else base_name,
                    simplify_type(col.type),
                    nullok=nullok,
                )
                for col in key.unique_columns
            ]

        def fkdef_for_key(base_name, nullok, key):
            return ForeignKey.define(
                [
                    '%s_%s' % (base_name, col.name) if len(key.unique_columns) > 1 else base_name
                    for col in key.unique_columns
                ],
                key.table.schema.name,
                key.table.name,
                [ col.name for col in key.unique_columns ],
                on_update='CASCADE',
                on_delete='CASCADE',
                constraint_name=make_id(table_name, base_name, 'fkey'),
            )

        for cdef in column_defs:
            if isinstance(cdef, tuple):
                out_column_defs.extend(cdefs_for_key(*cdef))
                out_fkey_defs.append(fkdef_for_key(*cdef))
            else:
                out_column_defs.append(cdef)

        return out_column_defs, out_fkey_defs

    @classmethod
    def define(
        cls,
        tname: str,
        column_defs: Iterable[dict | Key | Table | tuple[str, bool, Key | Table] | tuple[str, Key | Table]] = [],
        key_defs: Iterable[dict] = [],
        fkey_defs: Iterable[dict] = [],
        comment: str | None = None,
        acls: dict = {},
        acl_bindings: dict = {},
        annotations: dict = {},
        provide_system: bool = True,
        provide_system_fkeys: book = True,
        key_column_search_order: Iterable[str] | None = None,
    ):
        """Build a table definition.

        :param tname: the name of the newly defined table
        :param column_defs: a list of custom Column.define() results and/or reference targets (see below)
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param provide_system: whether to inject standard system column definitions when missing from column_defs
        :param provide_system_fkeys: whether to also inject foreign key definitions for RCB/RMB
        :param key_column_search_order: override heuristic for choosing a Key from a Table input

        Each reference target may be one of:
           - Key
           - Table
           - tuple (str, Key|Table)
           - tuple (str, bool, Key|Table)

        A target Key specifies the columns of the remote table to
        reference. A target Table instance specifies the remote table
        and relies on heuristics to choose the target Key. A target
        tuple specifies a string "base name" and optionally a boolean
        "nullok" for the implied referencing columns. When omitted, a
        default base name is constructed from target table
        information, and a default nullok=False is specified for
        referencing columns. The key_column_search_order parameter
        influences the heuristic for selecting a target Key for an
        input target Table.

        """
        column_defs = list(column_defs) # materialize to allow replay
        used_names = { cdef["name"] for cdef in column_defs if isinstance(cdef, dict) and 'name' in cdef }
        column_defs, fkey_defs = cls._expand_references(tname, column_defs, fkey_defs, used_names, key_column_search_order)

        if provide_system:
            column_defs = cls.system_column_defs(column_defs)
            key_defs = cls.system_key_defs(key_defs)
            if provide_system_fkeys:
                fkey_defs = cls.system_fkey_defs(tname, fkey_defs)
        else:
            key_defs = list(key_defs)

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
    def define_vocabulary(
        cls,
        tname: str,
        curie_template: str,
        uri_template: str = '/id/{RID}',
        column_defs: Iterable[dict | Key | Table | tuple[str, bool, Key | Table] | tuple[str, Key | Table]] = [],
        key_defs=[],
        fkey_defs=[],
        comment: str | None = None,
        acls: dict = {},
        acl_bindings: dict = {},
        annotations: dict = {},
        provide_system: bool = True,
        provide_system_fkeys: bool = True,
        provide_name_key: bool = True,
        key_column_search_order: Iterable[str] | None = None,
    ):
        """Build a vocabulary table definition.

        :param tname: the name of the newly defined table
        :param curie_template: the RID-based template for the CURIE of locally-defined terms, e.g. 'MYPROJECT:{RID}'
        :param uri_template: the RID-based template for the URI of locally-defined terms, e.g. 'https://server.example.org/id/{RID}'
        :param column_defs: a list of Column.define() results and/or reference targets (see below)
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param provide_system: whether to inject standard system column definitions when missing from column_defs
        :param provide_system_fkeys: whether to also inject foreign key definitions for RCB/RMB
        :param provide_name_key: whether to inject a key definition for the Name column
        :param key_column_search_order: override heuristic for choosing a Key from a Table input

        These core vocabulary columns are generated automatically if
        absent from the input column_defs.

        - ID: ermrest_curie, unique not null, default curie template "%s:{RID}" % curie_prefix
        - URI: ermrest_uri, unique not null, default URI template "/id/{RID}"
        - Name: text, unique not null
        - Description: markdown, not null
        - Synonyms: text[]

        However, caller-supplied definitions override the default. See
        Table.define() documentation for an explanation reference
        targets in the column_defs list and the related
        key_column_search_order parameter.

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
                return frozenset(k['unique_columns'])
            return [
                key_def
                for key_def in [
                        Key.define(['ID']),
                        Key.define(['URI']),
                ] + ([ Key.define(['Name']) ] if provide_name_key else [])
                if ktup(key_def) not in { ktup(kdef): kdef for kdef in custom }
            ] + custom

        used_names = {'ID', 'URI', 'Name', 'Description', 'Synonyms'}
        column_defs, fkey_defs = cls._expand_references(tname, column_defs, fkey_defs, used_names, key_column_search_order)
        column_defs = add_vocab_columns(column_defs)
        key_defs = add_vocab_keys(key_defs)

        return cls.define(
            tname,
            column_defs,
            key_defs,
            fkey_defs,
            comment,
            acls,
            acl_bindings,
            annotations,
            provide_system=provide_system,
            provide_system_fkeys=provide_system_fkeys,
            key_column_search_order=key_column_search_order,
        )

    @classmethod
    def define_asset(
        cls,
        sname: str,
        tname: str,
        hatrac_template=None,
        column_defs: Iterable[dict | Key | Table | tuple[str, bool, Key | Table] | tuple[str, Key | Table]] = [],
        key_defs=[],
        fkey_defs=[],
        comment: str | None = None,
        acls: dict = {},
        acl_bindings: dict = {},
        annotations: dict = {},
        provide_system: bool = True,
        provide_system_fkeys: bool = True,
        key_column_search_order: Iterable[str] | None = None,
    ):
        """Build an asset  table definition.

          :param sname: the name of the schema for the asset table
          :param tname: the name of the newly defined table
          :param hatrac_template: template for the hatrac URL.  Will undergo substitution to template can include
                 elmenents such at {{{MD5}}} or {{{Filename}}}. The default template puts files in
                     /hatrac/schema_name/table_name/md5.filename
                 where the filename and md5 value is computed on upload and the schema_name and table_name are the
                 values of the provided arguments.  If value is set to False, no hatrac_template is used.
          :param column_defs: a list of Column.define() results and/or reference targets (see below)
          :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
          :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
          :param comment: a comment string for the table
          :param acls: a dictionary of ACLs for specific access modes
          :param acl_bindings: a dictionary of dynamic ACL bindings
          :param annotations: a dictionary of annotations
          :param provide_system: whether to inject standard system column definitions when missing from column_defs
          :param provide_system_fkeys: whether to also inject foreign key definitions for RCB/RMB
          :param key_column_search_order: override heuristic for choosing a Key from a Table input

          These core asset table columns are generated automatically if
          absent from the input column_defs.

          - Filename: ermrest_curie, unique not null, default curie template "%s:{RID}" % curie_prefix
          - URL: Location of the asset, unique not null.  Default template is:
                    /hatrac/cat_id/sname/tname/{{{MD5}}}.{{{Filename}}} where tname is the name of the asset table.
          - Length: Length of the asset.
          - MD5: text
          - Description: markdown, not null

          However, caller-supplied definitions override the
          default. See Table.define() documentation for an explanation
          reference targets in the column_defs list and the related
          key_column_search_order parameter.

          In addition to creating the columns, this function also creates an asset annotation on the URL column to
          facilitate use of the table by Chaise.

        """

        if hatrac_template is None:
            hatrac_template = '/hatrac/{{$catalog.id}}/%s/%s/{{{MD5}}}.{{#encode}}{{{Filename}}}{{/encode}}' % (sname, tname)

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
                return frozenset(k['unique_columns'])

            return [
                key_def
                for key_def in [
                    Key.define(['URL']),
                ]
                if ktup(key_def) not in {ktup(kdef): kdef for kdef in custom}
            ] + custom

        def add_asset_annotations(custom):
            asset_annotations = {
                tag.table_display: {
                    'row_name': {
                        'row_markdown_pattern': '{{{Filename}}}'
                    }
                }
            }
            asset_annotations.update(custom)
            return asset_annotations

        used_names = {'URL', 'Filename', 'Description', 'Length', 'MD5'}
        column_defs, fkey_defs = cls._expand_references(tname, column_defs, fkey_defs, used_names, key_column_search_order)

        return cls.define(
            tname,
            add_asset_columns(column_defs),
            add_asset_keys(key_defs),
            fkey_defs,
            comment if comment is not None else 'Asset table.',
            acls,
            acl_bindings,
            add_asset_annotations(annotations),
            provide_system=provide_system,
            provide_system_fkeys=provide_system_fkeys,
            key_column_search_order=key_column_search_order,
        )

    @classmethod
    def define_page(
        cls,
        tname,
        column_defs: Iterable[dict | Key | Table | tuple[str, bool, Key | Table] | tuple[str, Key | Table]] = [],
        key_defs=[],
        fkey_defs=[],
        comment: str | None = None,
        acls: dict = {},
        acl_bindings: dict = {},
        annotations: dict = {},
        provide_system: bool = True,
        provide_system_fkeys: bool = True,
        key_column_search_order: Iterable[str] | None = None,
    ):
        """Build a wiki-like "page" table definition.

        :param tname: the name of the newly defined table
        :param column_defs: a list of Column.define() results for extra or overridden column definitions
        :param key_defs: a list of Key.define() results and/or reference targets (see below)
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param provide_system: whether to inject standard system column definitions when missing from column_defs
        :param provide_system_fkeys: whether to also inject foreign key definitions for RCB/RMB
        :param key_column_search_order: override heuristic for choosing a Key from a Table input

        These core page columns are generated automatically if absent from the input column_defs.

        - Title: text, unique not null
        - Content: markdown

        However, caller-supplied definitions override the default. See
        Table.define() documentation for an explanation reference
        targets in the column_defs list and the related
        key_column_search_order parameter.
        """

        def add_page_columns(custom):
            return [
                col_def
                for col_def in [
                        Column.define(
                            'Title',
                            builtin_types['text'],
                            nullok=False,
                            comment='Unique title for the page.'
                        ),
                        Column.define(
                            'Content',
                            builtin_types['markdown'],
                            nullok=True,
                            comment='Content of the page in markdown.'
                        ),
                ]
                if col_def['name'] not in { c['name']: c for c in custom }
            ] + custom

        def add_page_keys(custom):
            def ktup(k):
                return frozenset(k['unique_columns'])
            return [
                key_def
                for key_def in [
                        Key.define(['Title'])
                ]
                if ktup(key_def) not in { ktup(kdef): kdef for kdef in custom }
            ] + custom

        def add_page_annotations(custom):
            page_annotations = {
                tag.table_display: {
                    'row_name': {
                        'row_markdown_pattern': '{{{Title}}}'
                    },
                    'detailed': {
                        'hide_column_headers': True,
                        'collapse_toc_panel': True
                    }
                },
                tag.visible_columns: {
                    'compact': ['Title'],
                    'detailed': ['Content'],
                    'entry': ['Title', 'Content'],
                    'filter': {'and': []}
                }
            }
            page_annotations.update(annotations)
            return page_annotations

        used_names = {'Title', 'Content'}
        column_defs, fkey_defs = cls._expand_references(tname, column_defs, fkey_defs, used_names, key_column_search_order)

        return cls.define(
            tname,
            add_page_columns(column_defs),
            add_page_keys(key_defs),
            fkey_defs,
            comment,
            acls,
            acl_bindings,
            add_page_annotations(annotations),
            provide_system=provide_system,
            provide_system_fkeys=provide_system_fkeys,
            key_column_search_order=key_column_search_order,
        )

    @classmethod
    def define_association(
        cls,
        associates: Iterable[Key | Table | tuple[str, Key | Table]],
        metadata: Iterable[Key | Table | dict | tuple[str, bool, Key | Table]] = [],
        table_name: str | None = None,
        comment: str | None = None,
        provide_system: bool = True,
        provide_system_fkeys: bool = True,
        key_column_search_order: Iterable[str] | None = None,
    ) -> dict:
        """Build an association table definition.

        :param associates: reference targets being associated (see below)
        :param metadata: additional metadata fields and/or reference targets for impure associations
        :param table_name: name for the association table or None for default naming
        :param comment: comment for the association table or None for default comment
        :param provide_system: add ERMrest system columns when True
        :param provide_system_fkeys: whether to also inject foreign key definitions for RCB/RMB
        :param key_column_search_order: override heuristic for choosing a Key from a Table input

        This is a utility function to help build an association table
        definition. It simplifies the task, but removes some
        control. For full customization, consider using Table.define()
        directly instead.

        A normal ("pure") N-ary association is a table with N foreign
        keys referencing N primary keys in referenced tables, with a
        composite primary key covering the N foreign keys. These pure
        association tables manage a set of distinct combinations of
        the associated foreign key values.

        An "impure" association table adds additional metadata
        alongside the N foreign keys.

        The "associates" parameter takes an iterable of reference
        targets.  The association will be comprised of foreign keys
        referencing these associates. This includes columns to store
        the associated foreign key values, foreign key constraints to
        the associated tables, and a composite key constraint
        covering all the associated foreign key values.

        The "metadata" parameter takes an iterable Column.define()
        results and/or reference targets. The association table will
        be augmented with extra metadata columns and foreign keys as
        directed by these inputs.

        See the Table.define() method documentation for more on
        reference targets. Association columns must be defined with
        nullok=False, so the associates parameter is restricted to a
        more limited form of reference target input without
        caller-controlled nullok boolean values.

        """
        associates = list(associates)
        metadata = list(metadata)

        if key_column_search_order is not None:
            # materialize iterable for reuse
            key_column_search_order = list(key_column_search_order)
        else:
            key_column_search_order = cls.default_key_column_search_order
                
        if len(associates) < 2:
            raise ValueError('An association table requires at least 2 associates')

        used_names = set()

        for assoc in associates:
            if not isinstance(assoc, (tuple, Table, Key)):
                raise TypeError("Associates must be Table or Key instances, not %s" % (assoc,))

        # first pass: build "pure" association table parts
        # HACK: use dummy table name if we don't have one yet
        tname = table_name if table_name is not None else "dummy"
        cdefs, fkdefs = cls._expand_references(tname, associates, [], used_names)

        if table_name is None:
            # use first pass results to build table_name
            def get_assoc_name(assoc):
                if isinstance(assoc, tuple):
                    table = assoc[1]
                elif isinstance(assoc, Key):
                    table = key.table
                elif isinstance(assoc, Table):
                    table = assoc
                else:
                    raise ValueError("expected (str, Key|Table) | Key | Table, not %s" % (assoc,))
                return table.name
            table_name = make_id(*[ get_assoc_name(assoc) for assoc in associates ])
            # HACK: repeat first pass to make proper fkey def constraint names
            used_names = set()
            cdefs, fkdefs = cls._expand_references(table_name, associates, [], used_names)

        # build assoc key from union of associates' foreign key columns
        k_cnames = []
        for fkdef in fkdefs:
            k_cnames.extend([ colref["column_name"] for colref in fkdef["foreign_key_columns"] ])

        kdefs = [
            Key.define(
                k_cnames,
                constraint_name=make_id(table_name, 'assoc', 'key'),
            )
        ]

        # run second pass to expand out metadata targets
        cdefs, fkdefs = cls._expand_references(table_name, cdefs + metadata, fkdefs, used_names)

        return Table.define(
            table_name,
            cdefs,
            kdefs,
            fkdefs,
            comment=comment,
            provide_system=provide_system,
            provide_system_fkeys=provide_system_fkeys,
            key_column_search_order=key_column_search_order,
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

    def clear(self, clear_comment=False, clear_annotations=True, clear_acls=True, clear_acl_bindings=True):
        """Clear all configuration in table and children.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_acls:
            self.acls.clear()
        if clear_acl_bindings:
            self.acl_bindings.clear()
        if clear_annotations:
            self.annotations.clear()
        if clear_comment:
            self.comment = None
        for col in self.column_definitions:
            col.clear(clear_comment=clear_comment, clear_annotations=clear_annotations, clear_acls=clear_acls, clear_acl_bindings=clear_acl_bindings)
        for key in self.keys:
            key.clear(clear_comment=clear_comment, clear_annotations=clear_annotations)
        for fkey in self.foreign_keys:
            fkey.clear(clear_comment=clear_comment, clear_annotations=clear_annotations, clear_acls=clear_acls, clear_acl_bindings=clear_acl_bindings)

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
        if existing is None or not equivalent(self.acls, existing.acls, method='acls'):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings, method='acl_bindings'):
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
            annotations=nochange,
            update_mappings=UpdateMappings.no_update
    ):
        """Alter existing schema definition.

        :param schema_name: Destination schema name (default nochange)
        :param table_name: Replacement table name (default nochange)
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)

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
            old_schema_name = self.schema.name
            del self.schema.tables[self.name]
            self.schema = self.schema.model.schemas[changed['schema_name']]
            for key in self.keys:
                if key.constraint_schema:
                    key.constraint_schema = self.schema
            for fkey in self.foreign_keys:
                if fkey.constraint_schema:
                    del fkey.constraint_schema._fkeys[fkey.constraint_name]
                    fkey.constraint_schema = self.schema
                    fkey.constraint_schema._fkeys[fkey.constraint_name] = fkey
            self.schema.tables[self.name] = self
            if update_mappings:
                for key in self.keys:
                    mmo.replace(self.schema.model, [old_schema_name] + [key.constraint_name], [self.schema.name] + [key.constraint_name])
                for fkey in self.foreign_keys:
                    mmo.replace(self.schema.model, [old_schema_name] + [fkey.constraint_name], [self.schema.name] + [fkey.constraint_name])
                if update_mappings == UpdateMappings.immediate:
                    self.schema.model.apply()

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

    def create_column(self, column_def: dict) -> Column:
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

    def create_key(self, key_def: dict) -> Key:
        """Add a new key to this table in the remote database based on key_def.

           Returns a new Key instance based on the server-supplied
           representation of the new key, and adds it to self.keys
           too.

        """
        def add_key(key):
            self.keys.append(key)
            return key
        return self._create_table_part('key', add_key, Key, key_def)

    def create_fkey(self, fkey_def: dict) -> ForeignKey:
        """Add a new foreign key to this table in the remote database based on fkey_def.

           Returns a new ForeignKey instance based on the
           server-supplied representation of the new foreign key, and
           adds it to self.foreign_keys too.

        """
        def add_fkey(fkey):
            self.foreign_keys.append(fkey)
            fkey.digest_referenced_columns(self.schema.model)
            return fkey
        return self._create_table_part('foreignkey', add_fkey, ForeignKey, fkey_def)

    def create_reference(
        self,
        target: Key | Table | tuple[str, bool, Key | Table] | tuple[str, Key | Table],
        key_column_search_order: Iterable[str] | None = None,
    ) -> tuple[ list[Column], ForeignKey ]:
        """Add column(s) and a foreign key to this table in the remote database for a reference target.

        See Table.define() documentation for more about reference
        targets.

        Returns a list of new Column instances and a ForeignKey instance based on
        the server-supplied representation of the new model elements, and
        adds them to the self.columns and self.foreign_keys too.

        """
        used_names = { col.name for col in self.columns }
        cdefs, fkdefs = self._expand_references(self.name, [ target ], [], used_names, key_column_search_order)
        if not cdefs or len(fkdefs) != 1:
            raise NotImplementedError("BUG? got unexpected results from self._expand_reference()")
        cols = [ self.create_column(cdef) for cdef in cdefs ]
        fkeys = [ self.create_fkey(fkdef) for fkdef in fkdefs ]
        return cols, fkeys[0]

    def drop(self, cascade=False, update_mappings=UpdateMappings.no_update):
        """Remove this table from the remote database.

        :param cascade: drop dependent objects.
        :param update_mappings: update annotations to reflect changes (default False)
        """
        if self.name not in self.schema.tables:
            raise ValueError('Table %s does not appear to belong to schema %s.' % (self, self.schema))

        if cascade:
            for fkey in list(self.referenced_by):
                fkey.drop(update_mappings=update_mappings)

        self.catalog.delete(self.uri_path).raise_for_status()
        del self.schema.tables[self.name]
        for fkey in self.foreign_keys:
            fkey._cleanup()

        if update_mappings:
            for fkey in self.foreign_keys:
                mmo.prune(self.schema.model, [fkey.constraint_schema.name, fkey.constraint_name])
            if update_mappings == UpdateMappings.immediate:
                self.schema.model.apply()

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

    def is_association(self, min_arity=2, max_arity=2, unqualified=True, pure=True, no_overlap=True, return_fkeys=False):
        """Return (truthy) integer arity if self is a matching association, else False.

        min_arity: minimum number of associated fkeys (default 2)
        max_arity: maximum number of associated fkeys (default 2) or None
        unqualified: reject qualified associations when True (default True)
        pure: reject impure assocations when True (default True)
        no_overlap: reject overlapping associations when True (default True)
        return_fkeys: return the set of N associated ForeignKeys if True

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

        # return (truthy) arity or fkeys
        if return_fkeys:
            return covered_fkeys
        else:
            return len(covered_fkeys)

    def find_associations(self, min_arity=2, max_arity=2, unqualified=True, pure=True, no_overlap=True) -> Iterable[FindAssociationResult]:
        """Yield (iterable) Association objects linking to this table and meeting all criteria.

        min_arity: minimum number of associated fkeys (default 2)
        max_arity: maximum number of associated fkeys (default 2) or None
        unqualified: reject qualified associations when True (default True)
        pure: reject impure assocations when True (default True)
        no_overlap: reject overlapping associations when True (default True)

        See documentation for sibling method Table.is_association(...)
        for more explanation of these association detection criteria.

        """
        peer_tables = set()
        for fkey in self.referenced_by:
            peer = fkey.table
            if peer in peer_tables:
                # check each peer only once
                continue
            peer_tables.add(peer)
            answer = peer.is_association(min_arity=min_arity, max_arity=max_arity, unqualified=unqualified, pure=pure, no_overlap=no_overlap, return_fkeys=True)
            if answer:
                answer = set(answer)
                for fkey in answer:
                    if fkey.pk_table == self:
                        answer.remove(fkey)
                        yield FindAssociationResult(peer, fkey, answer)
                        # arbitrarily choose first fkey to self
                        # in case association is back to same table
                        break

    def sqlite3_table_name(self) -> str:
        """Return SQLite3 mapped table name for this table"""
        return "%s:%s" % (
            self.schema.name,
            self.name,
        )

    def sqlite3_ddl(self, keys: bool=True) -> str:
        """Return SQLite3 table definition DDL statement for this table.

        :param keys: If true, include unique constraints for each table key

        Caveat: this utility does not produce:
        - column default expressions
        - foreign key constraint DDL

        Both of these features are fragile in data export scenarios
        where we want to represent arbitrary ERMrest catalog dumps.

        """
        parts = [ col.sqlite3_ddl() for col in self.columns ]
        if keys:
            parts.extend([ key.sqlite3_ddl() for key in self.keys ])
        return ("""
CREATE TABLE IF NOT EXISTS %(tname)s (
  %(body)s
);
""" % {
    'tname': sql_identifier(self.sqlite3_table_name()),
    'body': ',\n  '.join(parts),
})

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @presence_annotation(tag.non_deletable)
    def non_deletable(self): pass
    
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

    @object_annotation(tag.export_2019)
    def export_2019(self): pass

    @object_annotation(tag.export_fragment_definitions)
    def export_fragment_definitions(self): pass
    
    @object_annotation(tag.citation)
    def citation(self): pass

    @object_annotation(tag.source_definitions)
    def source_definitions(self): pass
    
    @object_annotation(tag.indexing_preferences)
    def indexing_preferences(self): pass

    @object_annotation(tag.google_dataset)
    def google_dataset(self): pass
    
    @object_annotation(tag.column_defaults)
    def column_defaults(self): pass

    @object_annotation(tag.viz_3d_display)
    def viz_3d_display(self): pass
    
class Quantifier (str, Enum):
    """Logic quantifiers"""
    any = 'any'
    all = 'all'

def find_tables_with_foreign_keys(target_tables: Iterable[Table], quantifier: Quantifier=Quantifier.all) -> set[Table]:
    """Return set of tables with foreign key references to target tables.

    :param target_tables: an iterable of ermrest_model.Table instances
    :param quantifier: one of the Quantifiers 'any' or 'all' (default 'all')

    Each returned Table instance will be a table that references the
    targets according to the selected quantifier. A reference is a
    direct foreign key in the returned table that refers to a primary
    key of the target table.

    - quantifier==all: a returned table references ALL targets
    - quantifier==any: a returned table references AT LEAST ONE target

    For proper function, all target_tables instances MUST come from
    the same root Model instance hierarchy.

    """
    candidates = None
    for table in target_tables:
        referring = { fkey.table for fkey in table.referenced_by }
        if candidates is None:
            candidates = referring
        elif quantifier == Quantifier.all:
            candidates.intersection_update(referring)
        else:
            candidates.update(referring)
    return candidates

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

    def __repr__(self):
        cls = type(self)
        return "<%s.%s object %r.%r.%r at 0x%x>" % (
            cls.__module__,
            cls.__name__,
            self.table.schema.name if self.table is not None and self.table.schema is not None else None,
            self.table.name if self.table is not None else None,
            self.name,
            id(self),
        )

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

    def clear(self, clear_comment=False, clear_annotations=True, clear_acls=True, clear_acl_bindings=True):
        """Clear all configuration in column

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_acls:
            self.acls.clear()
        if clear_acl_bindings:
            self.acl_bindings.clear()
        if clear_annotations:
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
        if existing is None or not equivalent(self.acls, existing.acls, method='acls'):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings, method='acl_bindings'):
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
            annotations=nochange,
            update_mappings=UpdateMappings.no_update
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
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)

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
            oldname = self.name
            self.name = changed['name']
            self.table.column_definitions.elements[self.name] = self
            if update_mappings:
                basename = [self.table.schema.name, self.table.name]
                mmo.replace(self.table.schema.model, basename + [oldname], basename + [self.name])
                if update_mappings == UpdateMappings.immediate:
                    self.table.schema.model.apply()

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

    def drop(self, cascade=False, update_mappings=UpdateMappings.no_update):
        """Remove this column from the remote database.

        :param cascade: drop dependent objects (default False)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)
        """
        if self.name not in self.table.column_definitions.elements:
            raise ValueError('Column %s does not appear to belong to table %s.' % (self, self.table))

        if cascade:
            for fkey in list(self.table.foreign_keys):
                if self in fkey.foreign_key_columns:
                    fkey.drop(update_mappings=update_mappings)
            for key in list(self.table.keys):
                if self in key.unique_columns:
                    key.drop(cascade=True, update_mappings=update_mappings)

        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.column_definitions[self.name]

        if update_mappings:
            mmo.prune(self.table.schema.model, [self.table.schema.name, self.table.name, self.name])
            if update_mappings == UpdateMappings.immediate:
                self.table.schema.model.apply()

    def sqlite3_ddl(self) -> str:
        """Return SQLite3 column definition DDL fragment for this column."""
        parts = [
            sql_identifier(self.name),
            self.type.sqlite3_ddl(),
        ]
        if not self.nullok:
            parts.append('NOT NULL')
        return ' '.join(parts)

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @presence_annotation(tag.required)
    def required(self): pass
    
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
    elif names[0][0] == 'placeholder':
        # mitigate ermrest API response bug reflecting our 'placeholder' value
        if constraint.table.kind == 'table':
            if isinstance(constraint, Key):
                constraint_schema = constraint.table.schema
            elif isinstance(constraint, ForeignKey):
                # HACK: mostly correct for regular ermrest users
                # may be revised later during fkey digest for irregular cases with SQL views!
                constraint_schema = constraint.table.schema
            else:
                raise TypeError('_constraint_name_parts requires a Key or ForeignKey constraint argument, not %s' % (constraint,))
        else:
            constraint_schema = None
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

    def __repr__(self):
        cls = type(self)
        return "<%s.%s object %r.%r at 0x%x>" % (
            cls.__module__,
            cls.__name__,
            self.constraint_schema.name if self.constraint_schema is not None else None,
            self.constraint_name,
            id(self),
        )

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
    def define(cls, colnames, constraint_names=[], comment=None, annotations={}, constraint_name=None):
        """Build a key definition.

        :param colnames: List of names of columns participating in the key
        :param constraint_names: Legacy input [ [ schema_name, constraint_name ] ] (for API backwards-compatibility)
        :param comment: Comment string
        :param annotations: Dictionary of { annotation_uri: annotation_value, ... }
        :param constraint_name: Constraint name string

        The constraint_name kwarg takes a bare constraint name string
        and acts the same as setting the legacy constraint_names kwarg
        to: [ [ "placeholder", constraint_name ] ].  This odd syntax
        is for backwards-compatibility with earlier API versions, and
        mirrors the structure of constraint names in ERMrest model
        description outputs. In those outputs, the "placeholder" field
        contains the schema name of the table containing the
        constraint.

        """
        if not isinstance(colnames, list):
            raise TypeError('Colnames should be a list.')
        if constraint_name is not None:
            constraint_names = [ [ "placeholder", constraint_name ] ]
        return {
            'unique_columns': list(colnames),
            'names': constraint_names,
            'comment': comment,
            'annotations': annotations,
        }

    def clear(self, clear_comment=False, clear_annotations=True):
        """Clear all configuration in key

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_annotations:
            self.annotations.clear()
        if clear_comment:
            self.comment = None

    def apply(self, existing=None):
        """Apply configuration to corresponding key in catalog unless existing already matches.

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
            annotations=nochange,
            update_mappings=UpdateMappings.no_update
    ):
        """Alter existing schema definition.

        :param constraint_name: Unqualified constraint name string
        :param comment: Replacement comment (default nochange)
        :param annotations: Replacement annotations (default nochange)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)

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
            oldname = self.constraint_name
            self.constraint_name = changed['names'][0][1]
            if update_mappings:
                basename = [self.table.schema.name]
                mmo.replace(self.table.schema.model, basename + [oldname], basename + [self.constraint_name])
                if update_mappings == UpdateMappings.immediate:
                    self.table.schema.model.apply()

        if 'comment' in changes:
            self.comment = changed['comment']

        if 'annotations' in changes:
            self.annotations.clear()
            self.annotations.update(changed['annotations'])

        return self

    def drop(self, cascade=False, update_mappings=UpdateMappings.no_update):
        """Remove this key from the remote database.

        :param cascade: drop dependent objects (default False)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)
        """
        if self.name not in self.table.keys.elements:
            raise ValueError('Key %s does not appear to belong to table %s.' % (self, self.table))

        if cascade:
            for fkey in list(self.table.referenced_by):
                assert self.table == fkey.pk_table, "Expected key.table and foreign_key.pk_table to match"
                if set(self.unique_columns) == set(fkey.referenced_columns):
                    fkey.drop(update_mappings=update_mappings)

        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.keys[self.name]

        if update_mappings:
            mmo.prune(self.table.schema.model, [self.constraint_schema.name, self.constraint_name])
            if update_mappings == UpdateMappings.immediate:
                self.table.schema.model.apply()

    def sqlite3_ddl(self) -> str:
        """Return SQLite3 unique constraint DDL fragment for this key."""
        parts = [ sql_identifier(col.name) for col in self.unique_columns ]
        return 'UNIQUE (%s)' % (', '.join(parts),)

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

    def __repr__(self):
        cls = type(self)
        return "<%s.%s object %r.%r at 0x%x>" % (
            cls.__module__,
            cls.__name__,
            self.constraint_schema.name if self.constraint_schema is not None else None,
            self.constraint_name,
            id(self),
        )

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
            # HACK: clean up schema qualification for psuedo constraint
            # this may happen only with SQL views in the ermrest catalog
            if self.pk_table.kind != 'table' and self.constraint_name in self.table.schema._fkeys:
                del self.table.schema._fkeys[self.constraint_name]
                self.table.schema.model._fkeys[self.constraint_name] = self
                del self.table.foreign_keys.elements[(self.table.schema, self.constraint_name)]
                self.table.foreign_keys.elements[(None, self.constraint_name)] = self

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
    def define(cls, fk_colnames, pk_sname, pk_tname, pk_colnames, on_update='NO ACTION', on_delete='NO ACTION', constraint_names=[], comment=None, acls={}, acl_bindings={}, annotations={}, constraint_name=None):
        """Define a foreign key.

        :param fk_colnames: List of column names participating in the foreign key
        :param pk_sname: Schema name string of the referenced primary key
        :param pk_tname: Table name string of the referenced primary key
        :param pk_colnames: List of column names participating in the referenced primary key
        :param on_update: Constraint behavior when referenced primary keys are updated
        :param on_update: Constraint behavior when referenced primary keys are deleted
        :param constraint_names: Legacy input [ [ schema_name, constraint_name ] ] (for API backwards-compatibility)
        :param comment: Comment string
        :param acls: Dictionary of { acl_name: acl, ... }
        :param acl_bindings: Dictionary of { binding_name: acl_binding, ... }
        :param annotations: Dictionary of { annotation_uri: annotation_value, ... }
        :param constraint_name: Constraint name string

        The contraint behavior values for on_update and on_delete must
        be one of the following literal strings:

        'NO ACTION', 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT'

        The constraint_name kwarg takes a bare constraint name string
        and acts the same as setting the legacy constraint_names kwarg
        to: [ [ "placeholder", constraint_name ] ].  This odd syntax
        is for backwards-compatibility with earlier API versions, and
        mirrors the structure of constraint names in ERMrest model
        description outputs. In those outputs, the "placeholder" field
        contains the schema name of the table containing the
        constraint.

        """
        if len(fk_colnames) != len(pk_colnames):
            raise ValueError('The fk_colnames and pk_colnames lists must have the same length.')
        if constraint_name is not None:
            constraint_names = [ [ "placeholder", constraint_name ], ]
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

    def clear(self, clear_comment=False, clear_annotations=True, clear_acls=True, clear_acl_bindings=True):
        """Clear all configuration in foreign key

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.
        """
        if clear_acls:
            self.acls.clear()
            self.acls.update({"insert": ["*"], "update": ["*"]})
        if clear_acl_bindings:
            self.acl_bindings.clear()
        if clear_annotations:
            self.annotations.clear()
        if clear_comment:
            self.comment = None

    def apply(self, existing=None):
        """Apply configuration to corresponding foreign key in catalog unless existing already matches.

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
        if existing is None or not equivalent(self.acls, existing.acls, method='foreign_key_acls'):
            changes['acls'] = self.acls
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings, method='acl_bindings'):
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
            annotations=nochange,
            update_mappings=UpdateMappings.no_update
    ):
        """Alter existing schema definition.

        :param constraint_name: Replacement constraint name string
        :param on_update: Replacement on-update action string
        :param on_delete: Replacement on-delete action string
        :param comment: Replacement comment (default nochange)
        :param acls: Replacement ACL configuration (default nochange)
        :param acl_bindings: Replacement ACL bindings (default nochange)
        :param annotations: Replacement annotations (default nochange)
        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)

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
            oldname = self.constraint_name
            self.constraint_name = changed['names'][0][1]
            if self.constraint_schema:
                self.constraint_schema._fkeys[self.constraint_name] = self
            else:
                self.table.schema.model._pseudo_fkeys[self.constraint_name] = self
            if update_mappings:
                basename = [self.table.schema.name]
                mmo.replace(self.table.schema.model, basename + [oldname], basename + [self.constraint_name])
                if update_mappings == UpdateMappings.immediate:
                    self.table.schema.model.apply()

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

    def drop(self, update_mappings=UpdateMappings.no_update):
        """Remove this foreign key from the remote database.

        :param update_mappings: Update annotations to reflect changes (default UpdateMappings.no_updates)
        """
        if self.name not in self.table.foreign_keys.elements:
            raise ValueError('Foreign key %s does not appear to belong to table %s.' % (self, self.table))
        self.catalog.delete(self.uri_path).raise_for_status()
        del self.table.foreign_keys[self.name]
        self._cleanup()

        if update_mappings:
            mmo.prune(self.table.schema.model, [self.constraint_schema.name, self.constraint_name])
            if update_mappings == UpdateMappings.immediate:
                self.table.schema.model.apply()

    def _cleanup(self):
        """Cleanup references in the local model following drop from remote database.
        """
        del self.pk_table.referenced_by[self.name]
        if self.constraint_schema:
            del self.constraint_schema._fkeys[self.constraint_name]
        else:
            del self.table.schema.model._pseudo_fkeys[self.constraint_name]

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

    def sqlite3_ddl(self) -> str:
        """Return a SQLite3 column type DDL fragment for this type"""
        return {
            'boolean': 'boolean',
            'date': 'date',
            'float4': 'real',
            'float8': 'real',
            'int2': 'integer',
            'int4': 'integer',
            'int8': 'integer',
            'json': 'json',
            'jsonb': 'json',
            'timestamptz': 'datetime',
            'timestamp': 'datetime',
        }.get(self.typename, 'text')

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

    def sqlite3_ddl(self) -> str:
        """Return a SQLite3 column type DDL fragment for this type"""
        return self.base_type.sqlite3_ddl()

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

    def sqlite3_ddl(self) -> str:
        """Return a SQLite3 column type DDL fragment for this type"""
        return 'json'

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
                'longtext': 'text',
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
