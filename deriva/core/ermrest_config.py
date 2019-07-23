import json
from collections import OrderedDict
from . import urlquote


class AttrDict (dict):
    """Dictionary with optional attribute-based lookup.

       For keys that are valid attributes, self.key is equivalent to
       self[key].
    """
    def __getattr__(self, a):
        try:
            return self[a]
        except KeyError as e:
            raise AttributeError(str(e))

    def __setattr__(self, a, v):
        self[a] = v

# convenient enumeration of common annotation tags
tag = AttrDict({
    'generated':          'tag:isrd.isi.edu,2016:generated',
    'immutable':          'tag:isrd.isi.edu,2016:immutable',
    'display':            'tag:misd.isi.edu,2015:display',
    'visible_columns':    'tag:isrd.isi.edu,2016:visible-columns',
    'visible_foreign_keys': 'tag:isrd.isi.edu,2016:visible-foreign-keys',
    'foreign_key':        'tag:isrd.isi.edu,2016:foreign-key',
    'table_display':      'tag:isrd.isi.edu,2016:table-display',
    'table_alternatives': 'tag:isrd.isi.edu,2016:table-alternatives',
    'column_display':     'tag:isrd.isi.edu,2016:column-display',
    'asset':              'tag:isrd.isi.edu,2017:asset',
    'bulk_upload':        'tag:isrd.isi.edu,2017:bulk-upload',
    'export':             'tag:isrd.isi.edu,2016:export',
    'chaise_config':      'tag:isrd.isi.edu,2019:chaise-config',
})

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

class NodeConfig (object):
    """Generic model document node configuration management.

       annotations: map of annotations for node by key
       comment: comment string or None (if supported by sub-class)

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        self.uri_path = uri_path
        self.update_uri_path = uri_path
        self.annotations = dict(node_doc.get('annotations', {}))
        self._supports_comment = True
        self._comment = node_doc.get('comment')

    def apply(self, catalog, existing=None):
        """Apply configuration to corresponding node in catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The configuration in self.comment and self.annotations will be
        applied to the remote model node corresponding to self, unless
        existing node configuration is supplied and is equivalent.
        """
        if self._supports_comment:
            if existing is None or not equivalent(self._comment, existing._comment):
                if self._comment is not None:
                    catalog.put('%s/comment' % self.update_uri_path, data=self._comment)
                else:
                    catalog.delete('%s/comment' % self.update_uri_path)
        if existing is None or not equivalent(self.annotations, existing.annotations):
            catalog.put(
                '%s/%s' % (self.update_uri_path, 'annotation'),
                json=self.annotations
            )

    def clear(self, clear_comment=False):
        """Clear existing annotations on node, also clearing comment if clear_comment is True.

        NOTE: as a backwards-compatible heuristic, comments are
        retained by default so that a typical configuration-management
        client does not strip useful documentation from existing models.

        """
        self.annotations.clear()
        if clear_comment and self._supports_comment:
            self.comment = None

    def prejson(self):
        """Produce a representation of configuration as generic Python data structures"""
        d = dict()
        if self.annotations:
            d["annotations"] = self.annotations
        return d

    @property
    def comment(self):
        """Comment on this node in model, if supported.

        Raises TypeError if accessed when unsupported, e.g. on top-level catalog objects.
        """
        if self._supports_comment:
            return self._comment
        else:
            raise TypeError('%r does not support comment management.' % type(self).__name__)

    @comment.setter
    def comment(self, value):
        """Comment on this node in model, if supported.

        Raises TypeError if accessed when unsupported, e.g. on top-level catalog objects.
        """
        if self._supports_comment:
            self._comment = value
        else:
            raise TypeError('%r does not support comment management.' % type(self).__name__)

    @presence_annotation(tag.immutable)
    def immutable(self): pass

    @presence_annotation(tag.generated)
    def generated(self): pass

    @object_annotation(tag.display)
    def display(self): pass

class NodeConfigAcl (NodeConfig):
    """Generic model acl-bearing document node configuration management.

       acls: map of acls for node by key
       annotations: map of annotations for node by key
       comment: comment string or None (if supported by sub-class)

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        NodeConfig.__init__(self, uri_path, node_doc)
        self.acls = AttrDict(node_doc.get('acls', {}))

    def apply(self, catalog, existing=None):
        """Apply configuration to corresponding node in catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The configuration in self.comment, self.annotations, and
        self.acls will be applied to the remote model node
        corresponding to self, unless existing node configuration is
        supplied and is equivalent.

        """
        NodeConfig.apply(self, catalog, existing)
        if existing is None or not equivalent(self.acls, existing.acls):
            catalog.put(
                '%s/%s' % (self.update_uri_path, 'acl'),
                json=self.acls
            )

    def clear(self):
        """Clear existing acls and annotations on node."""
        NodeConfig.clear(self)
        self.acls.clear()

    def prejson(self):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfig.prejson(self)
        if self.acls:
            d["acls"] = self.acls
        return d


class NodeConfigAclBinding (NodeConfigAcl):
    """Generic model acl_binding-bearing document node configuration management.

       acl_bindings: map of acl bindings for node by key
       acls: map of acls for node by key
       annotations: map of annotations for node by key
       comment: comment string or None (if supported by sub-class)

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        NodeConfigAcl.__init__(self, uri_path, node_doc)
        self.acl_bindings = AttrDict(node_doc.get('acl_bindings', {}))

    def apply(self, catalog, existing=None):
        """Apply configuration to corresponding node in catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self, or None to apply configuration unconditionally.

        The configuration in self.comment, self.annotations,
        self.acls, and self.acl_bindings will be applied to the remote
        model node corresponding to self, unless existing node
        configuration is supplied and is equivalent.

        """
        NodeConfigAcl.apply(self, catalog, existing)
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings, method='acl_binding'):
            catalog.put(
                '%s/%s' % (self.update_uri_path, 'acl_binding'),
                json=self.acl_bindings
            )

    def clear(self):
        """Clear existing acl_bindings, acls, and annotations on node."""
        NodeConfigAcl.clear(self)
        self.acl_bindings.clear()

    def prejson(self):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfigAcl.prejson(self)
        if self.acl_bindings:
            d["acl_bindings"] = self.acl_bindings
        return d


class CatalogConfig (NodeConfigAcl):
    """Top-level catalog configuration management.

       acls: catalog-level ACL configuration
       annotations: catalog-level annotations
       schemas: all schemas in catalog, by name
    """
    def __init__(self, model_doc, **kwargs):
        NodeConfigAcl.__init__(self, "/schema", model_doc)
        self._supports_comment = False
        self.update_uri_path = ""
        self.schemas = {
            sname: kwargs.get('schema_class', CatalogSchema)(sname, sdoc, **kwargs)
            for sname, sdoc in model_doc.get('schemas', {}).items()
        }

    @classmethod
    def fromcatalog(cls, catalog):
        """Retrieve catalog config as a CatalogConfig management object."""
        return cls(catalog.get("/schema").json())

    @classmethod
    def fromfile(cls, schema_file):
        """Deserialize a JSON schema file as a CatalogConfig management object."""
        with open(schema_file) as sf:
            schema = sf.read()

        return cls(json.loads(schema, object_pairs_hook=OrderedDict))

    def apply(self, catalog, existing=None):
        """Apply catalog configuration to catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self.

        The configuration in self will be applied recursively to the
        corresponding model nodes in schema. For each node, the
        comment, annotations, acls, and/or acl_bindings will be
        applied where applicable.

        If existing is not provided (default), the current whole
        configuration will be retrieved from the catalog and used
        automatically to determine whether the configuration goals
        under this CatalogConfig instance are already met or need to
        be remotely applied.

        """
        if existing is None:
            existing = self.fromcatalog(catalog)
        NodeConfigAcl.apply(self, catalog, existing)
        for sname, schema in self.schemas.items():
            schema.apply(catalog, existing.schemas[sname])

    def clear(self):
        """Clear all configuration in catalog and children."""
        NodeConfigAcl.clear(self)
        for schema in self.schemas.values():
            schema.clear()

    def table(self, sname, tname):
        """Return table configuration for table with given name."""
        return self.schemas[sname].tables[tname]

    def column(self, sname, tname, cname):
        """Return column configuration for column with given name."""
        return self.table(sname, tname).column_definitions[cname]

    @object_annotation(tag.bulk_upload)
    def bulk_upload(self): pass

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfigAcl.prejson(self)
        d["schemas"] = {
            sname: schema.prejson()
            for sname, schema in self.schemas.items()
        }
        return d

class CatalogSchema (NodeConfigAcl):
    """Schema-level configuration management.

       acls: schema-level ACL configuration
       annotations: schema-level annotations
       tables: all tables in schema, by name

       Convenience access for common annotations:
         self.display: access mutable tag.display object
    """
    def __init__(self, sname, schema_doc, **kwargs):
        NodeConfigAcl.__init__(
            self,
            "/schema/%s" % urlquote(sname),
            schema_doc
        )
        self.name = sname
        self.tables = {
            tname: kwargs.get('table_class', CatalogTable)(sname, tname, tdoc, **kwargs)
            for tname, tdoc in schema_doc.get('tables', {}).items()
        }

    def apply(self, catalog, existing=None):
        """Apply schema configuration to catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self.

        The configuration in self will be applied recursively to the
        corresponding model nodes in catalog. For each node, the
        comment, annotations, acls, and/or acl_bindings will be
        applied where applicable unless existing value is equivalent.

        """
        NodeConfigAcl.apply(self, catalog, existing)
        for tname, table in self.tables.items():
            table.apply(catalog, existing.tables[tname] if existing else None)

    def clear(self):
        """Clear all configuration in schema and children."""
        NodeConfigAcl.clear(self)
        for table in self.tables.values():
            table.clear()

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfigAcl.prejson(self)
        d["schema_name"] = self.name
        d["tables"] = {
            tname: table.prejson()
            for tname, table in self.tables.items()
        }
        return d


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
            raise ValueError('Element name %s already exists.' % e.name)
        list.append(self, e)
        self.elements[e.name] = e

class MultiKeyedList (list):
    """Multi-keyed list."""
    def __init__(self, l):
        list.__init__(self, l)
        self.elements = {
            tuple(name): e
            for e in l
            for name in e.names
        }

    def __getitem__(self, idx):
        """Get element by key or by list index or slice."""
        if isinstance(idx, (tuple, list)):
            return self.elements[idx]
        else:
            return list.__getitem__(self, idx)

    def __delitem__(self, idx):
        """Delete element by key or by list index."""
        item = self[idx]
        list.__delitem__(self, self.index(item))
        for name in item.names:
            del self.elements[name]

    def append(self, e):
        """Append element to list and record its keys."""
        for name in e.names:
            if name in self.elements:
                raise ValueError('Element name %s already exists.' % e.names)
        list.append(self, e)
        for name in e.names:
            self.elements[name] = e

class CatalogTable (NodeConfigAclBinding):
    """Table-level configuration management.

       acl_bindings: table-level dynamic ACL bindings
       acls: table-level ACL configuration
       annotations: table-level annotations
       column_definitions: columns in table

       Convenience access to common annotations:
         self.alternatives: tag.table_alternatives object
         self.display: tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
         self.table_display: tag.table_display object
         self.visible_columns: tag.visible_columns object
         self.visible_foreign_keys: tag.visible_foreign_keys object
    """

    def __init__(self, sname, tname, table_doc, **kwargs):
        NodeConfigAclBinding.__init__(
            self,
            "/schema/%s/table/%s" % (urlquote(sname), urlquote(tname)),
            table_doc
        )
        self.sname = sname
        self.name = tname
        self.column_definitions = KeyedList([
            kwargs.get('column_class', CatalogColumn)(sname, tname, cdoc, **kwargs)
            for cdoc in table_doc.get('column_definitions', [])
        ])
        self.keys = MultiKeyedList([
            kwargs.get('key_class', CatalogKey)(sname, tname, kdoc, **kwargs)
            for kdoc in table_doc.get('keys', [])
        ])
        self.foreign_keys = MultiKeyedList([
            kwargs.get('foreign_key_class', CatalogForeignKey)(sname, tname, fkdoc, **kwargs)
            for fkdoc in table_doc.get('foreign_keys', [])
        ])

    def apply(self, catalog, existing=None):
        """Apply table configuration to catalog unless existing already matches.

        :param catalog: The EmrestCatalog instance to which configuration will be applied.
        :param existing: An instance comparable to self.

        The configuration in self will be applied recursively to the
        corresponding model nodes in catalog. For each node, the
        comment, annotations, acls, and/or acl_bindings will be
        applied where applicable unless existing is supplied and is
        equivalent.

        """
        NodeConfigAclBinding.apply(self, catalog, existing)
        for col in self.column_definitions:
            col.apply(catalog, existing.column_definitions[col.name] if existing else None)
        for key in self.keys:
            key.apply(catalog, existing.keys[key.names[0]] if existing else None)
        for fkey in self.foreign_keys:
            fkey.apply(catalog, existing.foreign_keys[fkey.names[0]] if existing else None)

    def clear(self):
        """Clear all configuration in table and children."""
        NodeConfigAclBinding.clear(self)
        for col in self.column_definitions:
            col.clear()
        for key in self.keys:
            key.clear()
        for fkey in self.foreign_keys:
            fkey.clear()

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfigAclBinding.prejson(self)
        d["schema_name"] = self.sname
        d["table_name"] = self.name
        d["column_definitions"] = [
            column.prejson()
            for column in self.column_definitions
        ]
        d["keys"] = [
            key.prejson()
            for key in self.keys
        ]
        d["foreign_keys"] = [
            fkey.prejson()
            for fkey in self.foreign_keys
        ]
        return d

    @object_annotation(tag.table_alternatives)
    def alternatives(self): pass

    @object_annotation(tag.table_display)
    def table_display(self): pass

    @object_annotation(tag.visible_columns)
    def visible_columns(self): pass

    @object_annotation(tag.visible_foreign_keys)
    def visible_foreign_keys(self): pass

class CatalogColumn (NodeConfigAclBinding):
    """Column-level configuration management.

       acl_bindings: column-level dynamic ACL bindings
       acls: column-level ACL configuration
       annotations: column-level annotations
       name: name of column

       Convenience access to common annotations:
         self.asset: tag.asset object
         self.column_display:: tag.column_display object
         self.display: tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """

    def __init__(self, sname, tname, column_doc, **kwargs):
        cname = column_doc['name']
        NodeConfigAclBinding.__init__(
            self,
            "/schema/%s/table/%s/column/%s" % (urlquote(sname), urlquote(tname), urlquote(cname)),
            column_doc
        )
        self.sname = sname
        self.tname = tname
        self.name = cname

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfig.prejson(self)
        d["name"] = self.name
        return d

    @object_annotation(tag.asset)
    def asset(self): pass

    @object_annotation(tag.column_display)
    def column_display(self): pass

class CatalogKey (NodeConfig):
    """Key-level configuration management.

       annotations: column-level annotations
       names: name(s) of key constraint
    """
    def __init__(self, sname, tname, key_doc, **kwargs):
        NodeConfig.__init__(
            self,
            '/schema/%s/table/%s/key/%s' % (
                urlquote(sname),
                urlquote(tname),
                ','.join([ urlquote(cname) for cname in key_doc['unique_columns'] ])
            ),
            key_doc
        )
        self.sname = sname
        self.tname = tname
        self.names = [ tuple(name) for name in key_doc['names'] ]
        self.unique_columns = key_doc['unique_columns']

    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        d = NodeConfig.prejson(self)
        d['unique_columns'] = self.unique_columns
        d['names'] = self.names
        return d


class CatalogForeignKey (NodeConfigAclBinding):
    """Foreign key-level configuration management.

       acl_bindings: foreign key-level acl-bindings
       acls: foreign key-level acls
       annotations: foreign key-level annotations
    """
    def __init__(self, sname, tname, fkey_doc, **kwargs):
        refcols = fkey_doc['referenced_columns']
        NodeConfigAclBinding.__init__(
            self,
            '/schema/%s/table/%s/foreignkey/%s/reference/%s:%s/%s' % (
                urlquote(sname),
                urlquote(tname),
                ','.join([ urlquote(col['column_name']) for col in fkey_doc['foreign_key_columns'] ]),
                urlquote(refcols[0]['schema_name']),
                urlquote(refcols[0]['table_name']),
                ','.join([ urlquote(col['column_name']) for col in refcols ]),
            ),
            fkey_doc
        )
        self.sname = sname
        self.tname = tname
        self.names = [ tuple(name) for name in fkey_doc['names'] ]
        self.foreign_key_columns = fkey_doc['foreign_key_columns']
        self.referenced_columns = fkey_doc['referenced_columns']
    
    def prejson(self, prune=True):
        """Produce a representation of configuration as generic Python data structures"""
        def expand(c):
            c['schema_name'] = self.sname
            c['table_name'] = self.tname
            return c
        d = NodeConfig.prejson(self)
        d['foreign_key_columns'] = [
            expand(c)
            for c in self.foreign_key_columns
        ]
        d['referenced_columns'] = self.referenced_columns
        d['names'] = self.names
        return d

    @object_annotation(tag.foreign_key)
    def foreign_key(self): pass
