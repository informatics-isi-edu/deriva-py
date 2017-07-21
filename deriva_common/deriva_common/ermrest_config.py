from . import urlquote


class AttrDict (dict):
    """Dictionary with optional attribute-based lookup.

       For keys that are valid attributes, self.key is equivalent to
       self[key].
    """
    def __getattr__(self, a):
        return self[a]

    def __setattr__(self, a, v):
        self[a] = v

# convenient enumeration of common annotation tags
tag = AttrDict({
    'generated':          'tag:isrd.isi.edu,2016:generated',
    'immutable':          'tag:isrd.isi.edu,2016:immutable',
    'display':            'tag:misd.isi.edu,2015:display',
    'visible_columns':    'tag:isrd.isi.edu,2016:visible-columns',
    'visible_foreign_keys': 'tag:isrd.isi.edu,2016:visible-foreign-keys',
    'table_display':      'tag:isrd.isi.edu,2016:table-display',
    'table_alternatives': 'tag:isrd.isi.edu,2016:table-alternatives',
    'column_display':     'tag:isrd.isi.edu,2016:column-display',
    'asset':              'tag:isrd.isi.edu,2017:asset',
})


def equivalent(doc1, doc2):
    """Determine whether two dict/array/literal documents are structurally equivalent."""
    if isinstance(doc1, dict) and isinstance(doc2, dict):
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

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        self.uri_path = uri_path
        self.update_uri_path = uri_path
        self.annotations = dict(node_doc.get('annotations', {}))

    def apply(self, catalog, existing=None):
        if existing is None or not equivalent(self.annotations, existing.annotations):
            catalog.put(
                '%s/%s' % (self.update_uri_path, 'annotation'),
                json=self.annotations
            )

    def clear(self):
        """Clear existing annotations on node."""
        self.annotations.clear()

    def annotation_obj(self, tag):
        """Generic access to annotation object under given tag.

           Returns object stored under tag in node's annotations, so
           that side-effects applied to it will affect the annotation.

           If annotation is not yet present, an empty object is added
           and returned.
        """
        if tag not in self.annotations:
            self.annotations[tag] = AttrDict({})
        return self.annotations[tag]

    def annotation_presence(self, tag):
        """Return True if annotation is present for given tag, False otherwise."""
        return tag in self.annotations

    def set_annotation_presence(self, tag, value):
        """Add or remove annotation with given tag depending on boolean presence value.

           True: add or replace tag with None value
           False: remove tag if it exists
        """
        if value:
            self.annotations[tag] = None
        else:
            self.annotations.pop(tag, None)

    def prejson(self):
        """Produce a representation of configuration as generic Python data structures"""
        d = dict()
        if self.annotations:
            d["annotations"] = self.annotations
        return d

    @property
    def immutable(self):
        return self.annotation_presence(tag.immutable)

    @immutable.setter
    def immutable(self, value):
        self.set_annotation_presence(tag.immutable, value)

    @property
    def generated(self):
        return self.annotation_presence(tag.generated)

    @generated.setter
    def generated(self, value):
        self.set_annotation_presence(tag.generated, value)

    @property
    def display(self):
        return self.annotation_obj(tag.display)


class NodeConfigAcl (NodeConfig):
    """Generic model acl-bearing document node configuration management.

       acls: map of acls for node by key
       annotations: map of annotations for node by key

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        NodeConfig.__init__(self, uri_path, node_doc)
        self.acls = AttrDict(node_doc.get('acls', {}))

    def apply(self, catalog, existing=None):
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

       Convenience access for common annotations:
         self.display: access mutable tag.display object
         self.generated: treat tag.generated as a boolean
         self.immutable: treat tag.immutable as a boolean
    """
    def __init__(self, uri_path, node_doc):
        NodeConfigAcl.__init__(self, uri_path, node_doc)
        self.acl_bindings = AttrDict(node_doc.get('acl_bindings', {}))

    def apply(self, catalog, existing=None):
        NodeConfigAcl.apply(self, catalog, existing)
        if existing is None or not equivalent(self.acl_bindings, existing.acl_bindings):
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
    def __init__(self, model_doc):
        NodeConfigAcl.__init__(self, "/schema", model_doc)
        self.update_uri_path = ""
        self.schemas = {
            sname: CatalogSchema(sname, sdoc)
            for sname, sdoc in model_doc.get('schemas', {}).items()
        }

    @classmethod
    def fromcatalog(cls, catalog):
        """Retrieve catalog config as a CatalogConfig management object."""
        return cls(catalog.get("/schema").json())

    def apply(self, catalog, existing=None):
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
    def __init__(self, sname, schema_doc):
        NodeConfigAcl.__init__(
            self,
            "/schema/%s" % urlquote(sname),
            schema_doc
        )
        self.name = sname
        self.tables = {
            tname: CatalogTable(sname, tname, tdoc)
            for tname, tdoc in schema_doc.get('tables', {}).items()
        }

    def apply(self, catalog, existing=None):
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

    def __init__(self, sname, tname, table_doc):
        NodeConfigAclBinding.__init__(
            self,
            "/schema/%s/table/%s" % (urlquote(sname), urlquote(tname)),
            table_doc
        )
        self.sname = sname
        self.name = tname
        self.column_definitions = KeyedList([
            CatalogColumn(sname, tname, cdoc)
            for cdoc in table_doc.get('column_definitions', [])
        ])
        self.keys = MultiKeyedList([
            CatalogKey(sname, tname, kdoc)
            for kdoc in table_doc.get('keys', [])
        ])
        self.foreign_keys = MultiKeyedList([
            CatalogForeignKey(sname, tname, fkdoc)
            for fkdoc in table_doc.get('foreign_keys', [])
        ])

    def apply(self, catalog, existing=None):
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
        d["column_definitions"] = [
            column.prejson()
            for column in self.column_definitions
        ]
        d["keys"] = [
            key.prejson()
            for key in self.keys
        ]
        return d

    @property
    def alternatives(self):
        return self.annotation_obj(tag.table_alternatives)

    @property
    def table_display(self):
        return self.annotation_obj(tag.table_display)

    @property
    def visible_columns(self):
        return self.annotation_obj(tag.visible_columns)

    @property
    def visible_foreign_keys(self):
        return self.annotation_obj(tag.visible_foreign_keys)


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

    def __init__(self, sname, tname, column_doc):
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

    @property
    def asset(self):
        return self.annotation_obj(tag.asset)

    @property
    def column_display(self):
        return self.annotation_obj(tag.column_display)


class CatalogKey (NodeConfig):
    """Key-level configuration management.

       annotations: column-level annotations
       names: name(s) of key constraint
    """
    def __init__(self, sname, tname, key_doc):
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
    def __init__(self, sname, tname, fkey_doc):
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
        d = NodeConfig.prejson(self)
        d['foreign_key_columns'] = self.foreign_key_columns
        d['referenced_columns'] = self.referenced_columns
        d['names'] = self.names
        return d
