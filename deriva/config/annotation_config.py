import sys
import json
import re
from deriva.core import ErmrestCatalog, AttrDict, ermrest_model, get_credential
from deriva.config.base_config import BaseSpec, BaseSpecList, ConfigUtil, ConfigBaseCLI

MY_VERSION = 0.99


class NoForeignKeyError(ValueError):
    pass


class AttrSpecList(BaseSpecList):
    SPEC_TYPES = ["catalog_annotations", "schema_annotations", "table_annotations", "column_annotations",
                  "foreign_key_annotations"]

    def __init__(self, known_attrs, specdict, strict=False):
        self.ignore_unmanaged = False
        self.managed_annotations = self.annotation_list(known_attrs.get(u'managed'))
        if self.managed_annotations is None:
            raise ValueError("No 'managed' attribute list")
        if known_attrs.get(u'ignore_all_unmanaged'):
            self.ignore_unmanaged = True
        self.ignored_annotations = self.annotation_list(known_attrs.get(u'ignored'))
        if self.ignored_annotations is None:
            self.ignored_annotations = []
        # dictlist = dictlist + [{"managed_annotations": self.managed_annotations}, {"ignored_annotations": self.ignored_annotations}, {"ignore_all_unmanaged": self.ignore_unmanaged}]
        BaseSpecList.__init__(self, AttrSpec, specdict, strict)

    def annotation_list(self, orig_list):
        if orig_list is None:
            return None
        new = []
        for item in orig_list:
            new.append(item)
        return new

    def add_list(self, dictlist):
        for d in dictlist:
            if len(d) > 0:
                s = AttrSpec(d, self.managed_annotations, self.ignore_unmanaged, self.ignored_annotations)
                self.add_spec(s)


class AttrSpec(BaseSpec):
    def __init__(self, specdict, managed_annotations, ignore_unmanaged, ignored_annotations):
        BaseSpec.__init__(self, specdict, ["uri", "value"], "attributes", ignore_unmanaged)
        self.ignore_unmanaged = ignore_unmanaged
        self.managed_annotations = managed_annotations
        self.ignored_annotations = ignored_annotations
        self.known_annotations = self.managed_annotations + self.ignored_annotations
        self.validate_annotation()

    def validate_annotation(self):
        return self.specdict.get("uri") in self.managed_annotations


class AttrConfig:
    def __init__(self, server, catalog_id, config_file, credentials, verbose=False, schema_name=None, table_name=None):
        self.config = json.load(open(config_file))
        self.ignored_schema_patterns = []
        ip = self.config.get("ignored_schema_patterns")
        if ip is not None:
            for p in ip:
                self.ignored_schema_patterns.append(re.compile(p))
        self.known_attrs = self.config.get(u'known_attributes')
        self.managed_annotations = self.known_attrs.get(u'managed')
        self.known_annotations = self.managed_annotations
        self.all_annotations = self.known_annotations
        self.ignored_annotations = self.known_attrs.get(u'ignored')
        if self.ignored_annotations is not None:
            self.all_annotations = self.all_annotations + self.ignored_annotations
        self.ignore_unmanaged = self.known_attrs.get(u'ignore_all_unmanaged')
        self.annotation_specs = dict()
        for key in AttrSpecList.SPEC_TYPES:
            self.annotation_specs[key] = self.make_speclist(key)
        self.server = server
        self.catalog_id = catalog_id
        self.verbose = verbose
        old_catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials)
        self.saved_toplevel_config = ConfigUtil.find_toplevel_node(old_catalog.getCatalogModel(), schema_name,
                                                                   table_name)
        self.catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials)
        self.toplevel_config = ConfigUtil.find_toplevel_node(self.catalog.getCatalogModel(), schema_name, table_name)

    def make_speclist(self, name):
        d = self.config.get(name)
        if d is None:
            d = [dict()]
        return AttrSpecList(self.known_attrs, d)

    def find_best_schema_specs(self, schema_name):
        specs = dict()
        for key in self.managed_annotations:
            specs[key] = self.annotation_specs["schema_annotations"].find_best_schema_spec(schema_name, key=key)
        return specs

    def find_best_table_specs(self, schema_name, table_name):
        specs = dict()
        for key in self.managed_annotations:
            specs[key] = self.annotation_specs["table_annotations"].find_best_table_spec(schema_name, table_name,
                                                                                         key=key)
        return specs

    def find_best_fkey_specs(self, fkey):
        specs = dict()
        for key in self.managed_annotations:
            specs[key] = self.annotation_specs["foreign_key_annotations"].find_best_foreign_key_spec(fkey.table.schema.name,
                                                                                                     fkey.table.name,
                                                                                                     fkey.names,
                                                                                                     key=key)
        return specs

    def find_best_column_specs(self, schema_name, table_name, column_name):
        specs = dict()
        for key in self.managed_annotations:
            specs[key] = self.annotation_specs["column_annotations"].find_best_column_spec(schema_name, table_name,
                                                                                           column_name, key=key)
        return specs

    def node_name(self, node):
        if isinstance(node, ermrest_model.Schema):
            return "schema {s}".format(s=str(node.name))
        if isinstance(node, ermrest_model.Table):
            return "table {s}.{t}".format(s=str(node.schema.name), t=str(node.name))
        if isinstance(node, ermrest_model.Column):
            return "column {s}.{t}.{c}".format(s=str(node.table.schema.name), t=str(node.table.name), c=str(node.name))
        if isinstance(node, ermrest_model.ForeignKey):
            return "foreign key {n}".format(n=str(node.names))
        return str("unknown node type {t}".format(t=type(node)))

    def set_node_annotations(self, node, specs, saved_node):
        if specs is None:
            if not self.ignore_unmanaged:
                if self.verbose:
                    print("{n}: clearing annotations".format(n=self.node_name(node)))
                node.annotations.clear()
            return
        for k in self.managed_annotations:
            s = specs.get(k)
            if s is not None and u'value' in s:
                if self.verbose:
                    print("{n}: setting {k} to {v}".format(n=self.node_name(node), k=k, v=s[u'value']))
                node.annotations[k] = s[u'value']
            elif k in node.annotations:
                if self.verbose:
                    print("{n}: clearing {k}".format(n=self.node_name(node), k=k))
                node.annotations.pop(k)
        if not self.ignore_unmanaged:
            for k in node.annotations.keys():
                if k not in self.all_annotations:
                    raise ValueError("annotation key {k} is neither managed nor ignored".format(k=k))

    def set_table_annotations(self, table, saved_table):
        self.set_node_annotations(table, self.find_best_table_specs(table.schema.name, table.name), saved_table)
        for column in table.column_definitions:
            self.set_column_annotations(column, self.find_named_column(saved_table, column.name))
        for fkey in table.foreign_keys:
            self.set_fkey_annotations(fkey, self.find_corresponding_fkey(saved_table, fkey))

    def find_corresponding_fkey(self, table, base_fkey):
        if table is None:
            return None
        if base_fkey.names is None or len(base_fkey.names) == 0:
            return None
        names = base_fkey.names[0]
        if len(names) != 2:
            return None
        for fkey in table.foreign_keys:
            if fkey is not None and fkey.names is not None and len(fkey.names) > 0:
                for n in fkey.names:
                    if len(n) == 2 and n[0] == names[0] and n[1] == names[1]:
                        return fkey
        return None

    def find_named_column(self, table, column_name):
        if table is None:
            return None
        for column in table.column_definitions:
            if column.name == column_name:
                return column
        return None

    def find_named_schema(self, catalog, schema_name):
        if catalog is None or catalog.schemas is None:
            return None
        return catalog.schemas.get(schema_name)

    def find_named_table(self, schema, table_name):
        if schema is None:
            return None
        if schema.tables is None:
            return None
        return schema.tables.get(table_name)

    def set_fkey_annotations(self, fkey, saved_fkey):
        self.set_node_annotations(fkey, self.find_best_fkey_specs(fkey), saved_fkey)

    def set_column_annotations(self, column, saved_column):
        self.set_node_annotations(column, self.find_best_column_specs(column.table.schema.name, column.table.name, column.name),
                                  saved_column)

    def set_schema_annotations(self, schema, saved_schema):
        for pat in self.ignored_schema_patterns:
            if pat.match(schema.name) is not None:
                print("ignoring schema {s}".format(s=schema.name))
                return
        specs = self.find_best_schema_specs(schema.name)
        self.set_node_annotations(schema, specs, saved_schema)
        for table in schema.tables.values():
            self.set_table_annotations(table, self.find_named_table(saved_schema, table.name))

    def set_catalog_annotations(self):
        specs = dict()
        for key in self.managed_annotations:
            specs[key] = self.annotation_specs["catalog_annotations"].find_catalog_spec(key)
        self.set_node_annotations(self.toplevel_config, specs, self.saved_toplevel_config)

        for schema in self.toplevel_config.schemas.values():
            self.set_schema_annotations(schema, self.find_named_schema(self.saved_toplevel_config, schema.name))

    def set_attributes(self):
        if isinstance(self.toplevel_config, ermrest_model.Model):
            self.set_catalog_annotations()
        elif isinstance(self.toplevel_config, ermrest_model.Schema):
            self.set_schema_annotations(self.toplevel_config, self.saved_toplevel_config)
        elif isinstance(self.toplevel_config, ermrest_model.Table):
            self.set_table_annotations(self.toplevel_config, self.saved_toplevel_config)
        else:
            raise ValueError("toplevel config is a {t}".format(t=str(type(self.toplevel_config))))

    def apply_annotations(self):
        self.toplevel_config.apply(self.saved_toplevel_config)


def main():
    cli = ConfigBaseCLI("annotation config tool", None, version=MY_VERSION)
    args = cli.parse_cli()
    table_name = cli.get_table_arg(args)
    schema_names = cli.get_schema_arg_list(args)
    credentials = get_credential(args.host, args.credential_file)
    for schema in schema_names:
        attr_config = AttrConfig(args.host, args.catalog, args.config_file, credentials, args.verbose or args.debug,
                                 schema, table_name)
        attr_config.set_attributes()
        if not args.dryrun:
            attr_config.apply_annotations()


if __name__ == '__main__':
    sys.exit(main())
