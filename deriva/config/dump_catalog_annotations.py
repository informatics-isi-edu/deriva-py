import sys
import json
import re
from deriva.core import ErmrestCatalog, AttrDict, get_credential
from deriva.config.base_config import BaseSpec, BaseSpecList, ConfigUtil, ConfigBaseCLI
from deriva.config.annotation_config import AttrSpec, AttrSpecList

MY_VERSION = 0.99


class Annotations:
    def __init__(self, server, catalog, credentials, config):
        self.annotations = {}
        self.ignored_schema_patterns = []
        ip = config.get("ignored_schema_patterns")
        if ip is not None:
            for p in ip:
                self.ignored_schema_patterns.append(re.compile(p))
        self.types = set()
        self.managed_attributes = []
        self.ignore_unmanaged = True
        self.ignored_attributes = []
        self.consolidated_annotations = {}
        self.annotations_to_delete = []
        if config is not None:
            known_attributes = config.get("known_attributes")
            if known_attributes is not None:
                self.managed_attributes = known_attributes.get("managed", [])
                self.ignored_attributes = known_attributes.get("ignored", [])
                self.annotations_to_delete = known_attributes.get("to_delete", [])
                self.ignore_unmanaged = known_attributes.get("ignore_all_unmanaged", True)
                self.annotations["known_attributes"] = known_attributes
            else:
                self.annotations["known_attributes"] = {'managed': [], 'ignore_all_unmanaged': True}
            self.consolidated_annotations = config.get("consolidated_annotations", {})
            for k in AttrSpecList.SPEC_TYPES:
                d = self.consolidated_annotations.get(k)
                if d is None:
                    d = [dict()]
                self.consolidated_annotations[k] = AttrSpecList(known_attributes, d)
                self.annotations[k] = self.munge_specs(self.consolidated_annotations[k])

        for k in self.managed_attributes:
            if k in self.annotations_to_delete:
                raise ValueError("{k} is both 'managed' and 'to_delete'".format(k=k))
        self.catalog = ErmrestCatalog('https', server, catalog, credentials)
        self.catalog_config = self.catalog.getCatalogModel()
        if self.catalog_config.annotations is not None:
            self.add_catalog_annotations(self.catalog_config)
        if self.catalog_config.schemas is not None:
            for s in self.catalog_config.schemas.values():
                self.add_schema_annotations(s)

    def munge_specs(self, annotation_list):
        speclist = []
        if annotation_list is not None:
            if isinstance(annotation_list, AttrSpecList):
                annotation_list = annotation_list.get_specs()
            for spec in annotation_list:
                speclist.append(spec.config_format())
        return speclist

    def consolidated_schema_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["schema_annotations"].get_specs():
            if c.schema_entry_matches(annotation.get("schema"), key=annotation.get("uri")):
                matches.append(c)
        return self.check_consolidation(matches, annotation.get("value"))

    def consolidated_table_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["table_annotations"].get_specs():
            if c.table_entry_matches(annotation.get("schema"),
                                     annotation.get("table"),
                                     key=annotation.get("uri")):
                matches.append(c)
        return self.check_consolidation(matches, annotation.get("value"))

    def consolidated_column_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["column_annotations"].get_specs():
            if c.column_entry_matches(annotation.get("schema"),
                                      annotation.get("table"),
                                      annotation.get("column"),
                                      key=annotation.get("uri")):
                matches.append(c)
        return self.check_consolidation(matches, annotation.get("value"))

    def consolidated_foreign_key_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["foreign_key_annotations"].get_specs():
            if c.foreign_key_entry_matches(annotation.get("schema"),
                                           annotation.get("table"),
                                           annotation.get("foreign_key_schema"),
                                           annotation.get("foreign_key"),
                                           key=annotation.get("uri")):
                matches.append(c)
        return self.check_consolidation(matches, annotation.get("value"))

    def check_consolidation(self, matches, value):
        if len(matches) != 1:
            # Zero or more than one matching pattern, so we need the exact spec to disambiguate
            return False

        match = matches[0]

        #        if match.get("override") == True:
        #            # We don't care what the original version was. We want to go with the pattern match
        #            return True

        return match.get("value") == value

    def add_catalog_annotations(self, catalog):
        annotations = self.find_relevant_annotations(catalog.annotations)
        if annotations is not None:
            for v in annotations:
                self.annotations["catalog_annotations"].append(v)

    def add_schema_annotations(self, schema):
        annotations = self.find_relevant_annotations(schema.annotations)
        if annotations is not None:
            for v in annotations:
                v["schema"] = schema.name
                if not self.consolidated_schema_annotation(v):
                    self.annotations["schema_annotations"].append(v)
        for table in schema.tables.values():
            self.add_table_annotations(table)

    def add_table_annotations(self, table):
        annotations = self.find_relevant_annotations(table.annotations)
        if annotations is not None:
            for v in annotations:
                v["schema"] = table.schema.name
                v["table"] = table.name
                if not self.consolidated_table_annotation(v):
                    self.annotations["table_annotations"].append(v)
        for column in table.column_definitions:
            self.add_column_annotations(table, column)
        for fkey in table.foreign_keys:
            self.add_foreign_key_annotations(fkey)

    def add_column_annotations(self, table, column):
        annotations = self.find_relevant_annotations(column.annotations)
        if annotations is not None:
            for v in annotations:
                v["schema"] = table.schema.name
                v["table"] = table.name
                v["column"] = column.name
                if not self.consolidated_column_annotation(v):
                    self.annotations["column_annotations"].append(v)

    def add_foreign_key_annotations(self, fkey):
        annotations = self.find_relevant_annotations(fkey.annotations)
        if annotations is not None:
            if len(fkey.names) < 1:
                raise ValueError("foreign key without a name")
            for v in annotations:
                v["schema"] = fkey.table.schema.name
                v["table"] = fkey.table.name
                v["foreign_key_schema"] = fkey.names[0][0]
                v["foreign_key"] = fkey.names[0][1]
                if not self.consolidated_foreign_key_annotation(v):
                    self.annotations["foreign_key_annotations"].append(v)

    def find_relevant_annotations(self, annotations):
        if annotations is None or len(annotations) == 0:
            return None
        new = []
        if self.managed_attributes is None:
            for k in annotations.keys():
                if k not in self.annotations_to_delete:
                    new.append({"uri": k, "value": annotations[k]})
                    self.types.add(k)
        else:
            for k in annotations.keys():
                if k in self.managed_attributes:
                    new.append({"uri": k, "value": annotations[k]})
                    self.types.add(k)
        if len(new) == 0:
            return None
        return new

    def dumps(self):
        return json.dumps(self.annotations, indent=4, sort_keys=True)

    def types_list(self):
        types = list(self.types)
        types.sort()
        return types


def main():
    cli = ConfigBaseCLI("annotation dump tool", None, version=MY_VERSION)
    cli.parser.add_argument('-l', help="list tags encountered", action="store_true")
    args = cli.parse_cli()
    managed_attrs = None
    if args.config_file is None:
        print("No config file specified")
        return 1
    if args.host is None:
        print("No host specified")
        return 1
    config = json.load(open(args.config_file))
    credentials = get_credential(args.host, args.credential_file)
    annotations = Annotations(args.host, args.catalog, credentials, config)
    if args.l:
        for t in annotations.types_list():
            print(t)
    if not args.l:
        print(annotations.dumps())
    return 0


if __name__ == '__main__':
    sys.exit(main())
