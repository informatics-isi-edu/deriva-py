import sys
import deriva_common
import os
from urllib import quote
import json
import re
from deriva_common import ErmrestCatalog, AttrDict
import argparse
from base_config import BaseSpec, BaseSpecList, ConfigUtil, ConfigBaseCLI
from annotation_config import AttrSpec, AttrSpecList
MY_VERSION=0.99

class Annotations:
    def __init__(self, server, catalog, credentials, config):
        self.annotations = {}
        ip = config.get("ignored_schema_patterns")
        if ip != None:
            for p in ip:
                self.ignored_schema_patterns.append(re.compile(p))
        self.types = set()
        self.managed_attributes = []
        self.ignore_unmanaged = True
        self.ignored_attributes = []
        self.consolidated_annotations = {}
        self.annotations_to_delete = []
        if config != None:
            known_attributes = config.get("known_attributes")
            if known_attributes != None:
                self.managed_attributes = known_attributes.get("managed")
                self.ignored_attributes = known_attributes.get("ignored")
                self.annotations_to_delete = known_attributes.get("to_delete")                
                self.ignore_unmanaged = known_attributes.get("ignore_all_unmanaged")
                self.annotations["known_attributes"] = known_attributes
            else:
                self.annotations["known_attributes"] = {'managed' : [], 'ignore_all_unmanaged' : True}
            self.consolidated_annotations = config.get("consolidated_annotations")
        for k in AttrSpecList.SPEC_TYPES:
            d = self.consolidated_annotations.get(k)
            if d == None:
                d = [dict()]
            self.consolidated_annotations[k] = AttrSpecList(known_attributes, d)
            self.annotations[k] = self.munge_specs(self.consolidated_annotations[k])

        for k in self.managed_attributes:
            if k in self.annotations_to_delete:
                raise ValueError("{k} is both 'managed' and 'to_delete'".format(k=k))
        self.catalog = ErmrestCatalog('https', server, catalog, credentials)
        self.catalog_config = self.catalog.getCatalogConfig()
        if self.catalog_config.annotations != None:
            self.annotations["catalog_annotations"] = self.catalog_config.annotations.values()
        if self.catalog_config.schemas != None:
            for s in self.catalog_config.schemas.values():
                self.add_schema_annotations(s)

    def munge_specs(self, annotation_list):
        speclist = []
        if annotation_list != None:
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
        return(self.check_consolidation(matches, annotation.get("value")))

    def consolidated_table_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["table_annotations"].get_specs():
            if c.table_entry_matches(annotation.get("schema"), annotation.get("table"), key=annotation.get("uri")):
                matches.append(c)
        return(self.check_consolidation(matches, annotation.get("value")))

    def consolidated_column_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["column_annotations"].get_specs():
            if c.column_entry_matches(annotation.get("schema"), annotation.get("table"), annotation.get("column"), key=annotation.get("uri")):
                matches.append(c)
        return(self.check_consolidation(matches, annotation.get("value")))
    
    def consolidated_foreign_key_annotation(self, annotation):
        matches = []
        for c in self.consolidated_annotations["foreign_key_annotations"].get_specs():
            if c.foreign_key_entry_matches(annotation.get("schema"), annotation.get("table"), annotation.get("foreign_key_schema"), annotation.get("foreign_key"), key=annotation.get("uri")):
                matches.append(c)
        return(self.check_consolidation(matches, annotation.get("value")))

    
    def check_consolidation(self, matches, value):
        if len(matches) != 1:
            # Zero or more than one matching pattern, so we need the exact spec to disambiguate
            return False
        
        match = matches[0]
        
#        if match.get("override") == True:
#            # We don't care what the original version was. We want to go with the pattern match
#            return True

        return match.get("value") == value

                
    def add_schema_annotations(self, schema):
        annotations = self.find_relevant_annotations(schema.annotations)
        if annotations != None:
            for v in annotations:
                v["schema"] = schema.name
                if not self.consolidated_schema_annotation(v):
                    self.annotations["schema_annotations"].append(v)
        for table in schema.tables.values():
            self.add_table_annotations(table)

    def add_table_annotations(self, table):
        annotations = self.find_relevant_annotations(table.annotations)
        if annotations != None:
            for v in annotations:
                v["schema"] = table.sname
                v["table"] = table.name
                if not self.consolidated_table_annotation(v):
                    self.annotations["table_annotations"].append(v)                
        for column in table.column_definitions:
            self.add_column_annotations(table, column)
        for fkey in table.foreign_keys:
            self.add_foreign_key_annotations(fkey)            
            
    def add_column_annotations(self, table, column):
        annotations = self.find_relevant_annotations(column.annotations)
        if annotations != None:
            for v in annotations:
                v["schema"] = table.sname
                v["table"] = table.name                        
                v["column"] = column.name
                if not self.consolidated_column_annotation(v):
                    self.annotations["column_annotations"].append(v)

    def add_foreign_key_annotations(self, fkey):
        annotations = self.find_relevant_annotations(fkey.annotations)                        
        if annotations != None:
            if len(fkey.names) < 1:
                raise ValueError("foreign key without a name")            
            for v in annotations:
                v["schema"] = fkey.sname
                v["table"] = fkey.tname
                v["foreign_key_schema"] = fkey.names[0][0]
                v["foreign_key"] = fkey.names[0][1]
                if not self.consolidated_foreign_key_annotation(v):                
                    self.annotations["foreign_key_annotations"].append(v)

    def find_relevant_annotations(self, annotations):
        if annotations == None or len(annotations) == 0:
            return None
        new = []
        if managed_attrs == None:
            for k in annotations.keys():
                if not k in self.annotations_to_delete:
                    new.append({"uri" : k, "value" : annotations[k]})
                    self.types.add(k)
        else:
            for k in annotations.keys():
                if k in managed_attrs:
                    new.append({"uri" : k, "value" : annotations[k]})
                    self.types.add(k)                    
        if len(new) == 0:
            return None
        return(new)
            
    def dumps(self):
        return json.dumps(self.annotations, indent=4, sort_keys = True)

    def types_list(self):
        l = list(self.types)
        l.sort()
        return l


if __name__ == '__main__':
    cli = ConfigBaseCLI("annotation dump tool", None, version=MY_VERSION)
    cli.parser.add_argument('-l', help="list tags encountered", action="store_true")    
    args = cli.parse_cli()
    managed_attrs = None
    if args.config_file == None:
        print("No config file specified")
        sys.exit(1)
    if args.host == None:
        print("No host specified")
        sys.exit(1)            
    config=json.load(open(args.config_file))
    credentials = ConfigUtil.get_credentials(args.host, args.credential_file)
    annotations = Annotations(args.host, args.catalog, credentials, config)
    if args.l:
        for t in annotations.types_list():
            print(t)
    if not args.l:
        print(annotations.dumps())
