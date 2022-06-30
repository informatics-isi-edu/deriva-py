import re
from deriva.core import BaseCLI
import platform

class ConfigUtil:
    @classmethod
    def find_toplevel_node(cls, catalog_config, schema_name, table_name):
        if table_name is not None and schema_name is None:
            raise ValueError("table specified without a schema")

        if schema_name is None:
            return catalog_config

        schema = catalog_config.schemas.get(schema_name)
        if schema is None:
            raise (ValueError("no schema named {s} found.".format(s=schema_name)))

        if table_name is None:
            return schema
        else:
            table = schema.tables.get(table_name)
            if table is None:
                raise (ValueError("no table named {t} in schema {s}".format(s=schema_name, t=table_name)))
            return table

class BaseSpec(dict):
    ATTRIBUTE_TYPES = ["schema", "table", "column", "foreign_key"]

    def __init__(self, specdict, speclist, spectype, validate_node_types_only=False):
        self.specdict = specdict
        self.speclist = speclist
        self.spectype = spectype
        self.node_types = ["schema", "table", "column", "foreign_key", "foreign_key_schema"]
        self.set_keys()        
        if not validate_node_types_only:
            self.validate()
        # self.validate_node_types() # FIXME: this causes an error for catalog annotations which do not have any of attributes listed in self.node_types

    def get_pattern(self, base):
        return self.get(self.make_pat_name(base))

    def is_exact_spec(self):
        for n in self.node_types:
            if self.get_pattern(n) is not None:
                return False
        return True

    def validate(self):
        for k in self.specdict.keys():
            valid = False
            for n in self.node_types:
                if k == n or k == self.make_pat_name(n):
                    valid = True
            if k in self.speclist:
                valid = True
            if not valid:
                raise ValueError("unknown key '{key}' in {t} spec".format(key=k, t=self.spectype))
        return True

    def has_node_key(self, key):
        return key in self.specdict or self.make_pat_name(key) in self.specdict

    def is_node_spec(self):
        for key in self.node_types:
            if key in self.specdict or self.make_pat_name(key) in self.specdict:
                return True
        return False

    def validate_node_types(self):
        if not self.is_node_spec():
            raise ValueError("Unknown node type {s}".format(s=str(self.specdict)))

        if not self.has_node_key('schema'):
            raise ValueError("No schema entry or pattern in spec {s}".format(s=str(self.specdict)))
        if not self.has_node_key('table'):
            if self.has_node_key('column') or self.has_node_key('foreign_key'):
                raise ValueError("No table entry or pattern in spec {s}".format(s=str(self.specdict)))

            if self.has_node_key('foreign_key'):
                if not self.has_node_key('foreign_key_schema'):
                    raise ValueError("foreign_key spec missing foreign_key_schema: {s}".format(s=str(self.specdict)))

    def config_format(self):
        new = dict()
        for k in self.specdict.keys():
            if isinstance(self.specdict[k], re._pattern_type):
                new[k] = self.specdict[k].pattern
            else:
                new[k] = self.specdict[k]
        return new

    def set_keys(self):
        for k in self.specdict.keys():
            self[k] = self.specdict[k]
        for n in self.node_types:
            if self.get_pattern(n) is not None:
                if self.get(n) is not None:
                    raise ValueError(
                        "can't have both '{n}' and '{p}' in the same spec".format(n=n, p=self.make_pat_name(n)))
                self[self.make_pat_name(n)] = re.compile(self.get_pattern(n))

    @classmethod
    def make_pat_name(cls, base):
        return base + '_pattern'

    def matches(self, base, value, exact=False, key=None):
        if key is not None and not self.get(u'uri') == key:
            return False
        if self.get(base) is not None and value == self.get(base):
            return True
        if exact:
            return False
        else:
            pat = self.get_pattern(base)
            return pat is not None and pat.match(value) is not None

    def schema_entry_matches(self, schema_name, exact=False, key=None):
        return self.matches('schema', schema_name, exact=exact, key=key)

    def foreign_key_entry_matches(self, table_schema_name, table_name, fkey_schema_name, fkey_name, exact=False,
                                  key=None):
        return self.table_entry_matches(table_schema_name, table_name, exact=exact, key=key) and self.matches(
            "foreign_key_schema", fkey_schema_name, exact=exact, key=key) and self.matches("foreign_key", fkey_name,
                                                                                           exact=exact, key=key)

    def table_entry_matches(self, schema_name, table_name, exact=False, key=None):
        return self.schema_entry_matches(schema_name, exact=exact, key=key) and self.matches("table", table_name,
                                                                                             exact=exact, key=key)

    def column_entry_matches(self, schema_name, table_name, column_name, exact=False, key=None):
        return self.table_entry_matches(schema_name, table_name, exact=exact, key=key) and self.matches('column',
                                                                                                        column_name,
                                                                                                        exact=exact,
                                                                                                        key=key)


class BaseSpecList:
    def __init__(self, type, dictlist=None, strict=True):
        dictlist = dictlist if dictlist else []
        self.type = type
        self.specs = []
        self.add_list(dictlist)
        self.strict = strict

    def get_specs(self):
        return self.specs

    def add_spec(self, spec):
        self.specs.append(spec)

    def add_list(self, dictlist):
        for d in dictlist:
            if len(d) > 0:
                self.add_spec(self.type(d))

    def find_best_schema_spec(self, schema_name, key=None):
        results = []
        for spec in self.specs:
            if spec.schema_entry_matches(schema_name, key=key):
                results.append(spec)
        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            exact_match = None
            for spec in results:
                if spec.schema_entry_matches(schema_name, exact=True, key=key):
                    if exact_match is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-match spec for schema {s}, key {k}".format(s=schema_name,
                                                                                                k=str(key)))
                        else:
                            print("WARNING: More than one exact-match spec for schema {s}, key {k}, skipping".format(
                                s=schema_name, k=str(key)))
                    else:
                        exact_match = spec
            if exact_match is None:
                if self.strict:
                    raise ValueError(
                        "More than one regexp match and no exact-match spec for schema {s}, key {k}".format(
                            s=schema_name, k=str(key)))
                else:
                    print(
                        "WARNING: More than one regexp match and no exact-match spec for schema {s}, key {k},"
                        " skipping".format(s=schema_name, k=str(key)))
            return exact_match

    def find_catalog_spec(self, key):
        results = []
        for spec in self.specs:
            if spec.get("uri") == key:
                return spec

    def find_best_foreign_key_spec(self, table_schema_name, table_name, names, key=None):
        exact_specs = []
        pattern_specs = []
        for n in names:
            new = self.find_best_foreign_key_name_spec(table_schema_name, table_name, n[0], n[1], key)
            if new is not None:
                if new.is_exact_spec():
                    exact_specs.append(new)
                else:
                    pattern_specs.append(new)
        if len(exact_specs) == 1:
            return exact_specs[0]
        elif len(exact_specs) == 0:
            if len(pattern_specs) == 1:
                return pattern_specs[0]
            elif len(pattern_specs) == 0:
                return None
            else:
                if self.strict:
                    raise ValueError(
                        "No exact-match and more than one pattern-match foreign key specification for foreign key with"
                        " names {n} on table {ts}.{t} : {s}".format(
                            n=str(names), t=table_name, ts=table_schema_name, s=str(list(pattern_specs))))
                else:
                    print(
                        "WARNING: No exact-match and more than one pattern-match foreign key specification for foreign"
                        " key with names {n} on table {ts}.{t} : {s}, skipping".format(
                            n=str(names), t=table_name, ts=table_schema_name, s=str(list(pattern_specs))))
        else:
            if self.strict:
                raise ValueError(
                    "More than one exact-match foreign key specification for foreign key with names {n} on table"
                    " {ts}.{t} : {s}".format(
                        n=str(names), t=table_name, ts=table_schema_name, s=str(list(exact_specs))))
            else:
                print(
                    "WARNING: More than one exact-match foreign key specification for foreign key with names "
                    "{n} on table {ts}.{t} : {s}, skipping".format(
                        n=str(names), t=table_name, ts=table_schema_name, s=str(list(exact_specs))))

    def find_best_foreign_key_name_spec(self, table_schema_name, table_name, fkey_schema_name, fkey_name, key):
        results = []
        for spec in self.specs:
            if spec.foreign_key_entry_matches(table_schema_name, table_name, fkey_schema_name, fkey_name, key=key):
                results.append(spec)
        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            result = None
            for spec in results:
                if spec.foreign_key_entry_matches(table_schema_name, table_name, fkey_schema_name, fkey_name,
                                                  exact=True, key=key):
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-match entry for foreign key {fs}.{f} on table {s}.{t}: {r}".format(
                                    s=table_schema_name, t=table_name, fs=fkey_schema_name, f=fkey_name,
                                    r=str(results)))
                        else:
                            print(
                                "WARNING: More than one exact-match entry for foreign key {fs}.{f} on table "
                                "{s}.{t}: {r}, skipping".format(
                                    s=table_schema_name, t=table_name, fs=fkey_schema_name, f=fkey_name,
                                    r=str(results)))
                    else:
                        result = spec
            if result is None:
                if self.strict:
                    raise ValueError(
                        "More than one matching entry for foreign key {fs}.{f} on table {s}.{t} but no exact match: "
                        "{r}".format(
                            s=table_schema_name, t=table_name, fs=fkey_schema_name, f=fkey_name, r=str(results)))
                else:
                    print(
                        "WARNING: More than one matching entry for foreign key {fs}.{f} on table {s}.{t} but no exact "
                        "match: {r}, skipping".format(
                            s=table_schema_name, t=table_name, fs=fkey_schema_name, f=fkey_name, r=str(results)))
            return result

    def find_best_table_spec(self, schema_name, table_name, key=None):
        results = []
        for spec in self.specs:
            if spec.table_entry_matches(schema_name, table_name, key=key):
                results.append(spec)
        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            result = None
            for spec in results:
                if spec.table_entry_matches(schema_name, table_name, exact=True, key=key):
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-match entry for table {s}.{t}".format(s=schema_name, t=table_name))
                        else:
                            print("WARNING: More than one exact-match entry for table {s}.{t}, skipping".format(
                                s=schema_name, t=table_name))
                    else:
                        result = spec
            if result is not None:
                return result
            for spec in results:
                if spec.schema_entry_matches(schema_name, exact=True, key=key):
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-schema and no exact-table acl entry for {s}.{t}".format(
                                    s=schema_name, t=table_name))
                        else:
                            print(
                                "WARNING: More than one exact-schema and no exact-table acl entry for {s}.{t},"
                                " skipping".format(
                                    s=schema_name, t=table_name))
                    else:
                        result = spec
            return result

    def find_best_single_foreign_key_spec(self, schema_name, table_name, fkey_name, key=None):
        results = []
        for spec in self.specs:
            if spec.foreign_key_entry_matches(schema_name, table_name, fkey_name, key=key):
                results.append(spec)
        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            result = None
            for spec in results:
                if spec.is_exact_spec():
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-match entry for foreign key {s}.{f}".format(s=schema_name,
                                                                                                 f=fkey_name))
                        else:
                            print(
                                "WARNING: More than one exact-match entry for foreign key {s}.{f}".format(s=schema_name,
                                                                                                          f=fkey_name))
                    else:
                        result = spec
            if result is not None:
                return result
            for spec in results:
                if spec.schema_entry_matches(schema_name, exact=True, key=key):
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-schema and no exact-fkey entry for {s}.{f}, skipping".format(
                                    s=schema_name, f=fkey_name))
                        else:
                            print(
                                "WARNING: More than one exact-schema and no exact-fkey entry for {s}.{f},"
                                " skipping".format(
                                    s=schema_name, f=fkey_name))
                    else:
                        result = spec
            return result

    def find_best_column_spec(self, schema_name, table_name, column_name, key=None):
        results = []
        for spec in self.specs:
            if spec.column_entry_matches(schema_name, table_name, column_name, key=key):
                results.append(spec)
                #                print("column_entry_matches: {s}".format(s=str(spec)))
        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            result = None
            for spec in results:
                if spec.is_exact_spec():
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-match acl entry for column {s}.{t}.{c}, key {k}".format(
                                    s=schema_name, t=table_name, c=column_name, k=str(key)))
                        else:
                            print(
                                "WARNING: More than one exact-match acl entry for column {s}.{t}.{c},"
                                " key {k}, skipping".format(
                                    s=schema_name, t=table_name, c=column_name, k=str(key)))
                    else:
                        result = spec
            if result is not None:
                return result
            for spec in results:
                if spec.table_entry_matches(schema_name, table_name, exact=True, key=key):
                    if result is not None:
                        if self.strict:
                            raise ValueError(
                                "More than one exact-schema/exact-table and no exact-column acl entry for column "
                                "{s}.{t}.{c}, key {k}".format(
                                    s=schema_name, t=table_name, c=column_name, k=str(key)))
                        else:
                            print(
                                "WARNING: More than one exact-schema/exact-table and no exact-column acl entry for "
                                "column {s}.{t}.{c}, key {k}, skipping".format(
                                    s=schema_name, t=table_name, c=column_name, k=str(key)))
                    else:
                        result = spec
            return result


class ConfigBaseCLI(BaseCLI):
    def __init__(self, description, epilog, version):
        BaseCLI.__init__(self, description, epilog, version)
        self.remove_options(['--config-file'])
        self.parser.add_argument('--config-file', metavar='<file>', help="Path to a configuration file.", required=True)
        self.parser.add_argument('-s', '--schema', help="schema name", default=None, action='append')
        self.parser.add_argument('-t', '--table', help="table name", default=None, action='append')
        self.parser.add_argument('-n', '--dryrun', help="dryrun", action="store_true")
        self.parser.add_argument('-v', '--verbose', help="verbose", action="store_true")
        self.parser.add_argument('catalog', help="catalog ID", type=str)
        self.parser.set_defaults(host=platform.uname()[1])

    @classmethod
    def check_schema_table_args(cls, args):
        # Allowed combinations:
        # - multiple schemas, no tables
        # - 1 schema, 1 table
        # - no schemas, no tables
        if args.table is not None:
            if len(args.table) > 1:
                raise ValueError("More than one table specified")
            if args.schema is None or len(args.schema) != 1:
                raise ValueError("Table specified without exactly one schema")

    @classmethod
    def get_schema_arg_list(cls, args):
        cls.check_schema_table_args(args)
        if args.schema is None:
            return [None]
        else:
            return args.schema

    @classmethod
    def get_table_arg(cls, args):
        cls.check_schema_table_args(args)
        if args.table is None:
            return None
        else:
            return args.table[0]
