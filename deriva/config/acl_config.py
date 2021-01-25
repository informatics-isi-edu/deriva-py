import sys
import json
import re
from deriva.core import ErmrestCatalog, AttrDict, ermrest_model, get_credential, __version__ as VERSION, \
    format_exception, urlquote
from deriva.config.base_config import BaseSpec, BaseSpecList, ConfigUtil, ConfigBaseCLI
from requests.exceptions import HTTPError
from uuid import UUID
import warnings


class NoForeignKeyError(ValueError):
    pass


class ACLSpecList(BaseSpecList):
    def __init__(self, dictlist=None):
        BaseSpecList.__init__(self, ACLSpec, dictlist)


class ACLSpec(BaseSpec):
    def __init__(self, specdict):
        BaseSpec.__init__(self, specdict, ["acl", "no_acl", "acl_bindings", "invalidate_bindings"], "acl")

    def validate(self):
        BaseSpec.validate(self)
        if self.get("no_acl") not in [True, False, None]:
            raise ValueError("no_acl must be True or False (or not present)")
        if self.get("acl") is not None and self.get("no_acl"):
            raise ValueError("can't specify an acl and no_acl=True in the same spec")
        if self.get("acl") is None and self.get("no_acl") == False:
            raise ValueError("if no_acl=False, an acl must be specified")


class AclConfig:
    NC_NAME = 'name'
    GC_NAME = 'groups'
    ACL_TYPES = ["catalog_acl", "schema_acls", "table_acls", "column_acls", "foreign_key_acls"]
    GLOBUS_PREFIX = 'https://auth.globus.org/'
    ROBOT_PREFIX_FORMAT = 'https://{server}/webauthn_robot/'

    def __init__(self, server, catalog_id, config_file, credentials, schema_name=None, table_name=None, verbose=False):
        self.config = json.load(open(config_file))
        self.ignored_schema_patterns = []
        self.verbose = verbose
        self.server = server
        self.catalog_id = catalog_id
        ip = self.config.get("ignored_schema_patterns")
        if ip is not None:
            for p in ip:
                self.ignored_schema_patterns.append(re.compile(p))
        self.acl_specs = {"catalog_acl": self.config.get("catalog_acl")}
        for key in self.ACL_TYPES:
            if key != "catalog_acl":
                self.acl_specs[key] = self.make_speclist(key)
        self.groups = self.config.get("groups")
        self.expand_groups()
        self.acl_definitions = self.config.get("acl_definitions")
        self.expand_acl_definitions()
        self.acl_bindings = self.config.get("acl_bindings")
        self.invalidate_bindings = self.config.get("invalidate_bindings")

        old_catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials)
        self.saved_toplevel_config = ConfigUtil.find_toplevel_node(old_catalog.getCatalogModel(), schema_name,
                                                                   table_name)
        self.catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials)
        self.toplevel_config = ConfigUtil.find_toplevel_node(self.catalog.getCatalogModel(), schema_name, table_name)

    def make_speclist(self, name):
        d = self.config.get(name)
        if d is None:
            d = dict()
        return ACLSpecList(d)

    def add_node_acl(self, node, acl_name):
        acl = self.acl_definitions.get(acl_name)
        if acl is None:
            raise ValueError("no acl set called '{name}'".format(name=acl_name))
        for k in acl.keys():
            node.acls[k] = acl[k]

    def add_node_acl_binding(self, node, table_node, binding_name):
        if not binding_name in self.acl_bindings:
            raise ValueError("no acl binding called '{name}'".format(name=binding_name))
        binding = self.acl_bindings.get(binding_name)
        try:
            node.acl_bindings[binding_name] = self.expand_acl_binding(binding, table_node)
        except NoForeignKeyError as e:
            detail = ''
            if isinstance(node, ermrest_model.Column):
                detail = 'on column {n}'.format(n=node.name)
            elif isinstance(node, ermrest_model.ForeignKey):
                detail = 'on foreign key {s}.{n}'.format(s=node.names[0][0], n=node.names[0][1])
            else:
                detail = ' {t}'.format(t=type(node))
            print(
                "couldn't expand acl binding {b} {d} table {s}.{t}".format(b=binding_name, d=detail, s=table_node.schema.name,
                                                                           t=table_node.name))
            raise e

    def expand_acl_binding(self, binding, table_node):
        if not isinstance(binding, dict):
            return binding
        new_binding = dict()
        for k in binding.keys():
            if k == "projection":
                new_binding[k] = []
                for proj in binding.get(k):
                    new_binding[k].append(self.expand_projection(proj, table_node))
            elif k == "scope_acl":
                new_binding[k] = self.get_group(binding.get(k))
            else:
                new_binding[k] = binding[k]
        return new_binding

    def expand_projection(self, proj, table_node):
        if isinstance(proj, dict):
            new_proj = dict()
            is_first_outbound = True
            for k in proj.keys():
                if k == "outbound_col":
                    if is_first_outbound:
                        is_first_outbound = False
                    else:
                        raise NotImplementedError(
                            "don't know how to expand 'outbound_col' on anything but the first entry in a projection; "
                            "use 'outbound' instead")
                    if table_node is None:
                        raise NotImplementedError(
                            "don't know how to expand 'outbound_col' in a foreign key acl/annotation; use 'outbound' "
                            "instead")
                    new_proj["outbound"] = self.expand_projection_column(proj[k], table_node)
                    if new_proj["outbound"] is None:
                        return None
                else:
                    new_proj[k] = proj[k]
                    is_first_outbound = False
            return new_proj
        else:
            return proj

    def expand_projection_column(self, col_name, table_node):
        for fkey in table_node.foreign_keys:
            if len(fkey.foreign_key_columns) == 1:
                col = fkey.foreign_key_columns[0]
                if col.table.name == table_node.name and col.table.schema.name == table_node.schema.name and col.name == col_name:
                    return fkey.names[0]
        raise NoForeignKeyError("can't find foreign key for column %I.%I(%I)", table_node.schema.name, table_node.name,
                                col_name)

    def set_node_acl_bindings(self, node, table_node, binding_list, invalidate_list):
        node.acl_bindings.clear()
        if binding_list is not None:
            for binding_name in binding_list:
                self.add_node_acl_binding(node, table_node, binding_name)
        if invalidate_list is not None:
            for binding_name in invalidate_list:
                if binding_list and binding_name in binding_list:
                    raise ValueError(
                        "Binding {b} appears in both acl_bindings and invalidate_bindings for table {s}.{t} node {n}".format(
                            b=binding_name, s=table_node.schema.name, t=table_node.name, n=node.name))
                node.acl_bindings[binding_name] = False

    def save_groups(self):
        glt = self.create_or_validate_group_table()
        if glt is not None and self.groups is not None:
            rows = []
            for name in self.groups.keys():
                row = {'name': name, 'groups': self.groups.get(name)}
                for c in ['RCB', 'RMB']:
                    if glt.getColumn(c) is not None:
                        row[c] = None
                rows.append(row)

            glt.upsertRows(self.catalog, rows)

    def create_or_validate_schema(self, schema_name):
        schema = self.catalog.getCatalogSchema()['schemas'].get(schema_name)
        if schema is None:
            self.catalog.post("/schema/{s}".format(s=schema_name))
        return self.catalog.getCatalogSchema()['schemas'].get(schema_name)

    def create_table(self, schema_name, table_name, table_spec, comment=None):
        if table_spec is None:
            table_spec = dict()
        if schema_name is None:
            return None
        table_spec["schema_name"] = schema_name
        table_spec["table_name"] = table_name
        if table_spec.get('comment') is None and comment is not None:
            table_spec['comment'] = comment
        if table_spec.get('kind') is None:
            table_spec['kind'] = 'table'
        self.catalog.post("/schema/{s}/table".format(s=schema_name), json=table_spec)
        schema = self.catalog.getCatalogSchema()['schemas'].get(schema_name)
        return schema['tables'].get(table_name)

    def create_or_validate_group_table(self):
        glt_spec = self.config.get('group_list_table')
        if glt_spec is None:
            return None
        sname = glt_spec.get('schema')
        tname = glt_spec.get('table')
        if sname is None or tname is None:
            raise ValueError("group_list_table missing schema or table")
        schema = self.create_or_validate_schema(sname)
        assert schema is not None
        glt = Table(schema['tables'].get(tname))
        if glt == {}:
            glt_spec = ermrest_model.Table.define(
                tname,
                column_defs=[
                    ermrest_model.Column.define(
                        self.NC_NAME,
                        ermrest_model.builtin_types.text,
                        nullok=False,
                        comment='Name of grouplist, used in foreign keys. This table is maintained by the acl-config '
                                'program and should not be updated by hand.'
                    ),
                    ermrest_model.Column.define(
                        self.GC_NAME,
                        ermrest_model.builtin_types['text[]'],
                        nullok=True,
                        comment='List of groups. This table is maintained by the acl-config program and should not be '
                                'updated by hand.'
                    )
                ],
                key_defs=[
                    ermrest_model.Key.define(
                        [self.NC_NAME],
                        constraint_names=[[sname, "{t}_{c}_u".format(t=tname, c=self.NC_NAME)]]
                    )
                ],
                comment="Named lists of groups used in ACLs. Maintained by the acl-config program. Do not update this "
                        "table manually.",
                annotations={'tag:isrd.isi.edu,2016:generated': None}
            )
            glt = Table(self.create_table(sname, tname, glt_spec))

        else:
            name_col = glt.getColumn(self.NC_NAME)
            if name_col is None:
                raise ValueError(
                    'table specified for group lists ({s}.{t}) lacks a "{n}" column'.format(s=sname, t=tname,
                                                                                            n=self.NC_NAME))
            if name_col.get('nullok'):
                raise ValueError(
                    "{n} column in group list table ({s}.{t}) allows nulls".format(n=self.NC_NAME, s=sname, t=tname))

            nc_uniq = False
            for key in glt.get('keys'):
                cols = key.get('unique_columns')
                if len(cols) == 1 and cols[0] == self.NC_NAME:
                    nc_uniq = True
                    break
            if not nc_uniq:
                raise ValueError(
                    "{n} column in group list table ({s}.{t}) is not a key".format(n=self.NC_NAME, s=sname, t=tname))

            val_col = glt.getColumn(self.GC_NAME)
            if val_col is None:
                raise ValueError(
                    'table specified for group lists ({s}.{t}) lacks a "{n}" column'.format(s=sname, t=tname,
                                                                                            n=self.GC_NAME))
        if glt == {}:
            return None
        else:
            return glt

    def set_node_acl(self, node, spec):
        node.acls.clear()
        acl_name = spec.get("acl")
        if acl_name is not None:
            self.add_node_acl(node, acl_name)

    def expand_groups(self):
        for group_name in self.groups.keys():
            self.expand_group(group_name)

    def get_group(self, group_name):
        group = self.groups.get(group_name)
        if group is None:
            group = [group_name]
        return group

    def validate_group(self, group):
        if group == '*':
            return
        elif group.startswith(self.GLOBUS_PREFIX):
            self.validate_globus_group(group)
        elif group.startswith(self.ROBOT_PREFIX_FORMAT.format(server=self.server)):
            self.validate_webauthn_robot(group)
        else:
            warnings.warn("Can't determine format of group '{g}'".format(g=group))

    def validate_globus_group(self, group):
        guid = group[len(self.GLOBUS_PREFIX):]
        try:
            UUID(guid)
        except ValueError:
            raise ValueError("Group '{g}' appears to be a malformed Globus group".format(g=group))
        if self.verbose:
            print("group '{g}' appears to be a syntactically-correct Globus group".format(g=group))

    def validate_webauthn_robot(self, group):
        robot_name = group[len(self.ROBOT_PREFIX_FORMAT.format(server=self.server)):]
        if not robot_name:
            raise ValueError("Group '{g}' appears to be a malformed webauthn robot identity".format(g=group))
        if self.verbose:
            print("group '{g}' appears to be a syntactically-correct webauthn robot identity".format(g=group))

    def expand_group(self, group_name):
        groups = []
        for child_name in self.groups.get(group_name):
            child = self.groups.get(child_name)
            if child is None:
                self.validate_group(child_name)
                groups.append(child_name)
            else:
                self.expand_group(child_name)
                groups = groups + self.groups[child_name]
        self.groups[group_name] = list(set(groups))

    def expand_acl_definitions(self):
        for acl_name in self.acl_definitions.keys():
            self.expand_acl_definition(acl_name)

    def expand_acl_definition(self, acl_name):
        spec = self.acl_definitions.get(acl_name)
        for op_type in spec.keys():
            groups = []
            raw_groups = spec[op_type]
            if isinstance(raw_groups, list):
                for group_name in spec[op_type]:
                    groups = groups + self.get_group(group_name)
            else:
                groups = self.get_group(raw_groups)
            spec[op_type] = groups

    def set_table_acls(self, table):
        spec = self.acl_specs["table_acls"].find_best_table_spec(table.schema.name, table.name)
        table.acls.clear()
        table.acl_bindings.clear()
        if spec is not None:
            self.set_node_acl(table, spec)
            self.set_node_acl_bindings(table, table, spec.get("acl_bindings"), spec.get("invalidate_bindings"))
        if self.verbose:
            print(
                "set table {s}.{t} acls to {a}, bindings to {b}".format(s=table.schema.name, t=table.name, a=str(table.acls),
                                                                        b=str(table.acl_bindings)))
        for column in table.column_definitions:
            self.set_column_acls(column, table)
        for fkey in table.foreign_keys:
            self.set_fkey_acls(fkey, table)

    def set_column_acls(self, column, table):
        spec = self.acl_specs["column_acls"].find_best_column_spec(column.table.schema.name, column.table.name, column.name)
        column.acls.clear()
        column.acl_bindings.clear()
        if spec is not None:
            self.set_node_acl(column, spec)
            self.set_node_acl_bindings(column, table, spec.get("acl_bindings"), spec.get("invalidate_bindings"))
        if self.verbose:
            print("set column {s}.{t}.{c} acls to {a}, bindings to {b}".format(s=column.table.schema.name, t=column.table.name,
                                                                               c=column.name, a=str(column.acls),
                                                                               b=str(column.acl_bindings)))

    def set_fkey_acls(self, fkey, table):
        spec = self.acl_specs["foreign_key_acls"].find_best_foreign_key_spec(fkey.table.schema.name, fkey.table.name, fkey.names)
        fkey.acls.clear()
        fkey.acl_bindings.clear()
        if spec is not None:
            self.set_node_acl(fkey, spec)
            self.set_node_acl_bindings(fkey, table, spec.get("acl_bindings"), spec.get("invalidate_bindings"))
        if self.verbose:
            print("set fkey {f} acls to {a}, bindings to {b}".format(f=str(fkey.names), a=str(fkey.acls),
                                                                     b=str(fkey.acl_bindings)))

    def set_catalog_acls(self, catalog):
        spec = self.acl_specs["catalog_acl"]
        if spec is not None:
            catalog.acls.clear()
            self.set_node_acl(catalog, spec)
        if self.verbose:
            print("set catalog acls to {a}".format(a=str(catalog.acls)))
        for schema in self.toplevel_config.schemas.values():
            self.set_schema_acls(schema)

    def set_schema_acls(self, schema):
        for pattern in self.ignored_schema_patterns:
            if pattern.match(schema.name) is not None:
                print("ignoring schema {s}".format(s=schema.name))
                return
        spec = self.acl_specs["schema_acls"].find_best_schema_spec(schema.name)
        schema.acls.clear()
        if spec is not None:
            self.set_node_acl(schema, spec)
        if self.verbose:
            print("set schema {s} acls to {a}".format(s=schema.name, a=str(schema.acls)))

        for table in schema.tables.values():
            self.set_table_acls(table)

    def set_acls(self):
        if isinstance(self.toplevel_config, ermrest_model.Model):
            self.set_catalog_acls(self.toplevel_config)
        elif isinstance(self.toplevel_config, ermrest_model.Schema):
            self.set_schema_acls(self.toplevel_config)
        elif isinstance(self.toplevel_config, ermrest_model.Table):
            self.set_table_acls(self.toplevel_config)
        else:
            raise ValueError("toplevel config is a {t}".format(t=str(type(self.toplevel_config))))

    def apply_acls(self):
        self.toplevel_config.apply(self.saved_toplevel_config)

    def dumps(self):
        """Dump a serialized (string) representation of the config.
        """
        return json.dumps(self.toplevel_config.prejson(), indent=2)


class Table(AttrDict):
    ERMREST_DEFAULT_COLS = ["RID", "RCB", "RMB", "RCT", "RMT"]

    def __init__(self, d):
        if d is None:
            return
        self.base_entity_url = "/entity/{s}:{t}".format(s=d['schema_name'], t=d['table_name'])
        AttrDict.__init__(self, d)

    def getColumn(self, name):
        if self.get('column_definitions') is None:
            return None
        for c in self['column_definitions']:
            if c.get('name') == name:
                return c
        return None

    def getBaseEntityURL(self):
        return self.base_entity_url

    def upsertRows(self, catalog, rows):
        try:
            self.insertRows(catalog, rows)
        except HTTPError as err:
            if err.response.status_code == 409:
                for row in rows:
                    self.upsertRow(catalog, row)

    def find_keys(self):
        keys = self.get('keys')
        if keys is None:
            return keys
        for k in keys:
            for u in k.get('unique_columns'):
                c = self.getColumn(u)
                if c.get('nullok'):
                    keys.remove(k)
                    break
        return keys

    def row_has_key(self, row, key):
        for u in key.get('unique_columns'):
            if row.get(u) is None:
                return False
        return True

    def getRowFilter(self, row):
        filters = []
        key = None
        for k in self.find_keys():
            if self.row_has_key(row, k):
                key = k
                break

        if key is None:
            raise ValueError("can't find appropriate key")
        for k in key.get('unique_columns'):
            filters.append("{k}={v}".format(k=urlquote(k), v=urlquote(row[k])))
        return filters

    def getRow(self, catalog, row, filters):
        url = "{u}/{f}".format(u=self.getBaseEntityURL(), f="&".join(filters))
        vals = catalog.get(url, headers={'Content-Type': 'application/json'}).json()
        if vals is None or len(vals) == 0:
            return None
        return vals[0]

    def getDefaultCols(self, add_ermrest_defaults=True):
        default_cols = []
        for col in self.column_definitions:
            if col.get("default") is not None:
                default_cols.append(col.get("name"))
        if add_ermrest_defaults:
            default_cols = list(set(default_cols + self.ERMREST_DEFAULT_COLS))
        return default_cols

    def upsertRow(self, catalog, row):
        try:
            return self.insertRows(catalog, [row])
        except HTTPError as err:
            if err.response.status_code == 409:
                return self.updateRow(catalog, row)

    def updateRow(self, catalog, row):
        filters = self.getRowFilter(row)
        old_row = self.getRow(catalog, row, filters)
        for c in self.getDefaultCols():
            if row.get(c) is None and old_row.get(c) is not None:
                row[c] = old_row[c]
        return catalog.put(self.getBaseEntityURL(), json=[row], headers={'Content-Type': 'application/json'})

    def insertRows(self, catalog, rows):
        default_cols = self.getDefaultCols(False)
        if default_cols is not None and len(default_cols) != 0:
            url = "{u}?defaults={d}".format(u=self.getBaseEntityURL(), d=",".join(default_cols))
        else:
            url = self.getBaseEntityURL()
        return catalog.post(url, json=rows, headers={'Content-Type': 'application/json'})

    def __str__(self):
        return dict.__str__(self)


class AclCLI(ConfigBaseCLI):
    def __init__(self):
        ConfigBaseCLI.__init__(self, "ACL configuration tool", None, version=VERSION)
        group = self.parser.add_mutually_exclusive_group()
        group.add_argument('-g', '--groups-only', help="create group table only", action="store_true")
        group.add_argument('-o', '--omit-groups', help="do not create group table", action="store_true")


def main():
    cli = AclCLI()
    args = cli.parse_cli()
    table_name = cli.get_table_arg(args)
    schema_names = cli.get_schema_arg_list(args)
    credentials = get_credential(args.host, args.credential_file)
    save_groups = not (args.dryrun or args.omit_groups)
    for schema in schema_names:
        acl_config = AclConfig(args.host, args.catalog, args.config_file, credentials, schema_name=schema,
                               table_name=table_name, verbose=args.verbose or args.debug)

        try:
            if save_groups:
                acl_config.save_groups()
                save_groups = False
            if not args.groups_only:
                acl_config.set_acls()
                if not args.dryrun:
                    acl_config.apply_acls()
        except HTTPError as e:
            print(format_exception(e))
            raise

        if args.dryrun:
            print(acl_config.dumps())


if __name__ == '__main__':
    sys.exit(main())
