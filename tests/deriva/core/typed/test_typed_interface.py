"""Tests for the typed interface module (deriva.core.typed).

These tests verify that the typed dataclass interfaces correctly convert to and
from the dict formats expected by the ERMrest API.
"""

import unittest

from deriva.core.typed import (
    # Types and enums
    BuiltinType,
    OnAction,
    AclMode,
    # Definition classes
    ColumnDef,
    KeyDef,
    ForeignKeyDef,
    TableDef,
    VocabularyTableDef,
    AssetTableDef,
    SchemaDef,
    # ACL classes
    Acl,
    AclBinding,
    AclBindings,
    # Annotation classes
    Tag,
    SortKey,
    ForeignKeyPath,
    SourceEntry,
    PseudoColumn,
    DisplayAnnotation,
    TableDisplayOptions,
    TableDisplayAnnotation,
    ColumnDisplayOptions,
    ColumnDisplayAnnotation,
    VisibleColumnsAnnotation,
    VisibleForeignKeysAnnotation,
    AssetAnnotation,
    ForeignKeyAnnotation,
    SourceDefinition,
    SourceDefinitionsAnnotation,
    CitationAnnotation,
)
from deriva.core.typed.types import TemplateEngine
from deriva.core.typed.column import text_column, int_column, float_column, bool_column, rid_column, boolean_column
from deriva.core.typed.key import rid_key, name_key, composite_key
from deriva.core.typed.foreign_key import simple_fkey, cascade_fkey, ermrest_client_fkey
from deriva.core.typed.table import simple_table, association_table
from deriva.core.typed.schema import domain_schema, ml_schema
from deriva.core.typed.acl import fkey_default_acls


class TestBuiltinTypes(unittest.TestCase):
    """Test the BuiltinType enum."""

    def test_to_ermrest_type(self):
        """Test conversion to ERMrest Type object."""
        from deriva.core.ermrest_model import Type
        type_obj = BuiltinType.text.to_ermrest_type()
        self.assertIsInstance(type_obj, Type)
        # Type objects have a prejson() method that returns a dict
        type_dict = type_obj.prejson()
        self.assertEqual(type_dict.get("typename"), "text")

    def test_from_typename(self):
        """Test creation from typename string."""
        self.assertEqual(BuiltinType.from_typename("text"), BuiltinType.text)
        self.assertEqual(BuiltinType.from_typename("int4"), BuiltinType.int4)
        self.assertEqual(BuiltinType.from_typename("float8"), BuiltinType.float8)

    def test_all_types_have_ermrest_mapping(self):
        """Test that all enum values can be converted to ERMrest types."""
        from deriva.core.ermrest_model import Type
        for builtin_type in BuiltinType:
            type_obj = builtin_type.to_ermrest_type()
            self.assertIsInstance(type_obj, Type)


class TestOnAction(unittest.TestCase):
    """Test the OnAction enum."""

    def test_values(self):
        """Test enum values match SQL syntax."""
        self.assertEqual(OnAction.NO_ACTION.value, "NO ACTION")
        self.assertEqual(OnAction.CASCADE.value, "CASCADE")
        self.assertEqual(OnAction.RESTRICT.value, "RESTRICT")
        self.assertEqual(OnAction.SET_NULL.value, "SET NULL")
        self.assertEqual(OnAction.SET_DEFAULT.value, "SET DEFAULT")


class TestColumnDef(unittest.TestCase):
    """Test the ColumnDef dataclass."""

    def test_defaults(self):
        """Test default values."""
        col = ColumnDef(name="test")
        self.assertEqual(col.name, "test")
        self.assertEqual(col.type, BuiltinType.text)
        self.assertEqual(col.nullok, True)
        self.assertIsNone(col.default)
        self.assertIsNone(col.comment)
        self.assertEqual(col.acls, {})
        self.assertEqual(col.acl_bindings, {})
        self.assertEqual(col.annotations, {})

    def test_to_dict(self):
        """Test conversion to dict format."""
        col = ColumnDef(
            name="Age",
            type=BuiltinType.int4,
            nullok=False,
            comment="Subject age in years",
        )
        col_dict = col.to_dict()

        self.assertEqual(col_dict.get("name"), "Age")
        self.assertEqual(col_dict.get("type", {}).get("typename"), "int4")
        self.assertEqual(col_dict.get("nullok"), False)
        self.assertEqual(col_dict.get("comment"), "Subject age in years")

    def test_from_dict(self):
        """Test creation from dict."""
        col_dict = {
            "name": "Weight",
            "type": {"typename": "float8"},
            "nullok": True,
            "default": 0.0,
            "comment": "Weight in kg",
            "acls": {"insert": ["*"]},
            "acl_bindings": {},
            "annotations": {"tag:test": "value"},
        }
        col = ColumnDef.from_dict(col_dict)

        self.assertEqual(col.name, "Weight")
        self.assertEqual(col.type, BuiltinType.float8)
        self.assertEqual(col.nullok, True)
        self.assertEqual(col.default, 0.0)
        self.assertEqual(col.comment, "Weight in kg")

    def test_with_annotation(self):
        """Test fluent annotation method."""
        col = ColumnDef(name="test")
        col2 = col.with_annotation("tag:test", "value")

        # Original unchanged
        self.assertEqual(col.annotations, {})
        # New has annotation
        self.assertEqual(col2.annotations, {"tag:test": "value"})

    def test_convenience_functions(self):
        """Test convenience factory functions."""
        text_col = text_column("Name")
        self.assertEqual(text_col.type, BuiltinType.text)
        self.assertEqual(text_col.nullok, True)  # Default is True

        int_col = int_column("Count")
        self.assertEqual(int_col.type, BuiltinType.int4)

        float_col = float_column("Score")
        self.assertEqual(float_col.type, BuiltinType.float8)

        bool_col = bool_column("Active")
        self.assertEqual(bool_col.type, BuiltinType.boolean)

        rid_col = rid_column("Reference")
        self.assertEqual(rid_col.type, BuiltinType.text)

        # Test with explicit nullok=False
        required_text = text_column("Required", nullok=False)
        self.assertEqual(required_text.nullok, False)


class TestKeyDef(unittest.TestCase):
    """Test the KeyDef dataclass."""

    def test_validation(self):
        """Test validation of key columns."""
        with self.assertRaises(ValueError):
            KeyDef(columns=[])

    def test_to_dict(self):
        """Test conversion to dict format."""
        key = KeyDef(columns=["Name"])
        key_dict = key.to_dict()

        self.assertEqual(key_dict.get("unique_columns"), ["Name"])

    def test_with_constraint_name(self):
        """Test key with explicit constraint name."""
        key = KeyDef(columns=["FirstName", "LastName"], constraint_name="person_name_key")
        key_dict = key.to_dict()

        self.assertEqual(key_dict.get("unique_columns"), ["FirstName", "LastName"])
        names = key_dict.get("names", [])
        self.assertTrue(len(names) > 0)
        self.assertEqual(names[0][1], "person_name_key")

    def test_from_dict(self):
        """Test creation from dict."""
        key_dict = {
            "unique_columns": ["Email"],
            "names": [["", "email_key"]],
            "comment": "Unique email constraint",
        }
        key = KeyDef.from_dict(key_dict)

        self.assertEqual(key.columns, ["Email"])
        self.assertEqual(key.constraint_name, "email_key")
        self.assertEqual(key.comment, "Unique email constraint")

    def test_convenience_functions(self):
        """Test convenience factory functions."""
        rid = rid_key()
        self.assertEqual(rid.columns, ["RID"])

        name = name_key()
        self.assertEqual(name.columns, ["Name"])

        composite = composite_key("FirstName", "LastName", "DOB")
        self.assertEqual(composite.columns, ["FirstName", "LastName", "DOB"])


class TestForeignKeyDef(unittest.TestCase):
    """Test the ForeignKeyDef dataclass."""

    def test_validation(self):
        """Test validation of foreign key."""
        with self.assertRaises(ValueError):
            ForeignKeyDef(
                columns=[],
                referenced_schema="domain",
                referenced_table="Parent",
            )

        with self.assertRaises(ValueError):
            ForeignKeyDef(
                columns=["A", "B"],
                referenced_schema="domain",
                referenced_table="Parent",
                referenced_columns=["X"],  # Length mismatch
            )

    def test_defaults(self):
        """Test default values."""
        fkey = ForeignKeyDef(
            columns=["Subject"],
            referenced_schema="domain",
            referenced_table="Subject",
        )

        self.assertEqual(fkey.referenced_columns, ["RID"])
        self.assertEqual(fkey.on_update, OnAction.NO_ACTION)
        self.assertEqual(fkey.on_delete, OnAction.NO_ACTION)

    def test_to_dict(self):
        """Test conversion to dict format."""
        fkey = ForeignKeyDef(
            columns=["Subject"],
            referenced_schema="domain",
            referenced_table="Subject",
            on_delete=OnAction.CASCADE,
        )
        fkey_dict = fkey.to_dict()

        fk_cols = fkey_dict.get("foreign_key_columns", [])
        self.assertEqual(len(fk_cols), 1)
        self.assertEqual(fk_cols[0].get("column_name"), "Subject")

        ref_cols = fkey_dict.get("referenced_columns", [])
        self.assertEqual(len(ref_cols), 1)
        self.assertEqual(ref_cols[0].get("schema_name"), "domain")
        self.assertEqual(ref_cols[0].get("table_name"), "Subject")
        self.assertEqual(ref_cols[0].get("column_name"), "RID")

        self.assertEqual(fkey_dict.get("on_delete"), "CASCADE")

    def test_from_dict(self):
        """Test creation from dict."""
        fkey_dict = {
            "foreign_key_columns": [{"column_name": "Parent"}],
            "referenced_columns": [
                {"schema_name": "domain", "table_name": "Node", "column_name": "RID"}
            ],
            "on_update": "CASCADE",
            "on_delete": "CASCADE",
            "names": [["", "child_parent_fkey"]],
        }
        fkey = ForeignKeyDef.from_dict(fkey_dict)

        self.assertEqual(fkey.columns, ["Parent"])
        self.assertEqual(fkey.referenced_schema, "domain")
        self.assertEqual(fkey.referenced_table, "Node")
        self.assertEqual(fkey.on_update, OnAction.CASCADE)
        self.assertEqual(fkey.on_delete, OnAction.CASCADE)
        self.assertEqual(fkey.constraint_name, "child_parent_fkey")

    def test_convenience_functions(self):
        """Test convenience factory functions."""
        simple = simple_fkey("Subject", "domain", "Subject")
        self.assertEqual(simple.columns, ["Subject"])
        self.assertEqual(simple.referenced_columns, ["RID"])

        cascade = cascade_fkey("Parent", "domain", "Node")
        self.assertEqual(cascade.on_update, OnAction.CASCADE)
        self.assertEqual(cascade.on_delete, OnAction.CASCADE)

        client = ermrest_client_fkey("Owner")
        self.assertEqual(client.referenced_schema, "public")
        self.assertEqual(client.referenced_table, "ERMrest_Client")
        self.assertEqual(client.referenced_columns, ["ID"])


class TestTableDef(unittest.TestCase):
    """Test the TableDef dataclass."""

    def test_defaults(self):
        """Test default values."""
        table = TableDef(name="Test")

        self.assertEqual(table.name, "Test")
        self.assertEqual(table.columns, [])
        self.assertEqual(table.keys, [])
        self.assertEqual(table.foreign_keys, [])
        self.assertEqual(table.provide_system, True)
        self.assertEqual(table.provide_system_fkeys, True)

    def test_to_dict(self):
        """Test conversion to dict format."""
        table = TableDef(
            name="Subject",
            columns=[
                ColumnDef("Name", BuiltinType.text, nullok=False),
                ColumnDef("Age", BuiltinType.int4),
            ],
            keys=[KeyDef(["Name"])],
            comment="Study subjects",
        )
        table_dict = table.to_dict()

        self.assertEqual(table_dict.get("table_name"), "Subject")
        self.assertEqual(table_dict.get("comment"), "Study subjects")

        # System columns should be added
        col_names = {c["name"] for c in table_dict.get("column_definitions", [])}
        self.assertIn("Name", col_names)
        self.assertIn("Age", col_names)
        self.assertIn("RID", col_names)
        self.assertIn("RCT", col_names)

    def test_with_column(self):
        """Test fluent column method."""
        table = TableDef(name="Test")
        table2 = table.with_column(ColumnDef("Name", BuiltinType.text))

        self.assertEqual(len(table.columns), 0)
        self.assertEqual(len(table2.columns), 1)

    def test_with_foreign_key(self):
        """Test fluent foreign key method."""
        table = TableDef(name="Test")
        fkey = ForeignKeyDef(
            columns=["Subject"],
            referenced_schema="domain",
            referenced_table="Subject",
        )
        table2 = table.with_foreign_key(fkey)

        self.assertEqual(len(table.foreign_keys), 0)
        self.assertEqual(len(table2.foreign_keys), 1)

    def test_simple_table(self):
        """Test simple_table convenience function."""
        table = simple_table(
            "Person",
            [ColumnDef("Name", BuiltinType.text)],
            comment="People",
        )

        self.assertEqual(table.name, "Person")
        self.assertEqual(table.comment, "People")


class TestVocabularyTableDef(unittest.TestCase):
    """Test the VocabularyTableDef dataclass."""

    def test_to_dict(self):
        """Test conversion to dict format."""
        vocab = VocabularyTableDef(
            name="Diagnosis_Type",
            curie_template="MYPROJECT:{RID}",
            comment="Types of diagnoses",
        )
        vocab_dict = vocab.to_dict()

        self.assertEqual(vocab_dict.get("table_name"), "Diagnosis_Type")

        # Standard vocab columns should be present
        col_names = {c["name"] for c in vocab_dict.get("column_definitions", [])}
        self.assertIn("ID", col_names)
        self.assertIn("URI", col_names)
        self.assertIn("Name", col_names)
        self.assertIn("Description", col_names)


class TestAssetTableDef(unittest.TestCase):
    """Test the AssetTableDef dataclass."""

    def test_to_dict(self):
        """Test conversion to dict format."""
        asset = AssetTableDef(
            schema_name="domain",
            name="Image",
            columns=[
                ColumnDef("Width", BuiltinType.int4),
                ColumnDef("Height", BuiltinType.int4),
            ],
            comment="Image assets",
        )
        asset_dict = asset.to_dict()

        self.assertEqual(asset_dict.get("table_name"), "Image")

        # Standard asset columns should be present
        col_names = {c["name"] for c in asset_dict.get("column_definitions", [])}
        self.assertIn("URL", col_names)
        self.assertIn("Filename", col_names)
        self.assertIn("Length", col_names)
        self.assertIn("MD5", col_names)
        self.assertIn("Width", col_names)
        self.assertIn("Height", col_names)


class TestSchemaDef(unittest.TestCase):
    """Test the SchemaDef dataclass."""

    def test_defaults(self):
        """Test default values."""
        schema = SchemaDef(name="test")

        self.assertEqual(schema.name, "test")
        self.assertIsNone(schema.tables)
        self.assertIsNone(schema.comment)
        self.assertEqual(schema.acls, {})
        self.assertEqual(schema.annotations, {})

    def test_to_dict(self):
        """Test conversion to dict format."""
        schema = SchemaDef(
            name="domain",
            comment="Domain schema",
            acls={"insert": ["*"]},
        )
        schema_dict = schema.to_dict()

        self.assertEqual(schema_dict.get("schema_name"), "domain")
        self.assertEqual(schema_dict.get("comment"), "Domain schema")
        self.assertEqual(schema_dict.get("acls"), {"insert": ["*"]})

    def test_with_table(self):
        """Test fluent table method."""
        schema = SchemaDef(name="test")
        table = TableDef(name="Person")
        schema2 = schema.with_table(table)

        self.assertIsNone(schema.tables)
        self.assertIn("Person", schema2.tables)

    def test_convenience_functions(self):
        """Test convenience factory functions."""
        domain = domain_schema()
        self.assertEqual(domain.name, "domain")

        ml = ml_schema()
        self.assertEqual(ml.name, "deriva-ml")


class TestAcl(unittest.TestCase):
    """Test the Acl dataclass."""

    def test_to_dict(self):
        """Test conversion to dict format."""
        acl = Acl(
            owner=["admin"],
            enumerate=["*"],
            select=["researchers"],
        )
        acl_dict = acl.to_dict()

        self.assertEqual(acl_dict.get("owner"), ["admin"])
        self.assertEqual(acl_dict.get("enumerate"), ["*"])
        self.assertEqual(acl_dict.get("select"), ["researchers"])
        self.assertNotIn("insert", acl_dict)  # None values excluded

    def test_from_dict(self):
        """Test creation from dict."""
        acl_dict = {
            "owner": ["admin"],
            "select": ["*"],
        }
        acl = Acl.from_dict(acl_dict)

        self.assertEqual(acl.owner, ["admin"])
        self.assertEqual(acl.select, ["*"])
        self.assertIsNone(acl.insert)

    def test_public_read(self):
        """Test public_read factory method."""
        acl = Acl.public_read(owner=["admin"])

        self.assertEqual(acl.owner, ["admin"])
        self.assertEqual(acl.enumerate, ["*"])
        self.assertEqual(acl.select, ["*"])

    def test_read_only(self):
        """Test read_only factory method."""
        acl = Acl.read_only(owner=["admin"])

        self.assertEqual(acl.insert, [])
        self.assertEqual(acl.update, [])
        self.assertEqual(acl.delete, [])

    def test_restricted(self):
        """Test restricted factory method."""
        acl = Acl.restricted(owner=["admin"], allowed=["researchers"])

        self.assertEqual(acl.owner, ["admin"])
        self.assertEqual(acl.select, ["researchers"])
        self.assertEqual(acl.insert, ["researchers"])


class TestAclBinding(unittest.TestCase):
    """Test the AclBinding dataclass."""

    def test_to_dict(self):
        """Test conversion to dict format."""
        binding = AclBinding(
            projection="Owner",
            projection_type="acl",
            types=["owner"],
        )
        binding_dict = binding.to_dict()

        self.assertEqual(binding_dict.get("projection"), "Owner")
        self.assertEqual(binding_dict.get("projection_type"), "acl")
        self.assertEqual(binding_dict.get("types"), ["owner"])

    def test_self_service(self):
        """Test self_service factory method."""
        binding = AclBinding.self_service()

        self.assertEqual(binding.projection, "RCB")
        self.assertEqual(binding.types, ["owner"])


class TestAclBindings(unittest.TestCase):
    """Test the AclBindings dataclass."""

    def test_to_dict(self):
        """Test conversion to dict format."""
        bindings = AclBindings(bindings={
            "self_service": AclBinding.self_service(),
        })
        bindings_dict = bindings.to_dict()

        self.assertIn("self_service", bindings_dict)
        self.assertEqual(bindings_dict["self_service"]["projection"], "RCB")

    def test_add(self):
        """Test add method."""
        bindings = AclBindings()
        bindings2 = bindings.add("self_service", AclBinding.self_service())

        self.assertEqual(len(bindings.bindings), 0)
        self.assertEqual(len(bindings2.bindings), 1)


class TestSortKey(unittest.TestCase):
    """Test the SortKey dataclass."""

    def test_ascending(self):
        """Test ascending sort key."""
        key = SortKey(column="Name")
        result = key.to_dict()

        self.assertEqual(result, "Name")

    def test_descending(self):
        """Test descending sort key."""
        key = SortKey(column="Date", descending=True)
        result = key.to_dict()

        self.assertEqual(result, {"column": "Date", "descending": True})


class TestForeignKeyPath(unittest.TestCase):
    """Test the ForeignKeyPath dataclass."""

    def test_validation(self):
        """Test validation."""
        with self.assertRaises(ValueError):
            ForeignKeyPath()  # Neither inbound nor outbound

        with self.assertRaises(ValueError):
            ForeignKeyPath(
                inbound=["schema", "fkey1"],
                outbound=["schema", "fkey2"],
            )

    def test_inbound(self):
        """Test inbound path."""
        path = ForeignKeyPath(inbound=["domain", "image_subject_fkey"])
        result = path.to_dict()

        self.assertEqual(result, {"inbound": ["domain", "image_subject_fkey"]})

    def test_outbound(self):
        """Test outbound path."""
        path = ForeignKeyPath(outbound=["domain", "subject_group_fkey"])
        result = path.to_dict()

        self.assertEqual(result, {"outbound": ["domain", "subject_group_fkey"]})


class TestSourceEntry(unittest.TestCase):
    """Test the SourceEntry dataclass."""

    def test_simple_column(self):
        """Test simple column source."""
        source = SourceEntry(column="Name")
        result = source.to_dict()

        self.assertEqual(result, "Name")

    def test_path_source(self):
        """Test path-based source."""
        source = SourceEntry(
            column="Name",
            path=[
                ForeignKeyPath(outbound=["domain", "subject_group_fkey"]),
            ],
        )
        result = source.to_dict()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {"outbound": ["domain", "subject_group_fkey"]})
        self.assertEqual(result[1], "Name")

    def test_from_dict_simple(self):
        """Test parsing simple column."""
        source = SourceEntry.from_dict("Name")

        self.assertEqual(source.column, "Name")
        self.assertEqual(source.path, [])

    def test_from_dict_path(self):
        """Test parsing path source."""
        source = SourceEntry.from_dict([
            {"outbound": ["domain", "fkey"]},
            "Name",
        ])

        self.assertEqual(source.column, "Name")
        self.assertEqual(len(source.path), 1)


class TestDisplayAnnotation(unittest.TestCase):
    """Test the DisplayAnnotation dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        display = DisplayAnnotation(
            name="Subject",
            markdown_name="**Subject**",
        )
        result = display.to_dict()

        self.assertEqual(result.get("name"), "Subject")
        self.assertEqual(result.get("markdown_name"), "**Subject**")

    def test_from_dict(self):
        """Test creation from dict."""
        display = DisplayAnnotation.from_dict({
            "name": "Test",
            "show_null": "",
        })

        self.assertEqual(display.name, "Test")
        self.assertEqual(display.show_null, "")


class TestTableDisplayAnnotation(unittest.TestCase):
    """Test the TableDisplayAnnotation dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        annotation = TableDisplayAnnotation(
            contexts={
                "row_name": TableDisplayOptions(
                    row_markdown_pattern="{{{Name}}}",
                    template_engine=TemplateEngine.handlebars,
                ),
            }
        )
        result = annotation.to_dict()

        self.assertIn("row_name", result)
        self.assertEqual(
            result["row_name"]["row_markdown_pattern"],
            "{{{Name}}}",
        )

    def test_set_context(self):
        """Test fluent set_context method."""
        annotation = TableDisplayAnnotation()
        annotation2 = annotation.set_context(
            "detailed",
            TableDisplayOptions(page_size=25),
        )

        self.assertEqual(len(annotation.contexts), 0)
        self.assertIn("detailed", annotation2.contexts)


class TestVisibleColumnsAnnotation(unittest.TestCase):
    """Test the VisibleColumnsAnnotation dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        annotation = VisibleColumnsAnnotation(
            contexts={
                "compact": ["RID", "Name", "Description"],
                "detailed": ["RID", "Name", "Description", "Created"],
            }
        )
        result = annotation.to_dict()

        self.assertEqual(result["compact"], ["RID", "Name", "Description"])
        self.assertEqual(len(result["detailed"]), 4)

    def test_with_pseudo_column(self):
        """Test with pseudo-column."""
        pseudo = PseudoColumn(
            source=SourceEntry(column="Subject", path=[
                ForeignKeyPath(outbound=["domain", "subject_fkey"]),
            ]),
            markdown_name="Subject Name",
        )
        annotation = VisibleColumnsAnnotation(
            contexts={
                "compact": ["RID", pseudo],
            }
        )
        result = annotation.to_dict()

        self.assertEqual(len(result["compact"]), 2)
        self.assertEqual(result["compact"][0], "RID")
        self.assertIsInstance(result["compact"][1], dict)


class TestAssetAnnotation(unittest.TestCase):
    """Test the AssetAnnotation dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        asset = AssetAnnotation(
            url_pattern="/hatrac/domain/images/{{{MD5}}}",
            filename_column="Filename",
            byte_count_column="Length",
            md5=True,
        )
        result = asset.to_dict()

        self.assertEqual(result["url_pattern"], "/hatrac/domain/images/{{{MD5}}}")
        self.assertEqual(result["filename_column"], "Filename")
        self.assertEqual(result["md5"], True)

    def test_from_dict(self):
        """Test creation from dict."""
        asset = AssetAnnotation.from_dict({
            "url_pattern": "/hatrac/test/{{{MD5}}}",
            "md5": "MD5_Column",
        })

        self.assertEqual(asset.url_pattern, "/hatrac/test/{{{MD5}}}")
        self.assertEqual(asset.md5, "MD5_Column")


class TestSourceDefinitionsAnnotation(unittest.TestCase):
    """Test the SourceDefinitionsAnnotation dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        annotation = SourceDefinitionsAnnotation(
            columns=True,
            fkeys=True,
            sources={
                "subject_name": SourceDefinition(
                    source=SourceEntry(column="Name", path=[
                        ForeignKeyPath(outbound=["domain", "subject_fkey"]),
                    ]),
                    markdown_name="Subject",
                ),
            },
        )
        result = annotation.to_dict()

        self.assertEqual(result["columns"], True)
        self.assertEqual(result["fkeys"], True)
        self.assertIn("subject_name", result["sources"])

    def test_add_source(self):
        """Test add_source method."""
        annotation = SourceDefinitionsAnnotation()
        annotation2 = annotation.add_source(
            "test",
            SourceDefinition(source=SourceEntry(column="Name")),
        )

        self.assertEqual(len(annotation.sources), 0)
        self.assertEqual(len(annotation2.sources), 1)


class TestTagConstants(unittest.TestCase):
    """Test the Tag class constants."""

    def test_tag_values(self):
        """Test that tag URIs are correct."""
        self.assertEqual(Tag.display, "tag:misd.isi.edu,2015:display")
        self.assertEqual(Tag.visible_columns, "tag:isrd.isi.edu,2016:visible-columns")
        self.assertEqual(Tag.asset, "tag:isrd.isi.edu,2017:asset")


class TestFkeyDefaultAcls(unittest.TestCase):
    """Test the fkey_default_acls function."""

    def test_default_fkey_acls(self):
        """Test foreign key default ACLs."""
        acl = fkey_default_acls()

        self.assertEqual(acl.insert, ["*"])
        self.assertEqual(acl.update, ["*"])


if __name__ == "__main__":
    unittest.main()
