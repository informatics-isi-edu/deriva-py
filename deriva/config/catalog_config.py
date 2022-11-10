"""DERIVA catalog configuration utilities."""

import logging
from deriva.core import tag, urlquote
from deriva.core.ermrest_model import Schema

logger = logging.getLogger(__name__)


def configure_baseline_ermrest_client(model, apply=True):
    """Baseline configuration of `ERMrest_Client` table.

    Set up `ERMrest_Client` table so that it has readable names and uses the display name of the user as the row
    name.

    :param model: an ermrest model object.
    :param apply: if true, apply configuration changes before returning.
    """
    ermrest_client = model.schemas['public'].tables['ERMrest_Client']

    # Set table and row name.
    ermrest_client.annotations.update({
        tag.display: {'name': 'User'},
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
        model.apply()


def configure_baseline_ermrest_group(model, apply=True):
    """Baseline configuration of `ERMrest_Group` table.

    Set up `ERMrest_Group` table so that it has readable names and uses the display name of the group as the row
    name.

    :param model: an ermrest model object.
    :param apply: if true, apply configuration changes before returning.
    """
    ermrest_group = model.schemas['public'].tables['ERMrest_Group']

    # Set table and row name.
    ermrest_group.annotations.update({
        tag.display: {'name': 'User Group'},
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
        model.apply()


def configure_baseline_catalog(model, apply=True):
    """A baseline catalog configuration.

    Update catalog to a baseline configuration:
    1. Setting default display mode to be to turn underscores to spaces.
    2. Configure `ERMrest_Client` and `ERMrest_Group` to have readable names.
    3. Create a schema called *WWW* and create a *Page* table in that schema
       configured to display web-page like content.
    4. Configure a basic navbar with links to all tables.

    Afterwards, an ACL configuration should be applied to the catalog. See the
    `deriva.config.examples` package data for a `self_serve_policy.json`
    template.

    :param model: an ermrest model object.
    :param apply: if true, apply configuration changes before returning.
    """
    # Configure baseline public schema
    configure_baseline_ermrest_client(model, apply=False)
    configure_baseline_ermrest_group(model, apply=False)

    # Create WWW schema
    if "WWW" not in model.schemas:
        model.create_schema(Schema.define_www("WWW"))

    # Configure baseline annotations
    model.annotations.update({
        # Set up catalog-wide name style
        tag.display: {'name_style': {'underline_space': True}},
        # Set up default chaise config
        tag.chaise_config: {
            "headTitle": "DERIVA",
            "navbarBrandText": "DERIVA",
            "navbarMenu": {
                "newTab": False,
                "children": [
                    {
                        "name": s.annotations.get(tag.display, {}).get('name', s.name.replace('_', ' ')),
                        "children": [
                            {
                                "name": t.annotations.get(tag.display, {}).get('name', t.name.replace('_', ' ')),
                                "url": f'/chaise/recordset/#{model.catalog.catalog_id}/{urlquote(s.name)}:{urlquote(t.name)}'
                            } for t in s.tables.values() if not t.is_association()
                        ]
                    } for s in model.schemas.values()
                ]

            },
            "systemColumnsDisplayCompact": ["RID"],
            "systemColumnsDisplayEntry": ["RID"]
        }
    })

    if apply:
        # Apply model changes
        model.apply()
