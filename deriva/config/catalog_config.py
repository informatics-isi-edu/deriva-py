"""DERIVA catalog configuration utilities."""

import logging
from deriva.core import tag

logger = logging.getLogger(__name__)


def configure_baseline_ermrest_client(model):
    """Baseline configuration of `ERMrest_Client` table.

    Set up `ERMrest_Client` table so that it has readable names and uses the display name of the user as the row
    name. To apply changes, `Model.apply()` must be invoked after using this method.

    :param model: an ermrest model object.
    """
    ermrest_client = model.schemas['public'].tables['ERMrest_Client']

    # Make ermrest_client table visible.  If the GUID or member name is considered sensitive, then this needs to be
    # changed.
    ermrest_client.acls['select'] = ['*']

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


def configure_baseline_ermrest_group(model):
    """Baseline configuration of `ERMrest_Group` table.

    Set up `ERMrest_Group` table so that it has readable names and uses the display name of the group as the row
    name. To apply changes, `Model.apply()` must be invoked after using this method.

    :param model: an ermrest model object.
    """
    ermrest_group = model.schemas['public'].tables['ERMrest_Group']

    # Make ERMrest_Group table visible. If the GUID or group name is considered sensitive, then this needs to be
    # changed.
    ermrest_group.acls['select'] = ['*']

    # Set table and row name.
    ermrest_group.annotations.update({
        tag.display: {'name': 'User Groups'},
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
