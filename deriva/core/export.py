"""Export functionality for ERMrest catalogs.

This module provides functions to export entities from ERMrest catalogs,
similar to the export button in Chaise. It supports both CSV/JSON file
exports and BDBag exports using export annotations or custom export specs.
"""

import logging
import os
import tempfile
from typing import Optional, Union

from .ermrest_catalog import ErmrestCatalog, DerivaServer
from .ermrest_model import Table
from .utils.core_utils import tag

logger = logging.getLogger(__name__)

# Export annotation tags
EXPORT_2016_TAG = "tag:isrd.isi.edu,2016:export"
EXPORT_2019_TAG = "tag:isrd.isi.edu,2019:export"


def _get_table_for_rid(catalog: ErmrestCatalog, rid: str) -> Table:
    """Resolve a RID to its table.

    Args:
        catalog: The ERMrest catalog binding.
        rid: The RID to resolve.

    Returns:
        The Table object containing the entity.

    Raises:
        KeyError: If the RID is not found.
    """
    result = catalog.resolve_rid(rid)
    return result.table


def _get_export_annotation(table: Table, context: str = "detailed") -> Optional[dict]:
    """Get the export annotation for a table.

    Checks for both 2019 (context-aware) and 2016 export annotations.

    Args:
        table: The table to get the export annotation from.
        context: The context to use for 2019 annotations (default: "detailed").

    Returns:
        The export annotation dict, or None if not found.
    """
    # Check for 2019 context-aware annotation first
    if EXPORT_2019_TAG in table.annotations:
        annotation = table.annotations[EXPORT_2019_TAG]
        # 2019 format can have context-specific entries or reference another context
        if context in annotation:
            entry = annotation[context]
            # Entry could be a string (reference to another context) or the actual annotation
            if isinstance(entry, str):
                entry = annotation.get(entry)
            return entry
        elif "*" in annotation:
            entry = annotation["*"]
            if isinstance(entry, str):
                entry = annotation.get(entry)
            return entry

    # Fall back to 2016 annotation
    if EXPORT_2016_TAG in table.annotations:
        return table.annotations[EXPORT_2016_TAG]

    return None


def _find_template(
    export_annotation: dict,
    template_name: Optional[str] = None,
    export_format: str = "bag"
) -> Optional[dict]:
    """Find an export template by name or format type.

    Args:
        export_annotation: The export annotation dict.
        template_name: Optional specific template name to find.
        export_format: The desired format type ("bag", "csv", "json").

    Returns:
        The matching template dict, or None if not found.
    """
    templates = export_annotation.get("templates", [])
    if not templates:
        return None

    # Map format to template type
    format_to_type = {
        "bag": "BAG",
        "csv": "FILE",
        "json": "FILE",
    }
    target_type = format_to_type.get(export_format, "BAG")

    for template in templates:
        # If template_name specified, match by displayname
        if template_name:
            if template.get("displayname") == template_name:
                return template
        else:
            # Match by type
            if template.get("type") == target_type:
                # For FILE type, prefer matching output type
                if target_type == "FILE" and export_format in ("csv", "json"):
                    outputs = template.get("outputs", [])
                    for output in outputs:
                        dest = output.get("destination", {})
                        if dest.get("type") == export_format:
                            return template
                else:
                    return template

    # If no exact match, return first template of the right type
    for template in templates:
        if template.get("type") == target_type:
            return template

    # Last resort: return first template
    return templates[0] if templates else None


def _template_to_download_config(
    template: dict,
    table: Table,
    rid: str,
    hostname: str,
    catalog_id: str,
    include_schema: bool = False,
) -> dict:
    """Convert an export template to a DerivaDownload config.

    Args:
        template: The export template from the annotation.
        table: The table containing the entity.
        rid: The RID of the entity to export.
        hostname: The server hostname.
        catalog_id: The catalog ID.
        include_schema: If True, include catalog schema in bag exports.

    Returns:
        A config dict suitable for DerivaDownload.
    """
    template_type = template.get("type", "BAG")
    outputs = template.get("outputs", [])

    # Build query processors from outputs
    query_processors = []

    schema_name = table.schema.name
    table_name = table.name

    for output in outputs:
        source = output.get("source", {})
        destination = output.get("destination", {})

        api = source.get("api", "entity")
        source_path = source.get("path", "")

        dest_name = destination.get("name", "output")
        dest_type = destination.get("type", "csv")
        dest_params = destination.get("params", {})

        # Build the query path
        # The path from the annotation is relative to the current entity
        # We need to construct the full path: /api/schema:table/RID=value/path
        base_path = f"/{api}/{schema_name}:{table_name}/RID={rid}"
        if source_path:
            # source_path may start with / or not
            if source_path.startswith("/"):
                query_path = base_path + source_path
            else:
                query_path = base_path + "/" + source_path
        else:
            query_path = base_path

        processor = {
            "processor": dest_type,
            "processor_params": {
                "query_path": query_path,
                "output_path": dest_name,
            }
        }

        # Add any additional params
        if dest_params:
            processor["processor_params"].update(dest_params)

        query_processors.append(processor)

    # If no outputs defined (BAG type can have no outputs), create a default
    if not query_processors:
        query_processors.append({
            "processor": "csv",
            "processor_params": {
                "query_path": f"/entity/{schema_name}:{table_name}/RID={rid}",
                "output_path": table_name,
            }
        })

    config = {
        "catalog": {
            "host": hostname,
            "catalog_id": catalog_id,
            "query_processors": query_processors,
        }
    }

    # Add bag config for BAG type
    if template_type == "BAG":
        displayname = template.get("displayname", "export")
        # Sanitize displayname for use in filename
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in displayname)
        config["bag"] = {
            "bag_name": f"{safe_name}_{rid}",
            "bag_archiver": "zip",
        }

        # Add schema export if requested
        if include_schema:
            # Insert schema processor at the beginning
            schema_processor = {
                "processor": "json",
                "processor_params": {
                    "query_path": "/schema",
                    "output_path": "schema.json",
                }
            }
            config["catalog"]["query_processors"].insert(0, schema_processor)

    return config


def _export_spec_to_download_config(
    export_spec: dict,
    table: Table,
    rid: str,
    hostname: str,
    catalog_id: str,
    include_schema: bool = False,
) -> dict:
    """Convert a user-provided export spec to a DerivaDownload config.

    The export spec can be in either the annotation format (with templates)
    or directly in the DerivaDownload config format.

    Args:
        export_spec: The export specification.
        table: The table containing the entity.
        rid: The RID of the entity to export.
        hostname: The server hostname.
        catalog_id: The catalog ID.
        include_schema: If True, include catalog schema in bag exports.

    Returns:
        A config dict suitable for DerivaDownload.
    """
    # Check if this is already in DerivaDownload format
    if "catalog" in export_spec and "query_processors" in export_spec.get("catalog", {}):
        # It's already in the right format, just substitute RID
        import copy
        config = copy.deepcopy(export_spec)
        # Substitute {RID} in query paths
        for processor in config["catalog"].get("query_processors", []):
            params = processor.get("processor_params", {})
            if "query_path" in params:
                params["query_path"] = params["query_path"].format(RID=rid)

        # Add schema if requested and this is a bag export
        if include_schema and "bag" in config:
            schema_processor = {
                "processor": "json",
                "processor_params": {
                    "query_path": "/schema",
                    "output_path": "schema.json",
                }
            }
            config["catalog"]["query_processors"].insert(0, schema_processor)

        return config

    # It's in annotation format - find and convert template
    if "templates" in export_spec:
        template = _find_template(export_spec)
        if template:
            return _template_to_download_config(
                template, table, rid, hostname, catalog_id, include_schema
            )

    # Single template provided directly
    if "type" in export_spec and "outputs" in export_spec:
        return _template_to_download_config(
            export_spec, table, rid, hostname, catalog_id, include_schema
        )

    raise ValueError("Invalid export_spec format. Must be either DerivaDownload config "
                     "format or export annotation format with templates.")


def export_entity(
    hostname: str,
    catalog_id: Union[str, int],
    rid: str,
    output_dir: Optional[str] = None,
    export_format: str = "bag",
    template_name: Optional[str] = None,
    export_spec: Optional[dict] = None,
    credentials: Optional[dict] = None,
    include_schema: bool = True,
) -> dict:
    """Export a single entity by RID, similar to Chaise export button.

    This function exports an entity from an ERMrest catalog using either:
    1. The export annotation defined on the table (default)
    2. A specific template from the annotation (via template_name)
    3. A custom export specification (via export_spec)

    Args:
        hostname: Server hostname (e.g., "www.facebase.org").
        catalog_id: Catalog ID (e.g., "1" or 1).
        rid: The RID of the entity to export.
        output_dir: Directory for output files. Defaults to current directory.
        export_format: Export format - "bag", "csv", or "json". Default is "bag".
        template_name: Optional name of specific export template to use.
        export_spec: Optional custom export specification. If provided, overrides
            the table's export annotation. Can be in either:
            - Export annotation format (with "templates" array)
            - DerivaDownload config format (with "catalog"/"query_processors")
        credentials: Optional credentials dict. If not provided, will attempt
            to load from credential store.
        include_schema: If True (default), include the catalog schema in bag exports.
            This is useful for understanding the data model of the exported data.

    Returns:
        A dict with export results:
        - "status": "success" or "error"
        - "path": Path to the exported file or bag
        - "format": The export format used
        - "rid": The exported RID
        - "table": The table name
        - "schema": The schema name

    Raises:
        KeyError: If the RID is not found in the catalog.
        ValueError: If no export template is found or export_spec is invalid.

    Example:
        # Export using table's default export annotation
        result = export_entity("www.facebase.org", "1", "3-KFBY")

        # Export as CSV using specific template
        result = export_entity(
            "www.facebase.org", "1", "3-KFBY",
            export_format="csv",
            template_name="CSV Export"
        )

        # Export using custom spec
        result = export_entity(
            "www.facebase.org", "1", "3-KFBY",
            export_spec={
                "catalog": {
                    "query_processors": [{
                        "processor": "csv",
                        "processor_params": {
                            "query_path": "/entity/isa:dataset/RID={RID}",
                            "output_path": "dataset"
                        }
                    }]
                }
            }
        )

        # Export without schema
        result = export_entity("www.facebase.org", "1", "3-KFBY", include_schema=False)
    """
    # Import here to avoid circular imports
    from deriva.transfer.download.deriva_download import DerivaDownload
    from deriva.core import get_credential as _get_credential

    # Set up output directory
    if output_dir is None:
        output_dir = os.getcwd()
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Get credentials if not provided
    if credentials is None:
        credentials = _get_credential(hostname)

    # Connect to catalog
    catalog_id = str(catalog_id)
    server = DerivaServer("https", hostname, credentials)
    catalog = server.connect_ermrest(catalog_id)

    # Resolve RID to table
    logger.info(f"Resolving RID {rid} in catalog {catalog_id} on {hostname}")
    table = _get_table_for_rid(catalog, rid)
    schema_name = table.schema.name
    table_name = table.name
    logger.info(f"RID {rid} resolved to {schema_name}:{table_name}")

    # Only include schema for bag exports
    should_include_schema = include_schema and export_format == "bag"

    # Determine the download config
    if export_spec is not None:
        # Use provided export spec
        logger.info("Using provided export_spec")
        config = _export_spec_to_download_config(
            export_spec, table, rid, hostname, catalog_id, should_include_schema
        )
    else:
        # Get export annotation from table
        export_annotation = _get_export_annotation(table)
        if export_annotation is None:
            raise ValueError(
                f"No export annotation found on table {schema_name}:{table_name}. "
                "Provide an export_spec parameter or add an export annotation to the table."
            )

        # Find the appropriate template
        template = _find_template(export_annotation, template_name, export_format)
        if template is None:
            raise ValueError(
                f"No suitable export template found in annotation for "
                f"format={export_format}, template_name={template_name}"
            )

        logger.info(f"Using export template: {template.get('displayname', 'unnamed')}")
        config = _template_to_download_config(
            template, table, rid, hostname, catalog_id, should_include_schema
        )

    # Execute the download
    logger.info(f"Starting export to {output_dir}")
    downloader = DerivaDownload(
        server={"host": hostname, "catalog_id": catalog_id},
        output_dir=output_dir,
        config=config,
        credentials=credentials,
    )

    outputs = downloader.download()

    # Get the output path
    if outputs:
        output_name = list(outputs.keys())[0]
        output_info = outputs[output_name]
        output_path = output_info.get("local_path", os.path.join(output_dir, output_name))
    else:
        output_path = output_dir

    return {
        "status": "success",
        "path": output_path,
        "format": export_format,
        "rid": rid,
        "table": table_name,
        "schema": schema_name,
    }
