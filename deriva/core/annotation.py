"""Definitions and implementation for validating ERMrest schema annotations."""

import json
import logging
import pkgutil
import jsonschema
import warnings

from .. import core
from . import tag

logger = logging.getLogger(__name__)


class _AnnotationSchemas (object):
    """Container of annotation schemas."""

    # TODO: inherit from 'collections.abc.Mapping'

    def __init__(self):
        super(_AnnotationSchemas, self).__init__()
        self._abbrev = dict((v, k) for k, v in tag.items())
        self._schemas = {}

    def __getitem__(self, key):
        try:
            return self._schemas[key]
        except KeyError:
            abbrev = self._abbrev[key]  # let this raise a 'KeyError' if the tag name is unknown
            s = pkgutil.get_data(core.__name__, 'schemas/%s.schema.json' % abbrev).decode()
            v = json.loads(s)
            self._schemas[key] = v
            return v

    def __iter__(self):
        return iter(self._schemas)

    def __len__(self):
        return len(self._schemas)


_schemas = _AnnotationSchemas()
_schema_store = {}
try:
    _schema_store = {  # the schema store for the schema resolver, stores all schemas that are extended
        _schemas[tag_name]['$id']: _schemas[tag_name] for tag_name in [tag.export, tag.source_definitions]
    }
except FileNotFoundError as e:
    warnings.warn("Unable to read base schemas. Error: %s" % e)


def _nop(validator, value, instance, schema):
    """NOP validator function."""
    return


def validate(model_obj, tag_name=None, validate_model_names=True):
    """Validate the annotation(s) of the model object.

    :param model_obj: model object container of annotations
    :param tag_name: tag name of the annotation to validate, if none, will validate all known annotations
    :param validate_model_names: validate model names used in annotations
    :return: a list of validation errors, if any
    """
    assert hasattr(model_obj, 'annotations')
    if not tag_name:
        errors = []
        for tag_name in model_obj.annotations:
            errors.extend(_validate(model_obj, tag_name, validate_model_names))
        return errors
    elif tag_name in model_obj.annotations:
        return _validate(model_obj, tag_name, validate_model_names)
    else:
        return []


def _validate(model_obj, tag_name, validate_model_names):
    """Validate an annotation of the model object.

    :param model_obj: model object container of annotations
    :param tag_name: tag name of the annotation to validate
    :param validate_model_names: validate model names used in annotations
    :return: a list of validation errors, if any
    """
    assert tag_name in model_obj.annotations
    logger.debug("Validating '%s' against schema for '%s'" % (_printable_name(model_obj), tag_name))
    try:
        schema = _schemas[tag_name]
        resolver = jsonschema.RefResolver.from_schema(schema, store=_schema_store)
        if validate_model_names:
            ExtendedValidator = jsonschema.validators.extend(
                jsonschema.Draft7Validator,
                {
                    'valid-table': _validate_table_fn(model_obj),
                    'valid-column': _validate_column_fn(model_obj),
                    'valid-constraint': _validate_constraint_fn(model_obj),
                    'valid-source-key': _validate_source_key_fn(model_obj),
                    'valid-source-path': _validate_source_path_fn(model_obj)
                }
            )
            validator = ExtendedValidator(schema, resolver=resolver)
            validator.validate(model_obj.annotations[tag_name])
        else:
            jsonschema.validate(model_obj.annotations[tag_name], schema, resolver=resolver)
    except jsonschema.ValidationError as e:
        logger.error("Failed to validate '%s' for annotation '%s'" % (_printable_name(model_obj), tag_name))
        logger.error(e)
        return [e]
    except KeyError as e:
        logger.error("Failed to validate '%s' for annotation '%s'" % (_printable_name(model_obj), tag_name))
        msg = 'Unknown annotation tag name "%s"' % tag_name
        logger.error(msg)
        return [jsonschema.ValidationError(msg, cause=e)]
    except FileNotFoundError as e:
        logger.warning('No schema document found for tag name %s : %s' % (tag_name, e))
    return []


def _printable_name(model_obj):
    """Returns a print-frendly name for a model object.

    :param model_obj: a model, schema, table, column, key, or foreign key object.
    :return: string representation of its name or "catalog" if no name found
    """
    if not hasattr(model_obj, 'name'):
        return 'catalog'
    if hasattr(model_obj, 'constraint_name'):
        return "%s:%s" % model_obj.name
    if hasattr(model_obj, 'table'):
        return "%s:%s:%s" % (model_obj.table.schema.name, model_obj.table.name, model_obj.name)
    if hasattr(model_obj, 'schema'):
        return "%s:%s" % (model_obj.schema.name, model_obj.name)
    if hasattr(model_obj, 'name'):
        return model_obj.name
    return 'unnamed model object'


def _is_qualified_name(value):
    """Tests if the given value looks like a schema-qualified name."""
    return isinstance(value, list) and len(value) == 2 and all(isinstance(item, str) for item in value)


def _validate_column_fn(model_obj):
    """Produces a column name validation function for the model object

    :param model_obj: expects column object
    :return: validation function
    """
    if hasattr(model_obj, 'table'):
        model_obj = model_obj.table
    if not hasattr(model_obj, 'column_definitions'):
        return _nop

    def _validation_func(validator, value, instance, schema):
        if not (value and isinstance(instance, str)):
            return

        try:
            model_obj.column_definitions[instance]
        except KeyError as e:
            raise jsonschema.ValidationError("'%s' not found in column definitions" % instance, cause=e,
                                             validator=validator, validator_value=value,
                                             instance=instance, schema=schema)

    return _validation_func


def _validate_table_fn(model_obj):
    """Produces a table name validation function for the model object

    :param model_obj: expects table object
    :return: validation function
    """
    if not hasattr(model_obj, 'schema'):
        return _nop

    def _validation_func(validator, value, instance, schema):
        if not (value and _is_qualified_name(instance)):
            return

        try:
            model_obj.schema.model.schemas[instance[0]].tables[instance[1]]
        except KeyError as e:
            raise jsonschema.ValidationError("Table %s not found in model" % instance, cause=e,
                                             validator=validator, validator_value=value,
                                             instance=instance, schema=schema)

    return _validation_func


def _validate_constraint_fn(model_obj):
    """Produces a constraint validation function for the model object.

    The directive takes a list value that may include one or all of the following strings:
     - 'inbound': inbound fkey relationships are valid,
     - 'outbound': outbound fkey relationships are valid
     - 'key': keys of the object are valid

    :param model_obj: table object
    :return: validation function
    """
    if not hasattr(model_obj, 'schema'):
        return _nop

    table = model_obj
    model = table.schema.model

    # define a validation function for the model object
    def _validation_func(validator, value, instance, schema):
        if not value or not _is_qualified_name(instance):
            return

        # validate if key
        if 'key' in value:
            try:
                schemaobj = model.schemas[instance[0]]
                key = table.keys[(schemaobj, instance[1])]
                return  # return immediately on validation condition
            except KeyError as e:
                # when no other validation options exist, terminate and raise exception
                if len(value) == 1:
                    raise jsonschema.ValidationError("%s not found in keys of '%s'" % (instance, table.name), cause=e,
                                                     validator=validator, validator_value=value,
                                                     instance=instance, schema=schema)

        # if not already validated as a key, then validate as inbound or outbound fkey
        try:
            if not (
                ('outbound' in value and model.fkey(instance).table == table) or
                ('inbound' in value and model.fkey(instance).pk_table == table)
            ):
                raise jsonschema.ValidationError("%s not related to '%s'" % (instance, table.name),
                                                 validator=validator, validator_value=value,
                                                 instance=instance, schema=schema)
            return
        except KeyError as e:
            raise jsonschema.ValidationError("unable to validate constraint %s" % instance, cause=e,
                                             validator=validator, validator_value=value,
                                             instance=instance, schema=schema)

    return _validation_func


def _validate_source_key_fn(model_obj):
    """Produces a source key validation function for the model object.

    :param model_obj: model object
    :return: validation function
    """
    sourcekeys = model_obj.annotations.get(tag.source_definitions, {}).get('sources', {})

    # define a validation function for the model object
    def _validation_func(validator, value, instance, schema):
        if value and isinstance(instance, str) and instance not in sourcekeys:
            raise jsonschema.ValidationError("'%s' not found in source definitions" % instance,
                                             validator=validator, validator_value=value, instance=instance, schema=schema)

    return _validation_func


def _validate_source_path_fn(model_obj):
    """Produces a source path validation function for the model object.

    :param model_obj: model object
    :return: validation function
    """
    if not (hasattr(model_obj, 'column_definitions') and hasattr(model_obj, 'schema')):
        return _nop

    _model = model_obj.schema.model

    # define a validation function for the model object
    def _validation_func(validator, value, instance, schema):
        if not value:  # 'true' to indicate desire to validate model
            return

        # validate the fkey path case
        if isinstance(instance, list):
            current_table = model_obj  # start the "current" table in the fkey path
            # iterate over the instance of the fkey path
            for item in instance:
                # validate that a column name belongs to the current table
                if isinstance(item, str):
                    if item not in {c.name for c in current_table.column_definitions}:
                        raise jsonschema.ValidationError(
                            "'%s' not found in column definitions of table %s" % (item, [current_table.schema.name, current_table.name]),
                            validator=validator, validator_value=value, instance=instance, schema=schema)
                    else:
                        # source-path should terminate on a column-name, syntactically
                        break

                # validate an inbound or outbound fkey in the path
                elif isinstance(item, dict) and any(_is_qualified_name(item[io]) for io in ['inbound', 'outbound'] if io in item):
                    for direction, match_with_table, update_current_table in [
                        ('outbound',    'table',    'pk_table'),
                        ('inbound',     'pk_table', 'table')
                    ]:
                        if direction in item:
                            constraint_name = item[direction]
                            try:
                                fkey = _model.fkey(constraint_name)  # test if fkey exists in model
                                if getattr(fkey, match_with_table) != current_table:  # test if current table matches fkey table property
                                    raise jsonschema.ValidationError(
                                        "%s foreign key %s not associated with %s" % (direction, constraint_name, [current_table.schema.name, current_table.name]),
                                        validator=validator, validator_value=value, instance=instance, schema=schema)
                                current_table = getattr(fkey, update_current_table)  # update the "current" table
                            except KeyError as e:
                                # fkey not found in model
                                raise jsonschema.ValidationError("%s not found in model fkeys" % constraint_name, cause=e,
                                                                 validator=validator, validator_value=value,
                                                                 instance=instance, schema=schema)
                else:
                    break  # unrecognized source-path syntax, break out and let schema validation take its course
        # and ignore anything unexpected

    return _validation_func
