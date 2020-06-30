"""Definitions and implementation for validating ERMrest schema annotations."""

import json
import logging
import pkgutil
import jsonschema

from .. import core
from .ermrest_model import tag

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


def validate(model_obj, tag_name=None):
    """Validate the annotation(s) of the model object.

    :param model_obj: model object container of annotations
    :param tag_name: tag name of the annotation to validate, if none, will validate all known annotations
    :return: a list of validation errors, if any
    """
    if not tag_name:
        errors = []
        for tag_name in model_obj.annotations:
            logger.info("Validating against schema for %s" % tag_name)
            errors.extend(_validate(model_obj, tag_name))
        return errors
    else:
        return _validate(model_obj, tag_name)


def _validate(model_obj, tag_name):
    """Validate an annotation of the model object.

    :param model_obj: model object container of annotations
    :param tag_name: tag name of the annotation to validate
    :return: a list of validation errors, if any
    """
    try:
        if tag_name in model_obj.annotations:
            jsonschema.validate(model_obj.annotations[tag_name], _schemas[tag_name])
    except jsonschema.ValidationError as e:
        logger.error(e)
        return [e]
    except KeyError as e:
        msg = 'Unknown annotation tag name "%s"' % tag_name
        logger.error(msg)
        return [jsonschema.ValidationError(msg, cause=e)]
    except FileNotFoundError as e:
        logger.warning('No schema document found for tag name %s : %s' % (tag_name, e))
    return []
