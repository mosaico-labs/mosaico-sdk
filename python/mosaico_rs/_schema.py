from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, get_args

import pyarrow as pa
from mosaicolabs import IMU, Message, Serializable
from pydantic import BaseModel

from ._errors import InvalidArgumentError
from ._views import Columns, IMUView, imu_model

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def is_int64(value: Any) -> bool:
    try:
        return _INT64_MIN <= operator.index(value) <= _INT64_MAX
    except TypeError:
        return False


def check_ontology(ontology: object) -> type[Serializable]:
    if not (isinstance(ontology, type) and issubclass(ontology, Serializable)):
        raise InvalidArgumentError(f"ontology must be an ontology class such as IMU, not {ontology!r}")
    if ontology.__ontology_tag__ is None:
        raise InvalidArgumentError(f"{ontology.__name__} has no ontology tag")
    return ontology


def wire_schema(ontology: type[Serializable]) -> pa.Schema:
    return pa.schema([*Message.__msco_pyarrow_struct__, *ontology.__msco_pyarrow_struct__])


@dataclass(frozen=True)
class OntologySpec:
    ontology: type[Serializable]
    tag: str
    schema: pa.Schema
    optional: frozenset[str]
    view: Callable[[Columns, int], Any]
    build: Callable[[Any], Serializable]


def _spec(
    ontology: type[Serializable], view: Callable[[Columns, int], Any], build: Callable[[Any], Serializable]
) -> OntologySpec:
    tag = ontology.__ontology_tag__
    assert tag is not None, ontology
    return OntologySpec(ontology, tag, wire_schema(ontology), frozenset(_optional(ontology, "")), view, build)


def _optional(model: type[BaseModel], path: str) -> Iterable[str]:
    for name, info in model.model_fields.items():
        name = f"{path}.{name}" if path else name
        if not info.is_required():
            yield name
        nested = _model(info.annotation)
        if nested is not None:
            yield from _optional(nested, name)


def _model(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for arg in get_args(annotation):
        model = _model(arg)
        if model is not None:
            return model
    return None


SPECS: dict[type[Serializable], OntologySpec] = {IMU: _spec(IMU, IMUView, imu_model)}


_VIEWS = {pa.string_view(): pa.string(), pa.binary_view(): pa.binary()}
_SERVED = {
    pa.string(): pa.string_view(),
    pa.large_string(): pa.string_view(),
    pa.binary(): pa.binary_view(),
    pa.large_binary(): pa.binary_view(),
}


def served_schema(ontology: type[Serializable]) -> pa.Schema:
    return pa.schema([_served(field) for field in wire_schema(ontology)])


def _served(field: pa.Field) -> pa.Field:
    return field.with_type(_SERVED.get(field.type, field.type))


def schema_problem(spec: OntologySpec, actual: pa.Schema) -> str | None:
    return _fields_problem(spec.schema, actual, "", spec.optional)


def _fields_problem(
    expected: Iterable[pa.Field], actual: pa.Schema | pa.StructType, path: str, optional: frozenset[str]
) -> str | None:
    for field in expected:
        name = f"{path}.{field.name}" if path else field.name
        i = actual.get_field_index(field.name)
        if i < 0:
            if name in optional:
                continue
            return f"missing field '{name}'"
        problem = _type_problem(field.type, actual.field(i).type, name, optional)
        if problem is not None:
            return problem
    return None


def _type_problem(expected: pa.DataType, found: pa.DataType, name: str, optional: frozenset[str]) -> str | None:
    if isinstance(expected, pa.StructType) and isinstance(found, pa.StructType):
        return _fields_problem(expected.fields, found, name, optional)
    if isinstance(expected, pa.ListType) and isinstance(found, pa.ListType):
        compatible = _type_problem(expected.value_type, found.value_type, name, optional) is None
    else:
        compatible = found == expected or _VIEWS.get(found) == expected
    if compatible:
        return None
    return f"field '{name}' is {_type_name(found)}, expected {_type_name(expected)}"


def _type_name(t: pa.DataType) -> str:
    return "struct" if isinstance(t, pa.StructType) else str(t)


def conform(expected: pa.Schema, actual: pa.Schema) -> pa.Schema:
    fields = _conform_fields(expected, actual)
    schema = pa.schema([f if actual.get_field_index(f.name) >= 0 else _served(f) for f in fields])
    return schema.with_metadata(actual.metadata) if actual.metadata else schema


def _conform_fields(expected: Iterable[pa.Field], actual: pa.Schema | pa.StructType) -> list[pa.Field]:
    fields = list(actual)
    at = 0
    for field in expected:
        i = actual.get_field_index(field.name)
        if i < 0:
            fields.insert(at, field)
            at += 1
            continue
        found = actual.field(i)
        at = fields.index(found) + 1
        if isinstance(field.type, pa.StructType) and isinstance(found.type, pa.StructType):
            fields[at - 1] = found.with_type(pa.struct(_conform_fields(field.type, found.type)))
    return fields


def fill(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    if batch.schema.equals(schema):
        return batch
    array = _fill(batch.to_struct_array(), pa.struct(list(schema)))
    assert isinstance(array, pa.StructArray)
    return pa.RecordBatch.from_struct_array(array).replace_schema_metadata(schema.metadata)


def _fill(array: pa.Array, target: pa.DataType) -> pa.Array:
    if array.type.equals(target) or not isinstance(target, pa.StructType):
        return array
    assert isinstance(array, pa.StructArray)
    n = len(array)
    children = [
        _fill(array.field(f.name), f.type) if array.type.get_field_index(f.name) >= 0 else pa.nulls(n, f.type)
        for f in target
    ]
    mask = array.is_null() if array.null_count else None
    return pa.StructArray.from_arrays(children, fields=list(target), mask=mask)
