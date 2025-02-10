# generated by datamodel-codegen:
#   filename:  DataTypes.json

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Type(Enum):
    DATA_TYPE = 'dataType'


class DataType(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    name: str
    displayName: Optional[str] = None
    purpose: Optional[str] = None
    hasCharLen: Optional[bool] = 'False'
    hasPrecision: Optional[bool] = 'False'
    hasScale: Optional[bool] = 'False'
    parquetType: str
    sqlType: str

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'DataType':
        return DataType.model_validate(obj, from_attributes=False)


class Model(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    field_schema: Optional[str] = Field(None, alias='$schema')
    type: Optional[Type] = None
    items: Optional[List[DataType]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'Model':
        return Model.model_validate(obj, from_attributes=False)
