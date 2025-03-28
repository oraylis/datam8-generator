"""
DataM8
Copyright (C) 2024-2025 ORAYLIS GmbH

This file is part of DataM8.

DataM8 is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

DataM8 is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""

# generated by datamodel-codegen:
#   filename:  DataProducts.json

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Type(Enum):
    DATA_PRODUCT = 'dataProduct'


class DataModule(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    name: Optional[str] = None
    displayName: Optional[str] = None
    purpose: Optional[str] = None
    explanation: Optional[str] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'DataModule':
        return DataModule.model_validate(obj, from_attributes=False)


class DataProduct(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    name: Optional[str] = None
    displayName: Optional[str] = None
    purpose: Optional[str] = None
    explanation: Optional[str] = None
    module: Optional[List[DataModule]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'DataProduct':
        return DataProduct.model_validate(obj, from_attributes=False)


class Model(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    field_schema: Optional[str] = Field(None, alias='$schema')
    type: Optional[Type] = None
    items: Optional[List[DataProduct]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'Model':
        return Model.model_validate(obj, from_attributes=False)
