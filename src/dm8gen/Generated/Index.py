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
#   filename:  Index.json

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class IndexEntry(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    locator: str
    name: str
    absPath: str
    references: Optional[List[str]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'IndexEntry':
        return IndexEntry.model_validate(obj, from_attributes=False)


class RawIndex(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    entry: Optional[List[IndexEntry]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'RawIndex':
        return RawIndex.model_validate(obj, from_attributes=False)


class StageIndex(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    entry: Optional[List[IndexEntry]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'StageIndex':
        return StageIndex.model_validate(obj, from_attributes=False)


class CoreIndex(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    entry: Optional[List[IndexEntry]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'CoreIndex':
        return CoreIndex.model_validate(obj, from_attributes=False)


class CuratedIndex(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    entry: Optional[List[IndexEntry]] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'CuratedIndex':
        return CuratedIndex.model_validate(obj, from_attributes=False)


class Model(BaseModel):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    rawIndex: Optional[RawIndex] = None
    stageIndex: Optional[StageIndex] = None
    coreIndex: Optional[CoreIndex] = None
    curatedIndex: Optional[CuratedIndex] = None

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_unset=True, mode='json')

    @staticmethod
    def from_dict(obj) -> 'Model':
        return Model.model_validate(obj, from_attributes=False)
