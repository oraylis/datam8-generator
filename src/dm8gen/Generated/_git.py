# generated by datamodel-codegen:
#   filename:  .git

from __future__ import annotations

from typing import Any

from pydantic import RootModel


class Model(RootModel[Any]):
    model_config = ConfigDict(extra='allow', populate_by_name=True)
    root: Any
