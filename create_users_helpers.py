from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

import pandas as pd


def get_target_instances(destination: str, authenticated_instances: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    if destination == "Todas":
        return list(authenticated_instances)
    return [item for item in authenticated_instances if item.get("name") == destination]


def init_default_users_state(default_superusers: Sequence[Dict[str, str]]) -> Tuple[pd.DataFrame, List[str]]:
    rows = [{**dict(row), "selected": True} for row in default_superusers]
    df = pd.DataFrame(rows)
    selection = [str(row.get("username", "")) for row in rows if str(row.get("username", ""))]
    return df, selection


def resolve_destination(current_destination: str, authenticated_instances: Sequence[Dict[str, str]]) -> str:
    options = ["Todas"] + [item.get("name", "") for item in authenticated_instances]
    return current_destination if current_destination in options else "Todas"
