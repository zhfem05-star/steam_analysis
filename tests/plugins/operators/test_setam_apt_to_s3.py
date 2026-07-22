import pytest
from operators.steam_api_to_s3 import SteamApiToS3Operators

def test_execute_raise_on_unknown_method():
    op = SteamApiToS3Operators(
        task_id = "test_task",
        method = "not_a_real_method",
        s3_key = "dummy.json",
    )
    with pytest.raises(ValueError):
        op.execute(context={})