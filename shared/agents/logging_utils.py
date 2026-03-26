from datetime import datetime
from shared.agents.db import execute_sql


CREATE_TABLE_SQL = """
create table if not exists agent_request_log (
    id bigserial primary key,
    created_at timestamp not null,
    user_email text,
    agent_name text not null,
    request_text text,
    status text,
    metadata_json text
)
"""


def ensure_agent_log_table() -> None:
    try:
        execute_sql(CREATE_TABLE_SQL)
    except Exception:
        pass


def log_agent_request(
    user_email: str,
    agent_name: str,
    request_text: str,
    status: str = "submitted",
    metadata_json: str = None,
) -> None:
    sql = """
    insert into agent_request_log
    (created_at, user_email, agent_name, request_text, status, metadata_json)
    values (%s, %s, %s, %s, %s, %s)
    """
    try:
        execute_sql(
            sql,
            (
                datetime.utcnow(),
                user_email,
                agent_name,
                request_text,
                status,
                metadata_json,
            ),
        )
    except Exception:
        pass