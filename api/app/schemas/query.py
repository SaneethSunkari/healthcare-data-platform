from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class QueryRequest(BaseModel):
    connection_id: str | None = Field(None, description="ID of a saved healthcare middleware connection.")
    db_type: Literal["postgresql", "mysql", "sqlite"] = Field("postgresql", description="Database engine type.")
    host: str | None = Field(None, description="Hostname or IP address.")
    port: int | None = Field(None, description="Port number.")
    database: str | None = Field(None, description="Database name.")
    username: str | None = Field(None, description="Database username.")
    password: str | None = Field(None, description="Database password.")
    sql: str = Field(..., description="A single read-only SQL SELECT query.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "healthcare-db",
                "sql": "SELECT encounter_type, AVG(cost) AS avg_cost FROM encounter_analytics GROUP BY encounter_type ORDER BY avg_cost DESC LIMIT 5;",
            }
        }
    )
