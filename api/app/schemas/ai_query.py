from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AIQueryRequest(BaseModel):
    connection_id: str | None = Field(None, description="ID of a saved healthcare middleware connection.")
    db_type: Literal["postgresql", "mysql", "sqlite"] = Field("postgresql", description="Database engine type.")
    host: str | None = Field(None, description="Hostname or IP address.")
    port: int | None = Field(None, description="Port number.")
    database: str | None = Field(None, description="Database name.")
    username: str | None = Field(None, description="Database username.")
    password: str | None = Field(None, description="Database password.")
    question: str = Field(..., description="Plain-English clinical question.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "healthcare-db",
                "question": "What are the top 5 most common conditions?",
            }
        }
    )
