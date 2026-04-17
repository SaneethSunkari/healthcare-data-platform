from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ConnectionRequest(BaseModel):
    connection_id: str | None = Field(
        None, description="ID of a previously saved connection. If provided, all other fields are optional."
    )
    db_type: Literal["postgresql", "mysql", "sqlite"] = Field("postgresql", description="Database engine type.")
    host: str | None = Field(None, description="Hostname or IP address.")
    port: int | None = Field(None, description="Port number.")
    database: str | None = Field(None, description="Database name.")
    username: str | None = Field(None, description="Database username.")
    password: str | None = Field(None, description="Database password.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "healthcare-db",
            }
        }
    )


class RegisterConnectionRequest(BaseModel):
    name: str = Field(..., description="A friendly label for this saved connection.")
    db_type: Literal["postgresql", "mysql", "sqlite"] = Field("postgresql", description="Database engine type.")
    host: str | None = Field(None, description="Hostname or IP address.")
    port: int | None = Field(None, description="Port number.")
    database: str | None = Field(None, description="Database name.")
    username: str | None = Field(None, description="Database username.")
    password: str | None = Field(None, description="Database password.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Healthcare DB",
                "db_type": "postgresql",
                "host": "127.0.0.1",
                "port": 15432,
                "database": "healthcare_db",
                "username": "postgres",
                "password": "postgres",
            }
        }
    )
