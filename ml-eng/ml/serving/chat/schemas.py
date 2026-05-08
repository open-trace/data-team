from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

StakeholderType = Literal[
    "government_public",
    "development_partners",
    "private_sector",
    "farmers_communities",
    "entrepreneurs_ecosystem",
]


class SessionCreateRequest(BaseModel):
    stakeholder_type: StakeholderType


class SessionCreateResponse(BaseModel):
    session_id: str
    created_at: str
    stakeholder_type: StakeholderType


class ChatMessage(BaseModel):
    role: str = Field(..., description="user or assistant")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    session_id: str | None = None
    stakeholder_type: StakeholderType | None = None
    conversation_history: list[ChatMessage] | None = None

    @model_validator(mode="after")
    def no_session_with_bootstrap_stakeholder(self):
        sid = (self.session_id or "").strip()
        if sid and self.stakeholder_type is not None:
            raise ValueError("stakeholder_type is only allowed when session_id is omitted (bootstrap)")
        return self


class ChatSuccessResponse(BaseModel):
    assistant_message: str
    session_id: str
    request_id: str
    created_at: str


class ErrorBody(BaseModel):
    code: str
    message: str
