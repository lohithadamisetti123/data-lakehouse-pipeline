from typing import Optional

from pydantic import BaseModel, Field


class Actor(BaseModel):
    id: Optional[int] = None
    login: Optional[str] = None
    display_login: Optional[str] = None
    gravatar_id: Optional[str] = None
    url: Optional[str] = None
    avatar_url: Optional[str] = None


class Repo(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    url: Optional[str] = None


class GitHubEvent(BaseModel):
    id: str
    type: Optional[str] = None
    created_at: str = Field(..., description="ISO8601 timestamp string")
    actor: Optional[Actor] = None
    repo: Optional[Repo] = None
    device_fingerprint: Optional[str] = None
