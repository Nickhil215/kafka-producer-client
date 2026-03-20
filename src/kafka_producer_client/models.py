import uuid
import time
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field

class ActionSource(str, Enum):
    HUMAN_API = "HUMAN_API"
    SYSTEM = "SYSTEM"

class ActionType(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    READ = "READ"

class RequesterType(str, Enum):
    TENANT = "TENANT"
    USER = "USER"
    SYSTEM = "SYSTEM"

class NodeType(str, Enum):
    PRODUCTCREATION = "PRODUCTCREATION"
    SCIENCES= "SCIENCES"
    
    # Add more as needed

class ActionLogRequestStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

class ActionLogCreateRequest(BaseModel):
    id: Optional[str] = None
    message: Optional[str] = None
    timestamp: int = Field(default_factory=lambda: int(time.time() * 1000))
    actionType: str
    requesterType: str
    universeId: Optional[str] = None
    nodeType: Optional[str] = None
    actionSource: str
    requesterId: Optional[str] = None
    nodeId: Optional[str] = None
    version: int = 1
    requestObject: Optional[Any] = None
    responseObject: Optional[Any] = None
    status: str
    name: Optional[str] = None
    constructName: Optional[str] = None
    parentTenantId: Optional[str] = None

class DataIngestionOperation(BaseModel):
    actionType: ActionType
    object: ActionLogCreateRequest
    id: Optional[str] = None
    schemaId: str
    tenantId: str

def create_action_log_operation(
    action_source: ActionSource,
    action_type: ActionType,
    requester_type: RequesterType,
    requester_id: str,
    node_type: NodeType,
    node_id: str,
    message: str,
    request_object: Any,
    response_object: Any,
    universe_id: str,
    status: ActionLogRequestStatus,
    schema_id: str,
    tenant_id: str,
    parent_tenant_id: Optional[str] = None
) -> DataIngestionOperation:
    """
    Replicates the Java logic to create a DataIngestionOperation containing
    an ActionLogCreateRequest.
    """
    
    # Generate ID like: UUID + "_" + timestamp
    timestamp_ms = int(time.time() * 1000)
    generated_id = f"{uuid.uuid4()}_{timestamp_ms}"
    
    # 1. Build ActionLogCreateRequest
    payload = ActionLogCreateRequest(
        id=generated_id,
        message=message,
        timestamp=timestamp_ms,
        actionType=action_type.value,
        requesterType=requester_type.value,
        universeId=universe_id,
        nodeType=node_type.value,
        actionSource=action_source.value,
        requesterId=requester_id,
        nodeId=node_id,
        requestObject=request_object,
        responseObject=response_object,
        status=str(status.value),
        parentTenantId=parent_tenant_id
    )
    
    # 2. Wrap in DataIngestionOperation
    return DataIngestionOperation(
        actionType=action_type,
        object=payload,
        id=node_id,
        schemaId=schema_id,
        tenantId=tenant_id
    )
