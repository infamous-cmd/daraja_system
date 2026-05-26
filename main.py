import os
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

# ==========================================
# 1. INITIALIZE SYSTEM LOGISTICS & ENVIRONMENT
# ==========================================
load_dotenv()

# Uses local relative storage for database persistence
DATABASE_URL = "sqlite:///./daraja_system.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(
    title="Daraja Civic Accountability Platform",
    description="Render production deployment microservice engine.",
    version="1.2.0"
)

# Open CORS configuration to let external browser pages connect smoothly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 2. DATABASE SCHEMA DESIGN (ORM Models)
# ==========================================
class DBIncident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True, index=True)
    tracking_id = Column(String, unique=True, index=True)
    category = Column(String, index=True)
    description = Column(Text)
    location_district = Column(String, index=True)
    reporter_contact = Column(String)
    status = Column(String, default="Ingested")
    is_escalated = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ==========================================
# 3. SCHEMA VALIDATION AND DATA WRAPPERS
# ==========================================
class SMSGatewayPayload(BaseModel):
    secret_gateway_key: str = Field(..., description="Modem signature validation string")
    sender_phone: str = Field(..., example="+256700000000")
    message_body: str = Field(..., example="System error report")

class PublicIncidentSchema(BaseModel):
    tracking_id: str
    category: str
    description: str
    location_district: str
    status: str
    created_at: datetime

    class Config:
        from_attributes = True

class FullIncidentResponseSchema(BaseModel):
    id: int
    tracking_id: str
    category: str
    description: str
    location_district: str
    reporter_contact: str
    status: str
    is_escalated: bool
    created_at: datetime

    class Config:
        from_attributes = True

# ==========================================
# 4. DEPENDENCIES & UTILITY HELPER LOGIC
# ==========================================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def parse_incident_category(message: str) -> str:
    msg_lower = message.lower()
    if any(k in msg_lower for k in ["water", "pipe", "borehole", "leak", "well"]):
        return "water-infrastructure"
    if any(k in msg_lower for k in ["road", "pothole", "bridge", "tarmac", "culvert"]):
        return "transportation"
    if any(k in msg_lower for k in ["power", "electricity", "transformer", "blackout", "yaka"]):
        return "energy-grid"
    return "general-delivery"

# ==========================================
# 5. PUBLIC CHANNELS (Public Dashboards Link Here)
# ==========================================
@app.get("/api/v1/public/telemetry-feed", response_model=List[PublicIncidentSchema])
async def fetch_public_sanitized_feed(db: Session = Depends(get_db)):
    """
    Returns sanitized incident events completely stripped of contact info 
    and tracking profiles for public visibility compliance.
    """
    return db.query(DBIncident).filter(DBIncident.is_escalated == False).order_by(DBIncident.created_at.desc()).all()

@app.get("/api/v1/health")
async def system_health_handshake():
    return {"status": "operational", "timestamp": datetime.utcnow().isoformat()}

# ==========================================
# 6. SMS HARDWARE GATEWAY PROCESSING ENDPOINT
# ==========================================
@app.post("/api/v1/gateway/sms", status_code=status.HTTP_201_CREATED)
async def process_incoming_gateway_sms(payload: SMSGatewayPayload, db: Session = Depends(get_db)):
    secure_modem_key = os.getenv("SMS_GATEWAY_KEY")
    if not secure_modem_key or payload.secret_gateway_key != secure_modem_key:
        raise HTTPException(status_code=403, detail="Gateway validation profile mismatch.")

    determined_category = parse_incident_category(payload.message_body)
    generated_tracking_id = f"DRJ-{int(datetime.utcnow().timestamp())}"
    escalation_check = "urgent" in payload.message_body.lower() or "danger" in payload.message_body.lower()

    new_incident = DBIncident(
        tracking_id=generated_tracking_id,
        category=determined_category,
        description=payload.message_body,
        location_district="Kassanda District",  
        reporter_contact=payload.sender_phone,
        is_escalated=escalation_check
    )
    db.add(new_incident)
    db.commit()
    return {"status": "queued", "tracking_id": generated_tracking_id}

# ==========================================
# 7. ADMINISTRATIVE DATA AUDIT ROUTE
# ==========================================
@app.get("/api/v1/audit/reconciliation-ledger", response_model=List[FullIncidentResponseSchema])
async def system_audit_reconciliation_handler(token: str, db: Session = Depends(get_db)):
    """
    Standard core reconciliation ledger used by administrators to crosscheck
    database tables and map unredacted intake telemetry.
    """
    admin_passkeys = [
        os.getenv("PASSKEY_BALAM"),
        os.getenv("PASSKEY_KABANDA"),
        os.getenv("PASSKEY_USER")
    ]
    
    if not token or token not in admin_passkeys or None in admin_passkeys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Node token invalid. System access restricted."
        )
    
    return db.query(DBIncident).order_by(DBIncident.created_at.desc()).all()