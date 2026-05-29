import os
import json
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
import tweepy

# ==========================================
# 1. INITIALIZE SYSTEM LOGISTICS & ENVIRONMENT
# ==========================================
load_dotenv()

# DYNAMIC DATABASE ROUTER: Links to a cloud database or falls back to local SQLite
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL)
else:
    DATABASE_URL = "sqlite:///./daraja_system.db"
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(
    title="Daraja Civic Accountability Platform",
    description="Production deployment microservice engine with safety overrides.",
    version="1.4.0"
)

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
    
    # NEW: Fields to support web application multimedia payloads safely
    media_url = Column(String, nullable=True)
    media_type = Column(String, nullable=True) # "image", "video", "audio", or null
    
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ==========================================
# 3. SCHEMA VALIDATION AND DATA WRAPPERS
# ==========================================
class SMSGatewayPayload(BaseModel):
    secret_gateway_key: str = Field(..., description="Modem signature validation string")
    sender_phone: str = Field(..., example="+256700000000")
    message_body: str = Field(..., example="System error report")

# NEW: Unified payload for rich mobile applications sending media files
class WebAppIncidentPayload(BaseModel):
    message_body: str
    sender_phone: str
    media_url: Optional[str] = None
    media_type: Optional[str] = None # "image", "video", "audio"

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
    media_url: Optional[str]
    media_type: Optional[str]
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

def load_central_registry():
    file_path = os.path.join(os.path.dirname(__file__), "daraja", "districts_registry.json")
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"⚠️ Registry dynamic read failure: {e}")
        return {"registry": {}}

def resolve_district_dynamically(message_text: str) -> str:
    """Scans text against the complete 138 districts map, adding major park catchments"""
    text_lower = message_text.lower()
    
    # Direct regional landmark overrides (e.g., Queen Elizabeth Park catchment defaults to Kasese)
    if "queen elizabeth" in text_lower or "qenp" in text_lower or "eleph" in text_lower:
        return "Kasese District"
        
    data_matrix = load_central_registry()
    registry = data_matrix.get("registry", {})
    
    for _, details in registry.items():
        district_name = details.get("name", "")
        if district_name and district_name.lower() in text_lower:
            return f"{district_name} District"
            
    return "Unknown Region (National Fallback)"

def parse_incident_category(message: str) -> str:
    """
    Parses categories with cross-lingual keyword matching.
    Includes an immediate safety override for high-priority crimes.
    """
    msg_lower = message.lower()
    
    # 🚨 EMERGENCY CRIME & POACHING DETECTOR (English, Luganda, Runyakitara variants)
    # Catches: crime, number plates (UAA...), killing wildlife, ivory (omusanga), theft (bubbi)
    crime_keywords = [
        "crime", "theft", "thief", "robbery", "assault", "killing", "murder", "gun", "weapon",
        "ivory", "elephant", "poacher", "uwan", "omusanga", "enjovu", "akabenje", "plate", "piki"
    ]
    # Simple regex-like match for Ugandan vehicle number plates (e.g., UBA 123X)
    has_number_plate = any(char.isdigit() for char in msg_lower) and "u" in msg_lower
    
    if any(k in msg_lower for k in crime_keywords) or has_number_plate:
        return "law-enforcement-emergency"
        
    # Infrastructure fallbacks
    if any(k in msg_lower for k in ["water", "pipe", "borehole", "leak", "well", "amaddi", "mazzi"]):
        return "water-infrastructure"
    if any(k in msg_lower for k in ["road", "pothole", "bridge", "tarmac", "enguudo"]):
        return "transportation"
    if any(k in msg_lower for k in ["power", "electricity", "transformer", "yaka", "amasanyalaze"]):
        return "energy-grid"
        
    return "general-delivery"

# ==========================================
# 5. ASYNC BACKGROUND ACCOUNTABILITY WORKERS
# ==========================================
async def broadcast_public_accountability_tweet(district_name: str, category: str, raw_report_details: str):
    TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
    TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
    TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
    TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
        print("⚠️ Twitter API credentials missing. Skipping public broadcast.")
        return

    try:
        client = tweepy.Client(
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_SECRET
        )
        
        # Format clean public alerts. Uses unique tags for law enforcement priority routing
        prefix = "🚨 #CRITICAL_EMERGENCY" if category == "law-enforcement-emergency" else "📢 #DarajaAlert"
        
        tweet_text = (
            f"{prefix}\n\n"
            f"📍 Location: #{district_name.replace(' ', '')}\n"
            f"Category: {category.upper()}\n"
            f"📝 Dispatch Details: \"{raw_report_details[:120]}...\"\n\n"
            f"Tracking System: Active. Sent to field desks. #Uganda"
        )
        
        client.create_tweet(text=tweet_text)
        print(f"🐦 Accountability tweet deployed successfully for {district_name}!")
        
    except Exception as e:
        print(f"⚠️ Failed to broadcast Twitter update: {e}")

# ==========================================
# 6. OPERATIONAL API INTAKE GATEWAYS
# ==========================================

@app.post("/api/v1/gateway/sms", status_code=status.HTTP_201_CREATED)
async def process_incoming_gateway_sms(
    payload: SMSGatewayPayload, 
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db)
):
    """Processes analog, plain-text SMS messages coming from hardware modems"""
    secure_modem_key = os.getenv("SMS_GATEWAY_KEY")
    if not secure_modem_key or payload.secret_gateway_key != secure_modem_key:
        raise HTTPException(status_code=403, detail="Gateway validation profile mismatch.")

    determined_category = parse_incident_category(payload.message_body)
    generated_tracking_id = f"DRJ-SMS-{int(datetime.utcnow().timestamp())}"
    resolved_district = resolve_district_dynamically(payload.message_body)
    
    # Escalate automatically if it's a critical security emergency
    escalation_check = (determined_category == "law-enforcement-emergency")

    new_incident = DBIncident(
        tracking_id=generated_tracking_id,
        category=determined_category,
        description=payload.message_body,
        location_district=resolved_district,  
        reporter_contact=payload.sender_phone,
        is_escalated=escalation_check
    )
    db.add(new_incident)
    db.commit()

    background_tasks.add_task(
        broadcast_public_accountability_tweet,
        district_name=resolved_district,
        category=determined_category,
        raw_report_details=payload.message_body
    )

    return {"status": "queued", "tracking_id": generated_tracking_id, "resolved_location": resolved_district}


@app.post("/api/v1/gateway/web-app", status_code=status.HTTP_201_CREATED)
async def process_incoming_webapp_report(
    payload: WebAppIncidentPayload, 
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db)
):
    """Processes rich payloads containing media files (photos, voice recordings, videos)"""
    determined_category = parse_incident_category(payload.message_body)
    generated_tracking_id = f"DRJ-APP-{int(datetime.utcnow().timestamp())}"
    resolved_district = resolve_district_dynamically(payload.message_body)
    escalation_check = (determined_category == "law-enforcement-emergency")

    new_incident = DBIncident(
        tracking_id=generated_tracking_id,
        category=determined_category,
        description=payload.message_body,
        location_district=resolved_district,  
        reporter_contact=payload.sender_phone,
        is_escalated=escalation_check,
        media_url=payload.media_url,
        media_type=payload.media_type
    )
    db.add(new_incident)
    db.commit()

    # Append media context placeholder indicators inside public timeline text strings if present
    text_with_media_flag = payload.message_body
    if payload.media_type:
        text_with_media_flag += f" [{payload.media_type.upper()} EVIDENCE ATTACHED]"

    background_tasks.add_task(
        broadcast_public_accountability_tweet,
        district_name=resolved_district,
        category=determined_category,
        raw_report_details=text_with_media_flag
    )

    return {"status": "queued", "tracking_id": generated_tracking_id, "resolved_location": resolved_district}

# ==========================================
# 7. PUBLIC CHANNELS & ADMINISTRATIVE AUDITING
# ==========================================
@app.get("/api/v1/public/telemetry-feed", response_model=List[PublicIncidentSchema])
async def fetch_public_sanitized_feed(db: Session = Depends(get_db)):
    return db.query(DBIncident).filter(DBIncident.is_escalated == False).order_by(DBIncident.created_at.desc()).all()

@app.get("/api/v1/health")
async def system_health_handshake():
    return {"status": "operational", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/v1/audit/reconciliation-ledger", response_model=List[FullIncidentResponseSchema])
async def system_audit_reconciliation_handler(token: str, db: Session = Depends(get_db)):
    admin_passkeys = [os.getenv("PASSKEY_BALAM"), os.getenv("PASSKEY_KABANDA"), os.getenv("PASSKEY_USER")]
    if not token or token not in admin_passkeys or None in admin_passkeys:
        raise HTTPException(status_code=401, detail="Node token invalid. Access restricted.")
    return db.query(DBIncident).order_by(DBIncident.created_at.desc()).all()