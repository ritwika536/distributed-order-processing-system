from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "sqlite:///./inventory.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Product(Base):
    __tablename__ = "products"
    id = Column(String, primary_key=True)
    name = Column(String)
    stock = Column(Integer)

class SeedRequest(BaseModel):
    product_id: str
    name: str
    stock: int

Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Inventory Service is running"}

@app.post("/seed")
def seed_product(req: SeedRequest):
    db = SessionLocal()
    existing = db.query(Product).filter(Product.id == req.product_id).first()

    if existing:
        existing.stock = req.stock
        existing.name = req.name
    else:
        new_product = Product(id=req.product_id, name=req.name, stock=req.stock)
        db.add(new_product)

    db.commit()
    db.close()
    return {"status": "seeded", "product_id": req.product_id, "stock": req.stock}
@app.get("/inventory/{product_id}")
def get_stock(product_id: str):
    db = SessionLocal()
    product = db.query(Product).filter(Product.id == product_id).first()
    db.close()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"product_id": product.id, "name": product.name, "stock": product.stock}
class ReserveRequest(BaseModel):
    product_id: str
    quantity: int

@app.post("/inventory/reserve")
def reserve_stock(req: ReserveRequest):
    db = SessionLocal()
    product = db.query(Product).filter(Product.id == req.product_id).first()

    if not product:
        db.close()
        raise HTTPException(status_code=404, detail="Product not found")

    if product.stock < req.quantity:
        db.close()
        raise HTTPException(status_code=409, detail="Insufficient stock")

    product.stock -= req.quantity
    db.commit()
    remaining = product.stock
    db.close()

    return {"status": "reserved", "product_id": req.product_id, "remaining_stock": remaining}
@app.post("/inventory/release")
def release_stock(req: ReserveRequest):
    db = SessionLocal()
    product = db.query(Product).filter(Product.id == req.product_id).first()
    if not product:
        db.close()
        raise HTTPException(status_code=404, detail="Product not found")

    product.stock += req.quantity
    db.commit()
    remaining = product.stock
    db.close()

    return {"status": "released", "product_id": req.product_id, "remaining_stock": remaining}