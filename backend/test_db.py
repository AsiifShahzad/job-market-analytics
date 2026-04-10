"""
Quick database connection test
Run: python test_db.py
"""
import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine

# Load env
load_dotenv()

async def test_connection():
    db_url = os.getenv('DATABASE_URL')
    print(f"Testing connection to: {db_url[:80]}...")
    
    try:
        engine = create_async_engine(db_url, echo=False)
        
        async with engine.begin() as conn:
            result = await conn.execute(__import__('sqlalchemy').text('SELECT 1'))
            print("✅ Database connection SUCCESS!")
            print(f"   Query result: {result.scalar()}")
            
    except Exception as e:
        print(f"❌ Database connection FAILED!")
        print(f"   Error: {type(e).__name__}: {str(e)[:200]}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(test_connection())
