import os
import databases
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
print("Testing database connection...")

database = databases.Database(DATABASE_URL)

async def test_connection():
    try:
        await database.connect()
        print("✅ Connected successfully!")
        await database.disconnect()
    except Exception as e:
        print("❌ Connection failed:", e)

import asyncio
asyncio.run(test_connection())
