import asyncio
import aiohttp
import logging
from pydantic import BaseModel, Field
import aiosqlite

async def init_db():

    async with aiosqlite.connect("intel_warehouse.db") as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT
            )
        """)

        await db.commit()

logging.basicConfig(

    filename = "capstone_run.log",
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

class UserIntel(BaseModel):

    name: str
    email: str

class PostIntel(BaseModel):

    title: str
    body: str

async def intel_worker(worker_id: int, queue: asyncio.Queue, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, db: aiosqlite.Connection):

    while not queue.empty():

        task = await queue.get()

        task_type = task["type"]
        item_id = task["id"]

        async with semaphore:

            logging.info(f"{worker_id} processing {task_type} {item_id}...")

            if task_type == "user":
                url = f"https://jsonplaceholder.typicode.com/users/{item_id}"
                async with session.get(url) as response:
                    data = await response.json()
                clear_data = UserIntel(**data).model_dump()

                await db.execute(
                    "INSERT OR REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
                    (item_id, clear_data["name"], clear_data["email"])
                )

                await db.commit()

            elif task_type == "post":
                url = f"https://jsonplaceholder.typicode.com/posts/{item_id}"
                async with session.get(url) as response:
                    data = await response.json()
                clear_data = PostIntel(**data).model_dump()

                await db.execute(
                    "INSERT OR REPLACE INTO posts (id, title, body) VALUES (?, ?, ?)",
                    (item_id, clear_data["title"], clear_data["body"])
                )

                await db.commit()

        logging.info(f"{worker_id} processed {task_type.upper()} {item_id}: {clear_data}")

        queue.task_done()
    
async def main():

    await init_db()

    item_queue = asyncio.Queue()

    for number in range(1, 11):

        item_queue.put_nowait({"type": "user", "id": number})
        item_queue.put_nowait({"type": "post", "id": number})

    semaphore = asyncio.Semaphore(2)

    async with aiohttp.ClientSession() as session:

        async with aiosqlite.connect("intel_warehouse.db") as db:

            logging.info("Launching Multi-API swarm...")

            await asyncio.gather(
                intel_worker(1, item_queue, session, semaphore, db),
                intel_worker(2, item_queue, session, semaphore, db),
                intel_worker(3, item_queue, session, semaphore, db)
            )

        logging.info("Multi-API swarm launched.")

asyncio.run(main())
