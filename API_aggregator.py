import asyncio
import aiohttp
import logging
from pydantic import BaseModel, Field

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

async def intel_worker(worker_id: int, queue: asyncio.Queue, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):

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

            elif task_type == "post":
                url = f"https://jsonplaceholder.typicode.com/posts/{item_id}"
                async with session.get(url) as response:
                    data = await response.json()
                clear_data = PostIntel(**data).model_dump()

        logging.info(f"{worker_id} processed {task_type.upper()} {item_id}: {clear_data}")

        queue.task_done()

async def main():

    item_queue = asyncio.Queue()

    for number in range(1, 11):

        item_queue.put_nowait({"type": "user", "id": number})
        item_queue.put_nowait({"type": "post", "id": number})

    semaphore = asyncio.Semaphore(2)

    async with aiohttp.ClientSession() as session:

        logging.info("Launching Multi-API swarm...")

        await asyncio.gather(
            intel_worker(1, item_queue, session, semaphore),
            intel_worker(2, item_queue, session, semaphore),
            intel_worker(3, item_queue, session, semaphore),
        )
    logging.info("Multi-API swarm launched.")

asyncio.run(main())
