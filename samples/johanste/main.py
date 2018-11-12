import os

import asyncio

from internal.cosmos.cosmos_client import CosmosClient
from internal.cosmos import documents

host = os.getenv('ACCOUNT_HOST', 'https://localhost:443')
masterKey = os.getenv('ACCOUNT_KEY', 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==')
connectionPolicy = documents.ConnectionPolicy()
testDbName = 'johanste'

async def list_databases_async():
    client = CosmosClient(host, {'masterKey': masterKey}, connection_policy=connectionPolicy)
    databases = await client.query_databases_async('SELECT * FROM root r WHERE r.id=\'' + testDbName + '\'')
    async for database in databases:
        print(database)
    await client._requests_session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(list_databases_async())