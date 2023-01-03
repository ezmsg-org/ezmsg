import asyncio
import argparse

from .graphserver import GraphServer
from .shmserver import SHMServer
from .netprotocol import AddressType, GRAPHSERVER_ADDR

from . import logger

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

async def shutdown_servers(address: AddressType = GRAPHSERVER_ADDR) -> None:
    try:
        async with GraphServer.connection(address) as connection:
            connection.shutdown()
        logger.info(f'Shutdown GraphServer running at {address}')
    except ConnectionRefusedError:
        logger.info('GraphServer is not running, or is refusing connections')
        
    try:
        await SHMServer.shutdown_server()
        logger.info('Shutdown SHMServer')
    except ConnectionRefusedError:
        logger.info('SHMServer not running, or is refusing connections')

parser = argparse.ArgumentParser('ezmsg.core')

parser.add_argument(
    'command', 
    type=str, 
    help = 'command', 
    choices=['start', 'shutdown']
)

class Args:
    command: str

args = parser.parse_args(namespace=Args)

if args.command == 'shutdown':
    try:
        loop.run_until_complete(shutdown_servers())
    finally:
        loop.close()
    