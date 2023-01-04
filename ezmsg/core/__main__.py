import asyncio
import argparse

from .graphserver import GraphServer
from .shmserver import SHMServer
from .netprotocol import (
    Address,
    GRAPHSERVER_PORT_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    GRAPHSERVER_PORT,
    SHMSERVER_PORT_ENV,
    SHMSERVER_PORT_DEFAULT,
    SHMSERVER_PORT
)

from . import logger

async def main() -> None:
    parser = argparse.ArgumentParser(
        'ezmsg.core',
        description = 'start and stop core ezmsg server processes',
        epilog = f"""
            Change server ports with environment variables.
            GraphServer will be hosted on ${GRAPHSERVER_PORT_ENV} (default {GRAPHSERVER_PORT_DEFAULT}).  
            SHMServer will be hosted on ${SHMSERVER_PORT_ENV} (default {SHMSERVER_PORT_DEFAULT}).
            Publishers will be assigned available ports starting from {SHMSERVER_PORT_ENV}.
        """
    )

    parser.add_argument(
        '-s', '--shutdown', 
        help = 'shutdown ezmsg core servers', 
        action = 'store_true'
    )

    parser.add_argument(
        '--hostname',
        type = str,
        help = 'hostname for GraphServer',
        default = '127.0.0.1'
    )

    class Args:
        shutdown: bool
        hostname: str

    logger.info(f'{GRAPHSERVER_PORT=}')
    logger.info(f'{SHMSERVER_PORT=}')

    args = parser.parse_args(namespace=Args)
    address = Address(args.hostname, GRAPHSERVER_PORT)

    if args.shutdown:
        try:
            async with GraphServer.connection(address) as connection:
                connection.shutdown()
            logger.info(f'Shutdown GraphServer running @{address}')
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(f'GraphServer not running @{address}, or host is refusing connections')
            
        try:
            await SHMServer.shutdown_server()
            logger.info('Shutdown SHMServer')
        except ConnectionRefusedError:
            logger.info('SHMServer not running, or is refusing connections')

    else:
        graph_server = None
        shm_server = await SHMServer.ensure_running()
        try:
            graph_server = await GraphServer.ensure_running(address)

            if graph_server is None:
                logger.info(f'GraphServer already running @ {address}')
            else:
                graph_server.join()
        
        except KeyboardInterrupt:
            logger.info('Interrupt detected; shutting down servers')

        finally:
            if graph_server is not None:
                graph_server._shutdown.set()
                graph_server.join()

            if shm_server is not None:
                shm_server._shutdown.set()
                shm_server.join()
        

asyncio.run(main())


    