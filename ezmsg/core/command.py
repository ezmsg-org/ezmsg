import asyncio
import argparse
import logging
import subprocess

from .graphserver import GraphServer
from .shmserver import SHMServer
from .netprotocol import (
    Address,
    AddressType,
    GRAPHSERVER_PORT_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    GRAPHSERVER_PORT,
    GRAPHSERVER_ADDR,
    SHMSERVER_PORT_ENV,
    SHMSERVER_PORT_DEFAULT,
    SHMSERVER_PORT
)

logger = logging.getLogger('ezmsg')

def cmdline() -> None:

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
        'command',
        help = 'command for ezmsg', 
        choices = ['serve', 'shutdown', 'start']
    )

    parser.add_argument(
        '--hostname',
        type = str,
        help = 'hostname for GraphServer',
        default = '127.0.0.1'
    )

    logger.info(f'{GRAPHSERVER_PORT=}; use ${GRAPHSERVER_PORT_ENV} to change.')
    logger.info(f'{SHMSERVER_PORT=}; use ${SHMSERVER_PORT_ENV} to change.')

    class Args:
        command: str
        hostname: str

    args = parser.parse_args(namespace=Args)

    asyncio.run(run_command(args.command, (args.hostname, GRAPHSERVER_PORT), **vars(args)))

async def run_command(cmd: str, address: AddressType = GRAPHSERVER_ADDR, **kwargs) -> None:

    address = Address(*address)

    if cmd == 'shutdown':
        try:
            await GraphServer.Connection(address).shutdown()
            logger.info(f'Shutdown GraphServer running @{address}')
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(f'GraphServer not running @{address}, or host is refusing connections')
            
        try:
            await SHMServer.shutdown_server()
            logger.info('Shutdown SHMServer')
        except ConnectionRefusedError:
            logger.info('SHMServer not running, or is refusing connections')

    elif cmd == 'serve':
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
                graph_server.stop()

            if shm_server is not None:
                shm_server.stop()

    elif cmd == 'start':
        popen = subprocess.Popen([
            "python", 
            "-m", "ezmsg.core", 
            "serve", 
            f"--hostname={address.host}"
        ])

        while True:
            try:
                await GraphServer.open(address)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.1)

        logger.info( f'Forked ezmsg servers.' )
        
    