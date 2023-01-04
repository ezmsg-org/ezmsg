import asyncio
import argparse
import socket
import traceback

from .graphserver import GraphServer
from .shmserver import SHMServer
from .netprotocol import (
    Address,
    GRAPHSERVER_PORT_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    GRAPHSERVER_PORT,
    GRAPHSERVER_ADDR,
    SHMSERVER_PORT_ENV,
    SHMSERVER_PORT_DEFAULT,
    SHMSERVER_PORT
)

from . import logger

# Port forwarding implementation shamelessly ripped from 
# https://github.com/medram/port-forwarding/blob/with-asyncio/port_forwarding.py

BUFFER_SIZE=4096

async def tunnel(src: socket.socket, dst: socket.socket) -> None:
    loop = asyncio.get_running_loop()
    while True:
        buffer = await loop.sock_recv(src, BUFFER_SIZE)
        if buffer:
            await loop.sock_sendall(dst, buffer)
        else:
            break
    logger.info('GraphServer forwarding terminated!')


async def accept(server: socket.socket, graph_address: Address):
    '''Accept server connection'''
    loop = asyncio.get_running_loop()

    client, addr = await loop.sock_accept(server)
    client.setblocking(False)
    # Establishing a connection to destination.
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.setblocking(False)
    await loop.sock_connect(conn, (graph_address.host, graph_address.port))

    logger.info(f'GraphServer forwarding established: {client.getsockname()} -> ? -> {conn.getpeername()}')

    # piping data
    loop.create_task(tunnel(client, conn))
    loop.create_task(tunnel(conn, client))


async def forward(local_address: Address, graph_address: Address):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(False)
        server.bind(local_address)
        server.listen()

        logger.info(f'Forwarding GraphServer @ {local_address}')

        while True:
            try:
                await accept(server, graph_address)

            except ConnectionRefusedError:
                logger.error('Destination connection refused.')
            except OSError:
                logger.error('socket.connect() error!')
                logger.error(traceback.format_exc())
            except Exception:
                logger.error(traceback.format_exc())


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
        '-f', '--forward',
        help = 'forward ezmsg GraphServer from HOSTNAME',
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
        forward: bool
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
            if args.forward:
                await GraphServer.open(address)
                await forward(Address(*GRAPHSERVER_ADDR), address)
            else:
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


    