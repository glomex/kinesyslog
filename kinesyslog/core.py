import logging
from asyncio import gather, get_event_loop, CancelledError, Task
from functools import partial
from tempfile import gettempdir

import click
import signal

from .server import DatagramSyslogServer, SecureSyslogServer, SyslogServer
from .sink import MessageSink
from .spool import EventSpool


def shutdown_exception_handler(loop, context):
    if "exception" not in context or not isinstance(context["exception"], CancelledError):
        loop.default_exception_handler(context)


@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug logging to STDERR.'
)
@click.option(
    '--profile',
    type=str,
    help='Use a specific profile from your credential file.'
)
@click.option(
    '--region',
    type=str,
    help='The region to use. Overrides config/env settings.'
)
@click.option(
    '--spool-dir',
    type=click.Path(exists=True, file_okay=False),
    help='Spool directory for compressed records prior to upload.',
    default=gettempdir(),
    show_default=True
)
@click.option(
    '--udp-port',
    type=int,
    help='Bind port for UDP listener; 0 to disable.',
    default=0,
    show_default=True,
)
@click.option(
    '--tcp-port',
    type=int,
    help='Bind port for TCP listener; 0 to disable.',
    default=0,
    show_default=True,
)
@click.option(
    '--key',
    type=click.Path(dir_okay=False),
    help='Private key file for TLS listener.',
    default='localhost.key',
    show_default=True,
)
@click.option(
    '--cert',
    type=click.Path(dir_okay=False),
    help='Certificate file for TLS listener.',
    default='localhost.crt',
    show_default=True,
)
@click.option(
    '--port',
    type=int,
    help='Bind port for TLS listener; 0 to disable.',
    default=6514,
    show_default=True,
)
@click.option(
    '--address',
    type=str,
    help='Bind address.',
    default='0.0.0.0',
    show_default=True,
)
@click.option(
    '--stream',
    type=str,
    help='Kinesis Firehose Delivery Stream Name.',
    required=True,
)
@click.command(short_help='List for incoming Syslog messages and submit to Kinesis Firehose')
def listen(**args):
    logging.basicConfig(level='INFO', format='%(asctime)-15s [%(process)d:%(thread)d] %(levelname)s:%(name)s:%(message)s')
    loop = get_event_loop()
    loop.set_exception_handler(shutdown_exception_handler)

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), partial(loop.stop))

    if args.get('debug', False):
        logging.getLogger('kinesyslog').setLevel('DEBUG')
        logging.getLogger('asyncio').setLevel('INFO')
        loop.set_debug(True)
    else:
        logging.getLogger('botocore').setLevel('ERROR')

    servers = []
    if args.get('port', 0):
        servers.append(SecureSyslogServer(host=args['address'], port=args['port'], certfile=args['cert'], keyfile=args['key']))
    if args.get('tcp_port', 0):
        servers.append(SyslogServer(host=args['address'], port=args['tcp_port']))
    if args.get('udp_port', 0):
        servers.append(DatagramSyslogServer(host=args['address'], port=args['udp_port']))

    if not servers:
        return

    with EventSpool(delivery_stream=args['stream'], spool_dir=args['spool_dir']) as e:
        with MessageSink(spool=e) as m:
            try:
                for server in servers:
                    loop.run_until_complete(server.start_server(sink=m))
                loop.run_forever()
            except KeyboardInterrupt:
                tasks = gather(*Task.all_tasks(loop=loop), loop=loop, return_exceptions=True)
                tasks.add_done_callback(partial(loop.stop))
                tasks.cancel()
                while not tasks.done() and not loop.is_closed():
                    loop.run_forever()
            finally:
                loop.close()
