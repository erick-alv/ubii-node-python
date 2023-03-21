import argparse
import asyncio
import logging
from pathlib import Path
from warnings import warn

log = logging.getLogger(__name__)


def import_pm(import_name: str):
    module, attribute = import_name.rsplit('.', maxsplit=1)
    assert module
    assert attribute
    from importlib import import_module
    module = import_module(module)
    return getattr(module, attribute)


def log_to_folder(log_config):
    for name, config in log_config['handlers'].items():
        log_file = config.get('filename', None)
        if log_file is None:
            continue

        if not log_file.startswith('logs/'):
            log_file = Path(f"logs/{log_file}")
            config['filename'] = str(log_file)

        log_file.parent.mkdir(parents=True, exist_ok=True)

    return log_config


def main():
    from ubii.node import connect_client
    from ubii.framework.logging import parse_args, logging_setup
    from ubii.framework.client import InitProcessingModules
    from codestare.async_utils.nursery import TaskNursery
    #from . import load_pm_entry_points # for some reason when trying to run this on pycharm it dies not work
    from ubii.cli import load_pm_entry_points
    parser = argparse.ArgumentParser()
    parser.add_argument('--processing-modules', action='append', default=[])
    parser.add_argument('--no-discover', action='store_true', default=False)

    args = parse_args(parser=parser)

    log_config = logging_setup.change(config=log_to_folder(args.log_config))

    pms = set()
    if not args.no_discover:
        pms.update(load_pm_entry_points())
    if args.processing_modules:
        pms.update(import_pm(name) for name in args.processing_modules)

    if pms:
        print(f"Imported {', '.join(map(repr, pms))}")
    else:
        warn(f"No processing modules imported")

    async def run():
        with connect_client() as client:
            client.is_dedicated_processing_node = True
            assert client.implements(InitProcessingModules)
            client[InitProcessingModules].late_init_processing_modules = pms
            await client

            while client.state != client.State.UNAVAILABLE:
                await asyncio.sleep(1)

        loop.stop()

    loop = asyncio.get_event_loop_policy().get_event_loop()
    nursery = TaskNursery(name="__main__", loop=loop)

    with log_config:
        nursery.create_task(run())
        loop.run_forever()

    assert not loop.is_running()
    loop.close()


if __name__ == '__main__':
    main()
