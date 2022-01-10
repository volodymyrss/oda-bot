import logging
import time
import yaml
import tempfile
import click
import subprocess
from click.core import Context
import requests

logger = logging.getLogger()

@click.group()
def cli():
    logging.basicConfig(
        level=logging.INFO,
        format='\033[36m%(asctime)s %(levelname)s %(module)s\033[0m  %(message)s',
    )
    logger.info("default logging level INFO")


@cli.command()
@click.option('--branch', default="master")
def update_dispatcher_chart(branch):    
    with tempfile.TemporaryDirectory() as dispatcher_chart_dir:
        subprocess.check_call([
            "git", "clone", 
            "git@gitlab.astro.unige.ch:oda/dispatcher/dispatcher-chart.git",
            dispatcher_chart_dir,
            "--recurse-submodules",
            "--depth", "1", #?
        ])

        try:
            subprocess.check_call([
                "make", "-C", dispatcher_chart_dir, "update"
            ])
            subprocess.check_call([
                "git", "push", "origin", branch
            ])
        except subprocess.CalledProcessError as e:
            logger.warning('can not update (maybe no updates available?): %s', e)


@cli.command()
@click.option('--source', default="orgs/oda-hub")
@click.option('--forget', is_flag=True, default=False)
@click.pass_context
def poll_github_events(ctx, source, forget):
    min_poll_interval_s = 5
    poll_interval_s = 60

    try:
        last_event_id = yaml.safe_load(open('oda-bot-runtime.yaml'))[source]['last_event_id']
        logger.info('found saved last event record %s', last_event_id)
    except:
        logger.warning('no saved last event record')
        last_event_id = 0

    if forget:
        logger.warning('discarding last event record')
        last_event_id = 0

    while True:
        try:
            r = requests.get(f'https://api.github.com/{source}/events')
        except Exception as e:
            logger.warning("problem with connection! sleeping %s s", min_poll_interval_s)
            time.sleep(min_poll_interval_s)
            continue
        
        if int(r.headers['X-RateLimit-Remaining']) > 0:
            poll_interval_s = (int(r.headers['X-RateLimit-Reset']) - time.time()) / int(r.headers['X-RateLimit-Remaining'])
            logger.info('max average poll interval %.2f s', poll_interval_s)
            poll_interval_s = max(min_poll_interval_s, poll_interval_s)            
        else:
            logger.warning('rate limit exceeded! %d', int(r.headers['X-RateLimit-Remaining']))
            poll_interval_s = (int(r.headers['X-RateLimit-Reset']) - time.time())

        events = r.json()

        n_new_push_events = 0
        new_last_event_id = last_event_id

        for event in events:
            if int(event['id']) > last_event_id:
                new_last_event_id = max(int(event['id']), new_last_event_id)
                logger.info("new event %s: %s %s %s %s", event['id'], event['repo']['name'], event['type'], event['payload'].get('ref', None), event['created_at'])
                if event['type'] == 'PushEvent' and event['payload']['ref'] in ['refs/heads/master', 'refs/heads/main'] and \
                   ('dispatcher' in event['repo']['name'] or 'oda_api' in event['repo']['name'] or 'oda_api' in event['repo']['name']):
                    n_new_push_events += 1

        if n_new_push_events > 0:
            logger.info("got %s new push events, will update")
            ctx.invoke(update_dispatcher_chart)

        logger.debug('returned last event ID %s', last_event_id)

        if new_last_event_id > last_event_id:
            yaml.dump({source: dict(last_event_id=new_last_event_id)}, open('oda-bot-runtime.yaml', "w"))
            last_event_id = new_last_event_id
            logger.info('new last event ID %s', last_event_id)
        
        time.sleep(min_poll_interval_s)            

if __name__ == "__main__":
    cli()
