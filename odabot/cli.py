#!/usr/bin/python

import json
import logging
import os
import re
import time
import yaml
import tempfile
import click
import subprocess
from click.core import Context
import requests
import dynaconf
from dateutil import parser
from datetime import datetime

import rdflib

from nb2workflow.deploy import deploy

logger = logging.getLogger()

import logging_tree

from dynaconf import Dynaconf

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load this files in the order.

@click.group()
@click.option('--debug', is_flag=True)
@click.pass_obj
def cli(obj, debug):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='\033[36m%(asctime)s %(levelname)s %(module)s\033[0m  %(message)s',
    )
    logger.info("default logging level INFO")

    obj['settings'] = Dynaconf(
        envvar_prefix="ODABOT",
        settings_files=[
            'settings.toml', 
            '.secrets.toml',
            os.path.join(os.getenv('HOME'), '.config/oda/bot/settings.toml')
        ],
    )


    logger.info("components: %s", obj['settings'].components)


@cli.command()
@click.option('--branch', default="master")
@click.argument('component')
def update_chart(component, branch):    
    with tempfile.TemporaryDirectory() as chart_dir:
        subprocess.check_call([
            "git", "clone", 
            f"git@gitlab.astro.unige.ch:oda/{component}/{component}-chart.git",
            chart_dir,
            "--recurse-submodules",
            "--depth", "1", #?
        ])
        # subprocess.check_call([
        #     "git", "config", "commit.gpgsign", "false"
        #     ])

        try:
            r = subprocess.check_call([
                    "make", "-C", chart_dir, "update"
                ],
                env={**os.environ, 
                     'GIT_CONFIG_COUNT': '1', 
                     'GIT_CONFIG_KEY_0': 'commit.gpgsign',
                     'GIT_CONFIG_VALUE_0': 'false'}
            )
            logger.error('\033[32msucceeded update (next to commit): %s\033[0m', r)
            r = subprocess.check_call([
                "git", "push", "origin", branch
            ])
        except subprocess.CalledProcessError as e:
            logger.error('\033[31mcan not update (maybe no updates available?): %s\033[0m', e)


@cli.command()
@click.option('--source', default="orgs/oda-hub")
@click.option('--forget', is_flag=True, default=False)
@click.pass_context
@click.pass_obj # in context too
def poll_github_events(obj, ctx, source, forget):
    min_poll_interval_s = 5
    poll_interval_s = 60
        
    logger.info('staring oda-bot')

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
            poll_interval_s = (int(r.headers['X-RateLimit-Reset']) - time.time())
            logger.warning('rate limit exceeded! %d reset in %d', int(r.headers['X-RateLimit-Remaining']), poll_interval_s)
            time.sleep(poll_interval_s)
            continue

        
        if r.status_code != 200:
            logger.error(r.text)
            time.sleep(10)
            continue

        events = r.json()


        n_new_push_event_by_component = {n: 0 for n in obj['settings']['components']}
        new_last_event_id = last_event_id

        for event in events:
            if int(event['id']) > last_event_id:
                new_last_event_id = max(int(event['id']), new_last_event_id)
                logger.info("new event %s: %s %s %s %s", event['id'], event['repo']['name'], event['type'], event['payload'].get('ref', None), event['created_at'])
                if event['type'] == 'PushEvent' and event['payload']['ref'] in ['refs/heads/master', 'refs/heads/main']:
                    for n, comp in obj['settings']['components'].items():
                        #('dispatcher' in event['repo']['name'] or 'oda_api' in event['repo']['name'] or 'oda_api' in event['repo']['name']):
                        if any([re.search(trig, event['repo']['name']) for trig in comp['triggers']]) and event['repo']['name'] != f'{n}-chart':
                            n_new_push_event_by_component[n] += 1

        for n, comp in obj['settings']['components'].items():
            if n_new_push_event_by_component[n] > 0:
                logger.info("\033[33mgot %s new push events for %s, will update\033[0m", n_new_push_event_by_component[n], n)
                ctx.invoke(update_chart, component=n)

        logger.debug('returned last event ID %s', last_event_id)

        if new_last_event_id > last_event_id:
            yaml.dump({source: dict(last_event_id=new_last_event_id)}, open('oda-bot-runtime.yaml', "w"))
            last_event_id = new_last_event_id
            logger.info('new last event ID %s', last_event_id)
        
        time.sleep(min_poll_interval_s)            

@cli.command()
@click.option("--dry-run", is_flag=True)
def update_workflows(dry_run):
    import odakb
    # TODO: config

    try:
        oda_bot_runtime = yaml.safe_load(open('oda-bot-runtime.yaml'))
    except FileNotFoundError:
        oda_bot_runtime = {}

    if "deployed_workflows" not in oda_bot_runtime:
        oda_bot_runtime["deployed_workflows"] = {}

    deployed_workflows = oda_bot_runtime["deployed_workflows"]

    for project in requests.get('https://renkulab.io/gitlab/api/v4/groups/5606/projects?include_subgroups=yes').json():
        last_activity_time = parser.parse(project['last_activity_at'])
        age = (time.time() - last_activity_time.timestamp())/24/3600

        logger.info("%20s %30s %10g ago %s", project['name'], last_activity_time, age, project['http_url_to_repo'])
        logger.info("%20s", project['topics'])
        logger.debug("%s", json.dumps(project))

        if 'live-workflow' in project['topics']:
            saved_last_activity_timestamp = deployed_workflows.get(project['http_url_to_repo'], {}).get('last_activity_timestamp', 0)

            if last_activity_time.timestamp() <= saved_last_activity_timestamp:
                logger.info("no need to deploy this workflow")
            else:
                if dry_run:
                    logger.info("would deploy this workflow")
                else:
                    logger.info("will deploy this workflow")
                    deployment_namespace = "oda-staging"
                    deployment_name = deploy(project['http_url_to_repo'], project['name'] + '-workflow', namespace=deployment_namespace)
                    odakb.sparql.insert(f'''
                        {rdflib.URIRef(project['http_url_to_repo']).n3()} a oda:WorkflowService;
                                                                        oda:last_activity_timestamp {last_activity_time.timestamp()};
                                                                        oda:last_deployed_timestamp {datetime.now().timestamp()};
                                                                        oda:service_name "{project['name']}";
                                                                        oda:deployment_namespace "{deployment_namespace}";
                                                                        oda:deployment_name "{deployment_name}" .  
                    ''')
                    deployed_workflows[project['http_url_to_repo']] = {'last_activity_timestamp': last_activity_time.timestamp()}
    
    yaml.dump(oda_bot_runtime, open('oda-bot-runtime.yaml', "w"))


@cli.command()
def verify_workflows():
    import odakb
    import oda_api.api
    api = oda_api.api.DispatcherAPI(url="https://dispatcher-staging.obsuks1.unige.ch")
    logger.info(api.get_instruments_list())

    for r in odakb.sparql.construct('?w a oda:WorkflowService; ?b ?c', jsonld=True):        
        logger.info("%s: %s", r['@id'], json.dumps(r, indent=4))
        
        api.get_instrument_description(r["http://odahub.io/ontology#service_name"][0]['@value'])
        

#TODO:  test service status and dispatcher status
# oda-api -u https://dispatcher-staging.obsuks1.unige.ch get -i cta-example


if __name__ == "__main__":
    cli(obj={})
