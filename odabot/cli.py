#!/usr/bin/python

import json
import odakb
import logging
import os
import re
import time
import yaml
import tempfile
import click
import subprocess
import requests
from datetime import datetime

import rdflib

from nb2workflow.deploy import deploy
#from nb2workflow.validate import validate, patch_add_tests, patch_normalized_uris

logger = logging.getLogger()

from dynaconf import Dynaconf

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load this files in the order.


def send_email(_to, subject, text):
    r = requests.post(
        "https://api.eu.mailgun.net/v3/in.odahub.io/messages",
        data={
            "from": 'ODA Workflow Bot <oda-bot@in.odahub.io>',
            "to": _to,
            "subject": subject,
            "text": text
        },
        auth=('api', open(os.path.join(os.getenv('HOME', '/'), '.mailgun')).read().strip())
    )    
    logger.info('sending email: %s %s', r, r.text)
    

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
            url = f'https://api.github.com/{source}/events'
            logger.info("requesting %s", url)
            r = requests.get(f'https://api.github.com/{source}/events')
            print(r, r.text)
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
            time.sleep(max(poll_interval_s, 10))
            continue

        
        if r.status_code != 200:
            logger.error(r.text)
            time.sleep(max(poll_interval_s, 10))
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
        
        time.sleep(poll_interval_s)            


def update_workflow(last_commit, last_commit_created_at, project):
    deployed_workflows = {}

    logger.info("will deploy this workflow")
    deployment_namespace = "oda-staging"

    # validation_results = [v for v in [
    #     validate(project['ssh_url_to_repo'], patch_normalized_uris, gitlab_project=project),        
    #     validate(project['ssh_url_to_repo'], patch_add_tests, gitlab_project=project)
    # ] if v is not None]
    
    validation_results = []

    logger.info("validation_results: %s", validation_results)
    if len(validation_results) > 0:
        send_email([
                            "vladimir.savchenko@gmail.com", 
                            last_commit['committer_email']
                        ], 
                    f"[ODA-Workflow-Bot] did not manage to deploy {project['name']}", 
                    ("Dear MMODA Workflow Developer\n\n"
                    f"Good news! ODA bot thinks there is some potential for improvement of your project {project['name']}: " 
                        f"{validation_results}"
                        "\n\nSincerely, ODA Bot"
                    ))
    else:
        try:
            deployment_info = deploy(project['http_url_to_repo'], project['name'] + '-workflow', namespace=deployment_namespace)
        except Exception as e:
            logger.warning('exception deploying! %s\n%s\%s', e, e.output.decode(), e.stderr.decode())
            send_email([
                            "vladimir.savchenko@gmail.com", 
                            # deployment_info['author'],
                            last_commit['committer_email']
                        ], 
                    f"[ODA-Workflow-Bot] unfortunately did NOT manage to deploy {project['name']}!", 
                    ("Dear MMODA Workflow Developer\n\n"
                        "ODA-Workflow-Bot just tried to deploy your workflow following some change, but did not manage!\n\n"
                        "It is possible it did not pass a test. In the future, we will provide here some details.\n"
                        "Meanwhile, please me sure to follow the manual https://odahub.io/docs/guide-development and ask us at will!\n\n"
                        "\n\nSincerely, ODA Bot"
                        f"\n\nthis exception dump may be helpful: {repr(e)}\n\n{getattr(e, 'stderr', '').decode()}"
                        ))

            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 'last_deployment_status': 'failed'}
            
        else:
            odakb.sparql.insert(f'''
                {rdflib.URIRef(project['http_url_to_repo']).n3()} a oda:WorkflowService;
                                                                oda:last_activity_timestamp "{last_commit_created_at}";
                                                                oda:last_deployed_timestamp "{datetime.now().timestamp()}";
                                                                oda:service_name "{project['name']}";
                                                                oda:deployment_namespace "{deployment_namespace}";
                                                                oda:deployment_name "{deployment_info['deployment_name']}" .  
            ''')
            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 'last_deployment_status': 'success'}

            # TODO: add details from workflow change, diff, signateu
            # TODO: in plugin, deploy on request

            send_email([
                            "vladimir.savchenko@gmail.com", 
                            # deployment_info['author'],
                            last_commit['committer_email']
                        ], 
                    f"[ODA-Workflow-Bot] deployed {project['name']} to {deployment_namespace}", 
                    ("Dear MMODA Workflow Developer\n\n"
                        "ODA-Workflow-Bot just deployed your workflow, and passed basic validation.\n"
                        "Please find below some details on the inferred workflow properties, "
                        "and check if the parameters and the default outputs are interpretted as you intended them to beЖ.\n\n"
                        f"{json.dumps(deployment_info, indent=4)}\n\n"
                        "You can now try using accessing your workflow, see https://odahub.io/docs/guide-development/#optional-try-a-test-service\n\n"
                        "\n\nSincerely, ODA Bot"
                        ))

    return deployed_workflows            
       


@cli.command()
@click.option("--dry-run", is_flag=True)
@click.option("--loop", default=0)
@click.option("--force", is_flag=True)
@click.option("--pattern", default=".*")
def update_workflows(dry_run, force, loop, pattern):
    # TODO: config

    while True:
        try:
            try:
                oda_bot_runtime = yaml.safe_load(open('oda-bot-runtime-workflows.yaml'))
            except FileNotFoundError:
                oda_bot_runtime = {}

            if "deployed_workflows" not in oda_bot_runtime:
                oda_bot_runtime["deployed_workflows"] = {}

            deployed_workflows = oda_bot_runtime["deployed_workflows"]

            updated = False

            for project in requests.get('https://renkulab.io/gitlab/api/v4/groups/5606/projects?include_subgroups=yes').json():            

                if re.match(pattern, project['name']) and 'live-workflow' in project['topics']:                
                    logger.info("%20s  ago %s", project['name'], project['http_url_to_repo'])
                    logger.info("%20s", project['topics'])
                    logger.debug("%s", json.dumps(project))

                    last_commit = requests.get(f'https://renkulab.io/gitlab/api/v4/projects/{project["id"]}/repository/commits?per_page=1&page=1').json()[0]
                    last_commit_created_at = last_commit['created_at']

                    logger.info('last_commit %s from %s', last_commit, last_commit_created_at)
                    
                    saved_last_commit_created_at = deployed_workflows.get(project['http_url_to_repo'], {}).get('last_commit_created_at', 0)

                    logger.info('last_commit_created_at %s saved_last_commit_created_at %s', last_commit_created_at, saved_last_commit_created_at )

                    # !!
                    # validation_result = validate(project['ssh_url_to_repo'], gitlab_project=project)

                    if last_commit_created_at == saved_last_commit_created_at and not force:
                        logger.info("no need to deploy this workflow")
                    else:
                        if dry_run:
                            logger.info("would deploy this workflow")
                        else:
                            deployed_workflows.update(update_workflow(last_commit, last_commit_created_at, project))
                            updated = True    
            
            if updated:
                logger.info("updated: will recreated dispatcher")

                with open('oda-bot-runtime-workflows.yaml', "w") as f:
                    yaml.dump(oda_bot_runtime, f)            

                subprocess.check_output(["kubectl", "delete", "pods", "-n", "oda-staging", "-l", "app.kubernetes.io/name==dispatcher"])

                # TODO: check live
                # oda-api -u staging get -i cta-example

            if loop > 0:
                logger.info("sleeping %s", loop)
                time.sleep(loop)
            else:
                break
        except Exception as e:
            logger.error("unexpected exception: %s", e)
            time.sleep(5)


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
