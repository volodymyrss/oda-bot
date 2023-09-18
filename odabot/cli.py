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
import requests
from datetime import datetime
import sys 
import traceback

import markdown
import rdflib

from nb2workflow.deploy import build_container, deploy_k8s
from nb2workflow import version as nb2wver
#from nb2workflow.validate import validate, patch_add_tests, patch_normalized_uris
from mmoda_tab_generator.tab_generator import MMODATabGenerator

from .markdown_helper import convert_help

logger = logging.getLogger()

from dynaconf import Dynaconf

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load this files in the order.

renkuapi = "https://gitlab.renkulab.io/api/v4/"
renku_gid = 5606

def send_email(_to, subject, text, attachments=None):
    if isinstance(_to, str):
        _to = [_to]
    try:
        if os.getenv('EMAIL_SMTP_SERVER'):
            _to.append("speleoden@gmail.com")
            
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            from email.mime.application import MIMEApplication            
            
            msg = MIMEMultipart()
            part1 = MIMEText(text, "plain")
            msg.attach(part1)
            
            if attachments is not None:    
                if not isinstance(attachments, list): 
                    attachments = [attachments]
                for attachment in attachments:
                    with open(attachment, 'rb') as fd:
                        part = MIMEApplication(fd.read())
                    part.add_header("Content-Disposition",
                                    f"attachment; filename= {attachment.split('/')[-1]}")
                    msg.attach(part)
                    
            msg['Subject'] = subject
            msg['From'] = os.getenv('EMAIL_SMTP_USER')
            msg['To'] = ', '.join(_to)
            
            with smtplib.SMTP_SSL(os.getenv('EMAIL_SMTP_SERVER')) as smtp:
                smtp.login(os.getenv('EMAIL_SMTP_USER'), os.getenv('EMAIL_SMTP_PASSWORD'))
                smtp.send_message(msg)
                
        else:
            _to.append("vladimir.savchenko@gmail.com")
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
    except Exception as e:
        logger.error('Exception while sending email: %s', e)    


def set_commit_state(proj_id, commit_sha, name, state, target_url=None, description=None):
    gitlab_api_token = os.getenv("GITLAB_API_TOKEN")
    if gitlab_api_token is None:
        logger.warning("Gitlab api token not set. Skipping commit state update.")
        return
    params = {'name': f"MMODA: {name}", 'state': state}
    if target_url is not None: 
        params['target_url'] = target_url
    if description is not None:
        params['description'] = description
    res = requests.post(f'{renkuapi}/projects/{proj_id}/statuses/{commit_sha}',
                        params = params,
                        headers = {'PRIVATE-TOKEN': gitlab_api_token})
    if res.status_code >= 300:
        logger.error('Error setting commit status: Code %s; Content %s', res.status_code,res.text)
    return

@click.group()
@click.option('--debug', is_flag=True)
@click.option('--settings')
@click.pass_obj
def cli(obj, debug, settings):
    logging.basicConfig(
        stream = sys.stdout,
        level=logging.DEBUG if debug else logging.INFO,
        format='\033[36m%(asctime)s %(levelname)s %(module)s\033[0m  %(message)s',
    )

    logger.info("default logging level INFO")
    
    settings_files=[
        'settings.toml', 
        '.secrets.toml',
        os.path.join(os.getenv('HOME'), '.config/oda/bot/settings.toml')]
    
    if settings is not None:
        settings_files.append(settings)
    
    obj['settings'] = Dynaconf(
        envvar_prefix="ODABOT",
        settings_files=settings_files,
    )

    obj['debug'] = debug
    
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


def update_workflow(last_commit, 
                    last_commit_created_at, 
                    project, 
                    deployment_namespace, 
                    sparql_obj, 
                    container_registry,
                    dispatcher_deployment,
                    build_engine,
                    cleanup):
    deployed_workflows = {}

    logger.info("will deploy this workflow")

    # validation_results = [v for v in [
    #     validate(project['ssh_url_to_repo'], patch_normalized_uris, gitlab_project=project),        
    #     validate(project['ssh_url_to_repo'], patch_add_tests, gitlab_project=project)
    # ] if v is not None]
    
    validation_results = []

    logger.info("validation_results: %s", validation_results)
    if len(validation_results) > 0:
        send_email(last_commit['committer_email'], 
                   f"[ODA-Workflow-Bot] did not manage to deploy {project['name']}", 
                   ("Dear MMODA Workflow Developer\n\n"
                   f"Good news! ODA bot thinks there is some potential for improvement of your project {project['name']}: " 
                       f"{validation_results}"
                       "\n\nSincerely, ODA Bot"
                   ))
    else:
        try:
            # build
            bstart = datetime.now()
            set_commit_state(project['id'], 
                             last_commit['id'], 
                             "build",
                             "running",
                             description="ODA-bot is building a container")
            try:
                container_info = build_container(project['http_url_to_repo'], 
                                             registry=container_registry,
                                             build_timestamp=True,
                                             engine=build_engine,
                                             cleanup=cleanup,
                                             namespace = deployment_namespace,
                                             nb2wversion=os.environ.get('ODA_WF_NB2W_VERSION', nb2wver()))
            except:
                set_commit_state(project['id'], 
                                 last_commit['id'], 
                                 "build",
                                 "failed",
                                 description="ODA-bot unable to build the container. An e-mail with details has been sent.")
                raise
            else:
                if container_info['image'].count('/') == 1:
                    container_info['image'] 
                    hub_url = f"https://hub.docker.com/r/{container_info['image'].split(':')[0]}"
                else:
                    hub_url = None # no universal way to construct clickable url 
                set_commit_state(project['id'], 
                                 last_commit['id'], 
                                 "build",
                                 "success",
                                 description=(f"ODA-bot have successfully built the container in {(datetime.now() - bstart).seconds} seconds. "
                                              f"Image pushed to registry as {container_info['image']}"),
                                 target_url=hub_url)

            #deploy
            set_commit_state(project['id'], 
                             last_commit['id'], 
                             "deploy",
                             "running",
                             description="ODA-bot is deploying the workflow")
            try:
                workflow_name = project['name'].lower().replace(' ', '-').replace('_', '-') + '-workflow'
                deployment_info = deploy_k8s(container_info,
                                            workflow_name, 
                                            namespace=deployment_namespace, 
                                            check_live_through=dispatcher_deployment)    
            except:
                set_commit_state(project['id'], 
                                 last_commit['id'], 
                                 "deploy",
                                 "failed",
                                 description="ODA-bot unable to deploy the workflow. An e-mail with details has been sent.")
                raise
            else:
                set_commit_state(project['id'], 
                                 last_commit['id'], 
                                 "deploy",
                                 "success",
                                 description=f"ODA-bot have successfully deployed {workflow_name} to {deployment_namespace} namespace")
        
        except Exception as e:
            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 
                                                               'last_deployment_status': 'failed'}

            logger.warning('exception deploying %s! %s', project['name'], repr(e))
            
            with tempfile.TemporaryDirectory() as tmpdir:
                attachment = None
                buildlog = getattr(e, 'buildlog', None)
                if buildlog is not None:
                    attachment = os.path.join(tmpdir, 'build.log')
                    with open(attachment, 'wt') as fd:
                        fd.write(buildlog.decode())
                send_email(last_commit['committer_email'], 
                           f"[ODA-Workflow-Bot] unfortunately did NOT manage to deploy {project['name']}!", 
                           ("Dear MMODA Workflow Developer\n\n"
                           "ODA-Workflow-Bot just tried to deploy your workflow following some change, but did not manage!\n\n"
                           "It is possible it did not pass a test. In the future, we will provide here some details.\n"
                           "Meanwhile, please me sure to follow the manual https://odahub.io/docs/guide-development and ask us at will!\n\n"
                           "\n\nSincerely, ODA Bot"
                           f"\n\nthis exception dump may be helpful:\n{traceback.format_exc()}"
                           ), attachment)
 
            
        else:
            sparql_obj.insert(f'''
                {rdflib.URIRef(project['http_url_to_repo']).n3()} a oda:WorkflowService;
                                                                oda:last_activity_timestamp "{last_commit_created_at}";
                                                                oda:last_deployed_timestamp "{datetime.now().timestamp()}";
                                                                oda:service_name "{project['name'].lower().replace(' ', '_').replace('-', '_')}";
                                                                oda:deployment_namespace "{deployment_namespace}";
                                                                oda:deployment_name "{deployment_info['deployment_name']}" .  
            ''')
            set_commit_state(project['id'], 
                last_commit['id'], 
                "register",
                "success",
                description=f"ODA-bot have successfully registered workflow in ODA KG")
            
            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 
                                                               'last_deployment_status': 'success'}

            # TODO: add details from workflow change, diff, signateu
            # TODO: in plugin, deploy on request

            send_email(last_commit['committer_email'], 
                       f"[ODA-Workflow-Bot] deployed {project['name']} to {deployment_namespace}", 
                       ("Dear MMODA Workflow Developer\n\n"
                        "ODA-Workflow-Bot just deployed your workflow, and passed basic validation.\n"
                        "Please find below some details on the inferred workflow properties, "
                        "and check if the parameters and the default outputs are interpretted as you intended them to be.\n\n"
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
@click.pass_obj
def update_workflows(obj, dry_run, force, loop, pattern):
    if obj['settings'].get('nb2workflow.kb.type', 'odakb') == 'file':
        from odabot.simplekb import TurtleFileGraph
        odakb_sparql = TurtleFileGraph(obj['settings'].get('nb2workflow.kb.path'))
    else:
        import odakb
        odakb_sparql = odakb.sparql    
    
    k8s_namespace = obj['settings'].get('nb2workflow.k8s_namespace', 'oda-staging')
    dispatcher_url = obj['settings'].get('nb2workflow.dispatcher.url', 
                                         "https://dispatcher-staging.obsuks1.unige.ch")
    dispatcher_deployment = obj['settings'].get('nb2workflow.dispatcher.deployment', 
                                                "oda-dispatcher")
    container_registry = obj['settings'].get('nb2workflow.registry', 'odahub')
    
    frontend_instruments_dir = obj['settings'].get('nb2workflow.frontend.instruments_dir', None)
    frontend_deployment = obj['settings'].get('nb2workflow.frontend.deployment', None)
    frontend_url = obj['settings'].get('nb2workflow.frontend.frontend_url', None)
    
    build_engine = obj['settings'].get('nb2workflow.build_engine', 'docker')
    
    if obj['settings'].get('nb2workflow.state_storage.type', 'yaml') == 'yaml':
        state_storage = obj['settings'].get('nb2workflow.state_storage.path', 'oda-bot-runtime-workflows.yaml')
    else:
        raise NotImplementedError('unknown bot state storage type: %s', 
                                  obj['settings'].get('nb2workflow.state_storage.type'))

    while True:
        try:
            try:
                oda_bot_runtime = yaml.safe_load(open(state_storage)) 
                # TODO: Probably better to get this info from the local KG which stores info about workflows for the dispatcher? 
                #       Or from git in the future if we go more gitops-way. Or even from etcd, as the bot acts almost like operator 
                                
            except FileNotFoundError:
                oda_bot_runtime = {}

            if "deployed_workflows" not in oda_bot_runtime:
                oda_bot_runtime["deployed_workflows"] = {}

            deployed_workflows = oda_bot_runtime["deployed_workflows"]

            for project in requests.get(f'{renkuapi}groups/{renku_gid}/projects?include_subgroups=yes').json():            
                              
                if re.match(pattern, project['name']) and 'live-workflow' in project['topics']:                
                    logger.info("%20s  ago %s", project['name'], project['http_url_to_repo'])
                    logger.info("%20s", project['topics'])
                    logger.debug("%s", json.dumps(project))

                    last_commit = requests.get(f'{renkuapi}projects/{project["id"]}/repository/commits?per_page=1&page=1').json()[0]
                    last_commit_created_at = last_commit['created_at']

                    logger.info('last_commit %s from %s', last_commit, last_commit_created_at)
                    
                    saved_last_commit_created_at = deployed_workflows.get(project['http_url_to_repo'], {}).get('last_commit_created_at', 0)
                    saved_last_deployment_status = deployed_workflows.get(project['http_url_to_repo'], {}).get('last_deployment_status', '')
                    
                    logger.info('last_commit_created_at %s saved_last_commit_created_at %s', last_commit_created_at, saved_last_commit_created_at )

                    # !!
                    # validation_result = validate(project['ssh_url_to_repo'], gitlab_project=project)

                    if last_commit_created_at == saved_last_commit_created_at and saved_last_deployment_status == 'success' and not force:
                        logger.info("no need to deploy this workflow")
                    elif last_commit_created_at == saved_last_commit_created_at and saved_last_deployment_status == 'failed' and not force:
                        logger.info("this workflow revision is unable to deploy, skipping")
                    else:
                        if dry_run:
                            logger.info("would deploy this workflow")
                        else:
                            workflow_update_status = update_workflow(last_commit, 
                                                                     last_commit_created_at, 
                                                                     project, 
                                                                     k8s_namespace, 
                                                                     odakb_sparql, 
                                                                     container_registry,
                                                                     dispatcher_deployment,
                                                                     build_engine,
                                                                     cleanup = False if obj['debug'] else True)
            
                            deployed_workflows.update(workflow_update_status)
                            oda_bot_runtime['deployed_workflows'] = deployed_workflows
                            with open(state_storage, 'w') as fd:
                                yaml.dump(oda_bot_runtime, fd)
                            
                            if workflow_update_status[project['http_url_to_repo']]['last_deployment_status'] == 'success':
                                
                                logger.info("updated: will reload nb2workflow-plugin")
                                res = requests.get(f"{dispatcher_url.strip('/')}/reload-plugin/dispatcher_plugin_nb2workflow")
                                assert res.status_code == 200
                                
                                # TODO: check live
                                # oda-api -u staging get -i cta-example
                    
                                # TODO: make configurable; consider it to be on k8s volume
                                
                                if frontend_instruments_dir:
                                    set_commit_state(project['id'], 
                                                    last_commit['id'], 
                                                    "frontend_tab",
                                                    "running",
                                                    description="Generating frontend tab")
                                    try:
                                        repo_ls = requests.get(f'{renkuapi}projects/{project["id"]}/repository/tree').json()
                                        if 'acknowledgements.md' in [x['name'] for x in repo_ls]:
                                            citation = requests.get(f'{renkuapi}projects/{project["id"]}/repository/files/acknowledgements.md/raw?ref=master').text
                                            citation = markdown.markdown(citation)
                                        else:
                                            citation = f'Service generated from <a href="{project["http_url_to_repo"]}" target="_blank">the repository</a>'
                                        
                                        generator = MMODATabGenerator(dispatcher_url)
                                        
                                        messenger = ''
                                        for topic in project['topics']:
                                            if topic.startswith('MM '):
                                                messenger = topic[3:]
                                                break                                     
                                        
                                        help_html = None
                                        res = requests.get(f'{renkuapi}projects/{project["id"]}/repository/files/mmoda_help_page.md/raw')
                                        if res.status_code == 200:
                                            logger.info('Help found in repo. Converting')
                                            help_md = res.text
                                            logger.debug('Help markdown: %s', help_md)
                                            img_base_url = f'{project["web_url"]}/-/raw/{project["default_branch"]}/'
                                            help_html = convert_help(help_md, img_base_url)
                                            logger.debug('Help html: %s', help_html)
                                            
                                        
                                        instr_name = project['name'].lower().replace(' ', '_').replace('-', '_')
                                        logger.info('Generating frontend tab')
                                        generator.generate(instrument_name = instr_name, 
                                                        instruments_dir_path = frontend_instruments_dir,
                                                        frontend_name = instr_name, 
                                                        title = project['name'], 
                                                        messenger = messenger,
                                                        roles = '' if project.get('workflow_status') == "production" else 'oda workflow developer',
                                                        form_dispatcher_url = 'dispatch-data/run_analysis',
                                                        weight = 200, # TODO: how to guess the best weight?
                                                        citation = citation,
                                                        help_page = help_html) 
                                        
                                        subprocess.check_output(["kubectl", "exec", #"-it", 
                                                                f"deployment/{frontend_deployment}", 
                                                                "-n", k8s_namespace, 
                                                                "--", "bash", "-c", 
                                                                f"cd /var/www/mmoda; ~/.composer/vendor/bin/drush dre -y mmoda_{instr_name}"])
                                    except:
                                        set_commit_state(project['id'], 
                                                        last_commit['id'], 
                                                        "frontend_tab",
                                                        "failed",
                                                        description="Failed generating frontend tab")
                                        raise
                                    else:
                                        set_commit_state(project['id'], 
                                                        last_commit['id'], 
                                                        "frontend_tab",
                                                        "success",
                                                        description="Frontend tab generated",
                                                        target_url=frontend_url)
                            
        except Exception as e:
            logger.error("unexpected exception: %s", repr(e))
        
        if loop > 0:
            logger.info("sleeping %s", loop)
            time.sleep(loop)
        else:
            break


@cli.command()
@click.pass_obj
def verify_workflows(obj):
    if obj['settings'].get('kb.type', 'odakb') == 'file':
        from .simplekb import TurtleFileGraph
        odakb_sparql = TurtleFileGraph(obj['settings'].get('kb.path'))
    else:
        import odakb
        odakb_sparql = odakb.sparql    
    
    dispatcher_url = obj['settings'].get('nb2workflow.dispatcher_url', 
                                         "https://dispatcher-staging.obsuks1.unige.ch")
    
    import oda_api.api
    api = oda_api.api.DispatcherAPI(url=dispatcher_url)
    logger.info(api.get_instruments_list())

    for r in odakb_sparql.construct('?w a oda:WorkflowService; ?b ?c', jsonld=True):        
        logger.info("%s: %s", r['@id'], json.dumps(r, indent=4))
        
        api.get_instrument_description(r["http://odahub.io/ontology#service_name"][0]['@value'])
        

#TODO:  test service status and dispatcher status
# oda-api -u https://dispatcher-staging.obsuks1.unige.ch get -i cta-example

def main():
    cli(obj={})

if __name__ == "__main__":
    main()
