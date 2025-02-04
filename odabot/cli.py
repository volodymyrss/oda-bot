#!/usr/bin/python

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
import yaml
import tempfile
import subprocess as sp
import requests
from datetime import datetime
import sys 
import traceback
import xml.etree.ElementTree as ET
from glob import glob

import click
from dynaconf import Dynaconf
from dynaconf.vendor.box import BoxList

import sentry_sdk

from odabot.notification_helpers import get_commit_state, send_email, set_commit_state

logger = logging.getLogger()

# will init if SENTRY_DSN is set
sentry = sentry_sdk.init()

try:
    import markdown
    import rdflib
    from nb2workflow.deploy import ContainerBuildException, NBRepo
    from nb2workflow import version as nb2wver
    #from nb2workflow.validate import validate, patch_add_tests, patch_normalized_uris
    from mmoda_tab_generator.tab_generator import MMODATabGenerator
    from .markdown_helper import convert_help
except ImportError:
    logger.warning('Deployment dependencies not loaded')

try:
    from nb2workflow.galaxy import to_galaxy    
    import frontmatter
except ImportError:
    logger.warning('Galaxy dependencies not loaded')

@click.group()
@click.option('--debug', is_flag=True)
@click.option('--settings')
@click.pass_obj
def cli(obj, debug, settings):
    logging.basicConfig(
        stream = sys.stdout,
        level=logging.DEBUG if debug else logging.INFO,
        format='\033[36m%(asctime)s %(levelname)s %(module)s\033[0m  %(message)s',
        force = True,
    )
    
    logger.info("logging level %s", 'INFO' if logger.level == 20 else 'DEBUG')
    
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


@cli.command()
@click.option('--branch', default="master")
@click.argument('component')
def update_chart(component, branch):    
    with tempfile.TemporaryDirectory() as chart_dir:
        sp.check_call([
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
            r = sp.check_call([
                    "make", "-C", chart_dir, "update"
                ],
                env={**os.environ, 
                     'GIT_CONFIG_COUNT': '1', 
                     'GIT_CONFIG_KEY_0': 'commit.gpgsign',
                     'GIT_CONFIG_VALUE_0': 'false'}
            )
            logger.error('\033[32msucceeded update (next to commit): %s\033[0m', r)
            r = sp.check_call([
                "git", "push", "origin", branch
            ])
        except sp.CalledProcessError as e:
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
    
    logger.info("components: %s", obj['settings'].components)

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
                    build_engine,
                    cleanup,
                    gitlab_api_url,
                    extra_emails=[],
                    creative_work_status="development",
                    gitlab_state_name_prefix="",
                    runtime_volume_config: dict|None = None,
                    email_notify_user: bool = True,
                    email_notify_admin: bool = True,
                    do_git_report: bool = True):
    deployed_workflows = {}
    deployment_info = None

    logger.info("will deploy this workflow")

    # validation_results = [v for v in [
    #     validate(project['ssh_url_to_repo'], patch_normalized_uris, gitlab_project=project),
    #     validate(project['ssh_url_to_repo'], patch_add_tests, gitlab_project=project)
    # ] if v is not None]

    validation_results = []

    logger.info("validation_results: %s", validation_results)
    if len(validation_results) > 0:
        
        to = last_commit['committer_email'] if email_notify_user else []
        bcc = extra_emails if email_notify_admin else []

        if to or bcc:
            send_email(to, 
                f"[ODA-Workflow-Bot] did not manage to deploy {project['name']}", 
                ("Dear MMODA Workflow Developer\n\n"
                f"Good news! ODA bot thinks there is some potential for improvement of your project {project['name']}: " 
                    f"{validation_results}"
                    "\n\nSincerely, ODA Bot"
                ),
                bcc=bcc
                )

    else:
        try:
            with NBRepo(
                repo_path=project['http_url_to_repo'],
                registry=container_registry
            ) as repo:
                # build
                bstart = datetime.now()
                if do_git_report: 
                    set_commit_state(
                        gitlab_api_url, 
                        project['id'], 
                        last_commit['id'], 
                        gitlab_state_name_prefix+"MMODA: build",
                        "running",
                        description="ODA-bot is building a container"
                        )
                try:
                    if build_engine == 'kaniko':
                        container_info = repo.build_with_kaniko(
                            build_timestamp=True,
                            nb2wversion=os.environ.get('ODA_WF_NB2W_VERSION', nb2wver()),
                            namespace=deployment_namespace,
                            cleanup=cleanup
                        )
                    elif build_engine == 'docker':
                        container_info = repo.build_with_docker(
                            no_push=False,
                            build_timestamp=True,
                            nb2wversion=os.environ.get('ODA_WF_NB2W_VERSION', nb2wver()),
                        )
                    else:
                        raise NotImplementedError(f'Unknown build engine: {build_engine}')

                except:
                    if do_git_report:
                        set_commit_state(
                            gitlab_api_url, 
                            project['id'], 
                            last_commit['id'], 
                            gitlab_state_name_prefix+"MMODA: build",
                            "failed",
                            description="ODA-bot unable to build the container. An e-mail with details has been sent."
                            )
                    raise
                else:
                    if container_info['image'].count('/') == 1:
                        container_info['image'] 
                        hub_url = f"https://hub.docker.com/r/{container_info['image'].split(':')[0]}"
                    else:
                        hub_url = None # no universal way to construct clickable url 
                    if do_git_report:
                        set_commit_state(
                            gitlab_api_url,
                            project["id"],
                            last_commit["id"],
                            gitlab_state_name_prefix + "MMODA: build",
                            "success",
                            description=(
                                f"ODA-bot have successfully built the container in {(datetime.now() - bstart).seconds} seconds. "
                                f"Image pushed to registry as {container_info['image']}"
                            ),
                            target_url=hub_url,
                        )

                # deploy
                if do_git_report:
                    set_commit_state(
                        gitlab_api_url,
                        project["id"],
                        last_commit["id"],
                        gitlab_state_name_prefix + "MMODA: deploy",
                        "running",
                        description="ODA-bot is deploying the workflow",
                    )
                try:
                    workflow_name = (
                        project["name"].lower().replace(" ", "-").replace("_", "-")
                        + "-workflow"
                    )

                    vol_kwargs = {"with_volume": False}
                    if runtime_volume_config is not None:
                        vol_kwargs = dict(
                            with_volume=True,
                            **runtime_volume_config
                        )

                    deployment_info = repo.deploy_k8s(
                        deployment_base_name=workflow_name,
                        namespace=deployment_namespace,
                        container_override=container_info,
                        **vol_kwargs,
                    )

                except:
                    if do_git_report:
                        set_commit_state(
                            gitlab_api_url,
                            project["id"],
                            last_commit["id"],
                            gitlab_state_name_prefix + "MMODA: deploy",
                            "failed",
                            description="ODA-bot unable to deploy the workflow. An e-mail with details has been sent.",
                        )
                    raise
                else:
                    if do_git_report:
                        set_commit_state(
                            gitlab_api_url,
                            project["id"],
                            last_commit["id"],
                            gitlab_state_name_prefix + "MMODA: deploy",
                            "success",
                            description=f"ODA-bot have successfully deployed {workflow_name} to {deployment_namespace} namespace",
                        )

        except ContainerBuildException as e:
            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 
                                                               'last_deployment_status': 'failed',
                                                               'stage_failed': 'build'}

            logger.error('Build error in %s! %s', project['name'], repr(e), 
                         extra={'Dockerfile': getattr(e, 'dockerfile', None),
                                'buildlog': getattr(e, 'buildlog', None)})


            with tempfile.TemporaryDirectory() as tmpdir:
                attachments = []
                buildlog = getattr(e, 'buildlog', None)
                dockerfile = getattr(e, 'dockerfile', None)
                if buildlog is not None:
                    attachments.append(os.path.join(tmpdir, 'build.log'))
                    with open(attachments[-1], 'wb') as fd:
                        fd.write(buildlog)
                if dockerfile is not None:
                    attachments.append(os.path.join(tmpdir, 'Dockerfile'))
                    with open(attachments[-1], 'wt') as fd:
                        fd.write(dockerfile)

                to = last_commit['committer_email'] if email_notify_user else []
                bcc = extra_emails if email_notify_admin else []                 
                if to or bcc:
                    send_email(to, 
                            f"[ODA-Workflow-Bot] unfortunately did NOT manage to deploy {project['name']}!", 
                            ("Dear MMODA Workflow Developer\n\n"
                            "ODA-Workflow-Bot just tried to build the container for your workflow following some change, but did not manage!\n\n"
                            "Please check attached files for more info.\n"
                            "\n\nSincerely, ODA Bot"
                            ), attachments, bcc=bcc)

        except Exception as e:
            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 
                                                               'last_deployment_status': 'failed',
                                                               'stage_failed': 'deploy'}

            logger.exception('exception deploying %s! %s', project['name'], repr(e))

            to = last_commit['committer_email'] if email_notify_user else []
            if to:
                send_email(
                    to, 
                    f"[ODA-Workflow-Bot] unfortunately did NOT manage to deploy {project['name']}!", 
                    ("Dear MMODA Workflow Developer\n\n"
                    "ODA-Workflow-Bot just tried to deploy your workflow following some change, but did not manage due to an internal error.\n"
                    "We are working on fixing the issue.\n"
                    "\n\nSincerely, ODA Bot"
                    ))    
            to = extra_emails if email_notify_admin else []                 
            if to:
                send_email(
                    extra_emails,
                    f"[ODA-Workflow-Bot] internal error while deploying {project['name']}",
                    traceback.format_exc())

        else:
            kg_record = f'''
                {rdflib.URIRef(project['http_url_to_repo']).n3()} a oda:WorkflowService;
                                    oda:last_activity_timestamp "{last_commit_created_at}";
                                    oda:last_deployed_timestamp "{datetime.now().timestamp()}";
                                    oda:service_name "{project['name'].lower().replace(' ', '_').replace('-', '_')}";
                                    oda:deployment_namespace "{deployment_namespace}";
                                    oda:deployment_name "{deployment_info['deployment_name']}";
                                    <https://schema.org/creativeWorkStatus> "{creative_work_status}".  
                '''

            sparql_obj.insert(kg_record)
            if do_git_report:
                set_commit_state(
                    gitlab_api_url, 
                    project['id'], 
                    last_commit['id'], 
                    gitlab_state_name_prefix+"MMODA: register",
                    "success",
                    description=f"ODA-bot have successfully registered workflow in ODA KG")

            deployed_workflows[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at, 
                                                               'last_deployment_status': 'success'}

            # TODO: add details from workflow change, diff, signateu
            # TODO: in plugin, deploy on request

    return deployed_workflows, deployment_info            


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
    # dispatcher_deployment = obj['settings'].get('nb2workflow.dispatcher.deployment',
    #                                             "oda-dispatcher")
    container_registry = obj['settings'].get('nb2workflow.registry', 'odahub')

    frontend_instruments_dir = obj['settings'].get('nb2workflow.frontend.instruments_dir', None)
    frontend_deployment = obj['settings'].get('nb2workflow.frontend.deployment', None)
    frontend_url = obj['settings'].get('nb2workflow.frontend.frontend_url', None)

    build_engine = obj['settings'].get('nb2workflow.build_engine', 'docker')

    gitlab_api_url = obj['settings'].get('gitlab.api_url', "https://gitlab.renkulab.io/api/v4/")
    gitlab_gid = obj['settings'].get('gitlab.gid', 5606)

    default_creative_status = obj['settings'].get('nb2workflow.default_creative_status', 'development')

    trigger_topics_dev = obj['settings'].get('nb2workflow.trigger_topics.development', ['live-workflow'])
    trigger_topics_public = obj['settings'].get('nb2workflow.trigger_topics.public', ['live-workflow-public'])

    runtime_volume_config = obj['settings'].get('nb2workflow.runtime_volume_config', None)
    
    email_notify_user = obj['settings'].get('nb2workflow.notifications.email.notify_user', True)
    email_notify_admin = obj['settings'].get('nb2workflow.notifications.email.notify_admin', True)
    admin_emails = obj['settings'].get('nb2workflow.notifications.email.admin_emails', [])

    do_git_report = obj['settings'].get('nb2workflow.notifications.gitlab.enable', True)
    state_name_prefix = obj['settings'].get('nb2workflow.notifications.gitlab.state_name_prefix', '')


    if obj['settings'].get('nb2workflow.state_storage.type', 'yaml') == 'yaml':
        state_storage = obj['settings'].get('nb2workflow.state_storage.path', 'oda-bot-runtime-workflows.yaml')
    else:
        raise NotImplementedError(f"unknown bot state storage type: {obj['settings'].get('nb2workflow.state_storage.type')}")

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

            for project in requests.get(f'{gitlab_api_url}groups/{gitlab_gid}/projects?include_subgroups=yes&order_by=last_activity_at').json():            
                if set(trigger_topics_public) & set(project['topics']):
                    project['creative_status'] = "production"
                else:
                    project['creative_status'] = default_creative_status

                if re.match(pattern, project['name']) and (set(trigger_topics_dev+trigger_topics_public) & set(project['topics'])):                
                    logger.info("%20s  ago %s", project['name'], project['http_url_to_repo'])
                    logger.info("%20s", project['topics'])
                    logger.debug("%s", json.dumps(project))

                    last_commit = requests.get(f'{gitlab_api_url}projects/{project["id"]}/repository/commits?per_page=1&page=1').json()[0]
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
                            logger.info("will deploy this workflow, creative_status=%s, topics=%s", project['creative_status'], project['topics'])
                            workflow_update_status, deployment_info = update_workflow(
                                last_commit,
                                last_commit_created_at,
                                project,
                                k8s_namespace,
                                odakb_sparql,
                                container_registry,
                                build_engine,
                                cleanup=False if obj["debug"] else True,
                                gitlab_api_url=gitlab_api_url,
                                extra_emails=admin_emails,
                                creative_work_status=project["creative_status"],
                                gitlab_state_name_prefix=state_name_prefix,
                                runtime_volume_config=runtime_volume_config,
                            )

                            logger.info('Workflow update status %s', workflow_update_status)
                            logger.info('Deployment info %s', deployment_info)

                            if workflow_update_status[project['http_url_to_repo']]['last_deployment_status'] == 'success':

                                # TODO: check live
                                # oda-api -u staging get -i cta-example

                                # TODO: make configurable; consider it to be on k8s volume

                                if frontend_instruments_dir:
                                    if do_git_report:
                                        set_commit_state(
                                            gitlab_api_url, 
                                            project['id'], 
                                            last_commit['id'], 
                                            state_name_prefix+"MMODA: frontend_tab",
                                            "running",
                                            description="Generating frontend tab")
                                    try:
                                        acknowl = f'Service generated from <a href="{project["http_url_to_repo"]}" target="_blank">the repository</a>'
                                        res = requests.get(f'{gitlab_api_url}projects/{project["id"]}/repository/files/acknowledgements.md/raw?ref=master')
                                        if res.status_code == 200:
                                            logger.info('Acknowledgements found in repo. Converting')
                                            acknowl = res.text
                                            logger.debug('Acknowledgements markdown: %s', acknowl)
                                            acknowl = markdown.markdown(acknowl)
                                            logger.debug('Acknowledgements html: %s', acknowl)

                                        messenger = ''
                                        for topic in project['topics']:
                                            if topic.startswith('MM '):
                                                messenger = topic[3:]
                                                break                                     

                                        help_html = None
                                        res = requests.get(f'{gitlab_api_url}projects/{project["id"]}/repository/files/mmoda_help_page.md/raw')
                                        if res.status_code == 200:
                                            logger.info('Help found in repo. Converting')
                                            help_md = res.text
                                            logger.debug('Help markdown: %s', help_md)
                                            img_base_url = f'{project["web_url"]}/-/raw/{project["default_branch"]}/'
                                            help_html = convert_help(help_md, img_base_url)
                                            logger.debug('Help html: %s', help_html)

                                        instr_name = project['name'].lower().replace(' ', '_').replace('-', '_')

                                        instrument_version_link = last_commit.get('web-url', None)
                                        instrument_version = last_commit.get('short_id', None)

                                        logger.info('Generating frontend tab')
                                        generator = MMODATabGenerator(dispatcher_url)
                                        generator.generate(
                                            instrument_name=instr_name,
                                            instruments_dir_path=frontend_instruments_dir,
                                            frontend_name=instr_name,
                                            title=project["name"],
                                            messenger=messenger,
                                            roles=(
                                                ""
                                                if project.get("creative_status")
                                                == "production"
                                                else "oda workflow developer"
                                            ),
                                            form_dispatcher_url="dispatch-data/run_analysis",
                                            weight=200,  # TODO: how to guess the best weight?
                                            citation=acknowl,
                                            instrument_version=instrument_version,
                                            instrument_version_link=instrument_version_link,
                                            help_page=help_html,
                                        )

                                        sp.check_output(
                                            [
                                                "kubectl",
                                                "exec",  # "-it",
                                                f"deployment/{frontend_deployment}",
                                                "-n",
                                                k8s_namespace,
                                                "--",
                                                "bash",
                                                "-c",
                                                f"cd /var/www/mmoda; ~/.composer/vendor/bin/drush dre -y mmoda_{instr_name}",
                                            ]
                                        )

                                        workflow_update_status[project['http_url_to_repo']]['last_deployment_status'] = 'success'

                                    except Exception as e:
                                        if do_git_report:
                                            set_commit_state(
                                                gitlab_api_url, 
                                                project['id'], 
                                                last_commit['id'], 
                                                state_name_prefix+"MMODA: frontend_tab",
                                                "failed",
                                                description="Failed generating frontend tab")

                                        workflow_update_status[project['http_url_to_repo']]['last_deployment_status'] = 'failed'
                                        workflow_update_status[project['http_url_to_repo']]['stage_failed'] = 'tab'

                                        to = last_commit['committer_email'] if email_notify_user else []
                                        if to:
                                            send_email(
                                                to, 
                                                f"[ODA-Workflow-Bot] failed to create the frontend tab for {project['name']}", 
                                                ("Dear MMODA Workflow Developer\n\n"
                                                "ODA-Workflow-Bot successfully deployed the backend component for your workflow following some change,\n"
                                                "but unfortunately did NOT manage to create the frontend tab!"
                                                "We are working on fixing the issue.\n"
                                                "\n\nSincerely, ODA Bot"
                                                ))
                                        to = admin_emails if email_notify_admin else []
                                        if to:
                                            send_email(
                                                admin_emails, 
                                                f"[ODA-Workflow-Bot] error creating frontend tab for {project['name']}",
                                                traceback.format_exc())
                                        
                                        logger.exception("exception while generating tab: %s", repr(e))

                                    else:
                                        if do_git_report:
                                            set_commit_state(
                                                gitlab_api_url, 
                                                project['id'], 
                                                last_commit['id'], 
                                                state_name_prefix+"MMODA: frontend_tab",
                                                "success",
                                                description="Frontend tab generated",
                                                target_url=frontend_url)
                                            
                                        to = last_commit['committer_email'] if email_notify_user else []
                                        bcc = admin_emails if email_notify_admin else []
                                        if to or bcc:
                                            send_email(
                                                to, 
                                                f"[ODA-Workflow-Bot] deployed {project['name']}", 
                                                ("Dear MMODA Workflow Developer\n\n"
                                                "ODA-Workflow-Bot just deployed your workflow, and passed basic validation.\n"
                                                "Please find below some details on the inferred workflow properties, "
                                                "and check if the parameters and the default outputs are interpretted as you intended them to be.\n\n"
                                                f"{json.dumps(deployment_info, indent=4)}\n\n"
                                                "You can now try using accessing your workflow, see https://odahub.io/docs/guide-development/#optional-try-a-test-service\n\n"
                                                "\n\nSincerely, ODA Bot"
                                                ), bcc = bcc)

                            deployed_workflows.update(workflow_update_status)
                            oda_bot_runtime['deployed_workflows'] = deployed_workflows
                            with open(state_storage, 'w') as fd:
                                yaml.dump(oda_bot_runtime, fd)

        except Exception as e:
            logger.exception("unexpected exception: %s", traceback.format_exc())

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

@cli.command()
@click.option("--dry-run", is_flag=True)
@click.option("--loop", default=0)
@click.option("--force", is_flag=True)
@click.option("--pattern", default=".*")
@click.pass_obj
def make_galaxy_tools(obj, dry_run, loop, force, pattern):
    tools_repo = obj['settings'].get('nb2galaxy.tools_repo', "https://github.com/esg-epfl-apc/tools-astro/")
    target_tools_repo = obj['settings'].get('nb2galaxy.target_tools_repo', "https://github.com/esg-epfl-apc/tools-astro.git")
    target_branch = obj['settings'].get('nb2galaxy.target_branch', "main")
    repo_cache_dir = obj['settings'].get('nb2galaxy.repo_cache_path', "/nb2galaxy-cache")
    state_storage = obj['settings'].get('nb2galaxy.state_storage', '/nb2galaxy-cache/oda-bot-runtime-galaxy.yaml')
    git_name = obj['settings'].get('nb2galaxy.git_identity.name', 'ODA bot')
    git_email = obj['settings'].get('nb2galaxy.git_identity.email', 'noreply@odahub.io')
    git_credentials = obj['settings'].get('nb2galaxy.git_credentials', os.path.join(os.environ.get('HOME', '/'), '.git-credentials'))
    available_channels = obj['settings'].get('nb2galaxy.conda_channels', ['conda-forge'])
    trigger_topics_public = obj['settings'].get('nb2galaxy.trigger_topics.public', ['galaxy-tool'])
    github_pr_labels = obj['settings'].get('nb2galaxy.github_pr_labels', ['test-live'])

    if isinstance(available_channels, BoxList):
        available_channels = available_channels.to_list()
    elif isinstance(available_channels, str):
        available_channels = available_channels.split(',')
    
    repo_cache_dir = os.path.abspath(repo_cache_dir)
    state_storage = os.path.abspath(state_storage)
    tools_repo_dir = os.path.join(repo_cache_dir, 'tools-astro')
    
    gitlab_api_url = obj['settings'].get('gitlab.api_url', "https://gitlab.renkulab.io/api/v4/")
    gitlab_gid = obj['settings'].get('gitlab.gid', 5606)

    os.makedirs(repo_cache_dir, exist_ok=True)
    
    with open(git_credentials) as fd:
        token = fd.read().split(':')[-1].split('@')[0]
    
    def git_clone_or_update(local_path, remote, branch=None, origin='origin', pre_cleanup=True):
        if os.path.isdir(local_path) and os.listdir():
            os.chdir(local_path)
            try:
                if branch is None:
                    # determine the default branch automatically
                    outp = sp.run(['git', 'remote', 'show', origin], check=True, capture_output=True)
                    m = re.search(r'^\s*(\w+ )*HEAD( \w+)*\s?:\s?(?P<branch>\w+)', outp.stdout.decode(), re.MULTILINE)
                    if m is None: 
                        raise RuntimeError(f"Can't determine default branch for remote {remote}.")
                    branch = m.group('branch')

                res = sp.run(['git', 'remote', 'get-url', '--push', origin], 
                            check=True, capture_output=True, text=True)
                if res.stdout.strip() != remote:
                    raise ValueError
                
                if pre_cleanup:
                    sp.run(['git', 'restore', '--staged', '.'], check=True)
                    sp.run(['git', 'restore', '.'], check=True)
                    sp.run(['git', 'clean', '-fd'], check=True)

                sp.run(['git', 'checkout', branch], check=True)
                sp.run(['git', 'pull', origin, branch], check=True)
                sp.run(['git', 'remote', 'update', origin, '--prune'])
            except (sp.CalledProcessError, ValueError):
                raise RuntimeError(f'{local_path} is not a valid tools repo')
        else:
            sp.run(['git', 'clone', remote, local_path], check=True)
            if branch is not None:
                sp.run(['git', 'checkout', branch], check=True)
                    
    try:
        oda_bot_runtime = yaml.safe_load(open(state_storage))
    except FileNotFoundError:
        oda_bot_runtime = {}
    
    def make_pr(source_repo, source_branch, target_repo, target_branch, title='New PR', body='', dry_run=False):
        repo_patt = re.compile(r'https://github\.com/(?P<user>[^/]+)/(?P<repo>[^\.]+)\.git')
        
        m = repo_patt.match(source_repo)
        s_user = m.group('user')
        s_repo = m.group('repo')
        
        m = repo_patt.match(target_repo)
        t_user = m.group('user')
        t_repo = m.group('repo')
                
        api_url = f"https://api.github.com/repos/{t_user}/{t_repo}/pulls"
        data = {'title': title,
                'body': body,
                'head': f'{s_user}:{source_branch}',
                'base': target_branch}
        headers = {"Accept": "application/vnd.github+json",
                   "Authorization": f"Bearer {token}",
                   "X-GitHub-Api-Version": "2022-11-28"}
        
        res = requests.get(api_url, params={'head': f'{s_user}:{source_branch}', 'state': 'open'}, headers=headers)       
        if res.status_code == 200:
            if res.json() != []:
                logger.info(f"Pull request already exist {res.json()[0]['html_url']}")
                return res.json()[0]
        else:
            raise RuntimeError(f'Error getting PRs. Status: {res.status_code}. Response text: {res.text}')
        
        if not dry_run:
            res = requests.post(api_url, json=data, headers=headers)
            
            if res.status_code != 201:
                raise RuntimeError(f'Error creating PR. Status: {res.status_code}. Response text: {res.text}')
            else:
                logger.info(f"New PR {res.json()['html_url']}")
                return res.json()

    def add_label_to_pr(repo, pr_num, labels):
        repo_patt = re.compile(r'https://github\.com/(?P<user>[^/]+)/(?P<repo>[^\.]+)\.git')

        m = repo_patt.match(repo)
        t_user = m.group('user')
        t_repo = m.group('repo')

        api_url = f"https://api.github.com/repos/{t_user}/{t_repo}/issues/{pr_num}/labels"
        # yes, issues, not pulls; the numbering is common so as the endpoint

        res = requests.post(
            api_url, 
            json={"labels": labels},
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28"
                }
            )
        if res.status_code != 200:
            logger.error('Error adding labels for PR %s', pr_num)


    if "deployed_tools" not in oda_bot_runtime:
        oda_bot_runtime["deployed_tools"] = {}
    deployed_tools = oda_bot_runtime["deployed_tools"]

    git_clone_or_update(tools_repo_dir, tools_repo, target_branch, pre_cleanup=True)
    os.chdir(tools_repo_dir)
    sp.run(['git', 'config', 'user.name', git_name], check=True)
    sp.run(['git', 'config', 'user.email', git_email], check=True)
    sp.run(['git', 'config', 'credential.helper', f'store --file={git_credentials}'])
    
    while True:
        git_clone_or_update(tools_repo_dir, tools_repo, target_branch)
        try:
            for project in requests.get(f'{gitlab_api_url}groups/{gitlab_gid}/projects?include_subgroups=yes&order_by=last_activity_at').json():
                try:    
                    if re.match(pattern, project['name']) and (set(trigger_topics_public) & set(project['topics'])):
                        logger.info("%20s %s", project['name'], project['http_url_to_repo'])
                        logger.debug("%s", json.dumps(project))

                        last_commit = requests.get(f'{gitlab_api_url}projects/{project["id"]}/repository/commits?per_page=1&page=1').json()[0]
                        last_commit_created_at = last_commit['created_at']

                        logger.info('last_commit %s from %s', last_commit, last_commit_created_at)
                        
                        saved_last_commit_created_at = deployed_tools.get(project['http_url_to_repo'], {}).get('last_commit_created_at', 0)
                        #saved_last_tool_version = deployed_tools.get(project['http_url_to_repo'], {}).get('last_tool_version', '0.0.0+galaxy0')
                        
                        logger.info('last_commit_created_at %s saved_last_commit_created_at %s', last_commit_created_at, saved_last_commit_created_at )

                        if last_commit_created_at == saved_last_commit_created_at and not force:
                            logger.info("no need to deploy this tool")
                        else:
                            current_state = get_commit_state(
                                gitlab_api_url=gitlab_api_url,
                                proj_id=project['id'],
                                commit_sha=last_commit['id'],
                                name = 'Galaxy tool'
                            )

                            if current_state == 'failed' and os.getenv('FORCE_FAILED_GALAXY_TOOLS', '0')=='0':
                                logger.info(f"Galaxy tool workflow was failed for {last_commit['id']}. Skipping.")
                                continue

                            set_commit_state(
                                        gitlab_api_url=gitlab_api_url,
                                        proj_id=project['id'],
                                        commit_sha=last_commit['id'],
                                        name='Galaxy tool',
                                        state='running',
                                        )
                            
                            wf_repo_dir = os.path.join(repo_cache_dir, project['path'])
                            git_clone_or_update(wf_repo_dir, project['http_url_to_repo'])

                            def repo_file_path_if_available(filename):
                                if os.path.isfile(os.path.join(wf_repo_dir, filename)):
                                    return os.path.join(wf_repo_dir, filename)
                                else:
                                    return None
                                
                            req_file = repo_file_path_if_available('requirements.txt')
                            env_file = repo_file_path_if_available('environment.yml')
                            bib_file = repo_file_path_if_available('citations.bib')
                            help_file = repo_file_path_if_available('galaxy_help.md')

                            os.chdir(tools_repo_dir)
                            tool_id = re.sub(r'[^a-z0-9_]', '_', f"{project['path']}_astro_tool")
                            tool_xml_path = os.path.join(tools_repo_dir, 'tools', project['path'], f"{tool_id}.xml")
                            if os.path.isfile(tool_xml_path):
                                tool_xml_root = ET.parse(tool_xml_path).getroot()
                                master_tool_version = tool_xml_root.attrib['version']
                                tool_name = tool_xml_root.attrib['name']
                                
                                version_parser = re.compile(r'(?P<maj>\d+)\.(?P<min>\d+)\.(?P<patch>\d+)\+galaxy(?P<suffix>\d+)')
                                m = version_parser.match(master_tool_version)
                                new_version = f"{m.group('maj')}.{m.group('min')}.{int(m.group('patch'))+1}+galaxy{m.group('suffix')}"
                            else:
                                new_version = "0.0.1+galaxy0"
                                tool_name = f"{project['name']}"

                            upd_branch_name = f"auto-update-galaxy-tool-{project['path']}-v{new_version.replace('+', '-')}"
                            try:
                                sp.run(['git', 'checkout', upd_branch_name], check=True)
                                sp.run(['git', 'pull', 'origin', upd_branch_name])
                            except sp.CalledProcessError:
                                sp.run(['git', 'checkout', '-b', upd_branch_name], check=True)
                            
                            # TODO: it could be optional or partial to preserve some manual additions
                            outd = os.path.join(tools_repo_dir, 'tools', project['path'])
                            shutil.rmtree(outd, ignore_errors=True)
                            
                            repo_files_baseurl = f"{project['web_url']}/-/raw/{last_commit['id']}/"
                            
                            to_galaxy(input_path=wf_repo_dir, 
                                      toolname=tool_name,
                                      out_dir=outd,
                                      tool_version=new_version,
                                      tool_id=tool_id,
                                      requirements_file=req_file,
                                      conda_environment_file=env_file,
                                      citations_bibfile=bib_file,
                                      help_file=help_file,
                                      available_channels=available_channels,
                                      test_data_baseurl=repo_files_baseurl
                                      )
                            
                            # creating shed file
                            if os.path.isfile(os.path.join(wf_repo_dir, '.shed.yml')):
                                shutil.copyfile(os.path.join(wf_repo_dir, '.shed.yml'),
                                                os.path.join(outd, '.shed.yml')
                                                )
                            else:
                                shed_content = {
                                    'name': tool_id,
                                    'owner': 'astroteam',
                                    'type': 'unrestricted',
                                    'categories': ['Astronomy'],
                                    'description': tool_name,
                                    'long_description': tool_name,
                                    'homepage_url': None,
                                    'remote_repository_url': 'https://github.com/esg-epfl-apc/tools-astro/tree/main/tools',
                                }
                                
                                if help_file is not None:
                                    fm = frontmatter.load(help_file)
                                    if 'description' in fm.keys():
                                        shed_content['description'] = fm['description']
                                        shed_content['long_description'] = fm.get('long_description', fm['description'])
                                
                                with open(os.path.join(outd, '.shed.yml'), 'wt') as fd:
                                    yaml.dump(shed_content, fd)

                            if help_file is not None:
                                fm = frontmatter.load(help_file)
                                if fm.get('additional_files', None):
                                    globlist = fm['additional_files']
                                    if isinstance(globlist, str):
                                        globlist = [ globlist ]
                                    elif isinstance(globlist, list):
                                        pass
                                    else:
                                        raise ValueError("Wrong specification of additional files")
                                    
                                    for fileglob in globlist:
                                        addfiles = glob(fileglob, root_dir=wf_repo_dir, recursive=True)
                                        for fp in addfiles:
                                            try:
                                                if os.path.isfile(os.path.join(wf_repo_dir, fp)):
                                                    shutil.copyfile(
                                                        os.path.join(wf_repo_dir, fp),
                                                        os.path.join(outd, fp)
                                                        )
                                                else:
                                                    shutil.copytree(
                                                        os.path.join(wf_repo_dir, fp),
                                                        os.path.join(outd, fp),
                                                        dirs_exist_ok=True
                                                        )
                                            except FileExistsError:
                                                pass


                            logger.info("Git status:\n" + sp.check_output(['git', 'status'], text=True))
                            
                            if dry_run:
                                logger.warning('Dry run. Cleaning up introduced updates.')
                                sp.run(['git', 'clean', '-fd'], check=True)
                            else:
                                try:                                
                                    r = sp.run(['git', 'add', '.'], capture_output=True, text=True)
                                    if r.returncode != 0:
                                        r.check_returncode()    
                                        
                                    r = sp.run(
                                        ['git', 'commit', '-m', 'automatic update', '-m', f"following {last_commit['web_url']}"], 
                                        capture_output=True, 
                                        text=True
                                        )
                                    
                                    if r.returncode == 1:
                                        changed = False
                                    elif r.returncode != 0:
                                        r.check_returncode()
                                    else:
                                        changed = True
                                        
                                    if changed:
                                        r = sp.run(['git', 'push', '--set-upstream', 'origin', upd_branch_name], 
                                                capture_output=True, text=True)
                                        if r.returncode != 0:
                                            r.check_returncode()
  
                                        pr = make_pr(
                                            tools_repo, 
                                            upd_branch_name, 
                                            target_tools_repo, 
                                            target_branch, 
                                            f"Update tool {tool_name} to {new_version}",
                                            )

                                        set_commit_state(
                                            gitlab_api_url=gitlab_api_url,
                                            proj_id=project['id'],
                                            commit_sha=last_commit['id'],
                                            name='Galaxy tool',
                                            state='success',
                                            description='Galaxy tool updated on GitHub',
                                            target_url=pr['html_url']
                                            )
                                        
                                        add_label_to_pr(
                                            tools_repo,
                                            pr['number'],
                                            github_pr_labels
                                        )

                                    else:
                                        pr = make_pr(
                                            tools_repo, 
                                            upd_branch_name, 
                                            target_tools_repo, 
                                            target_branch, 
                                            f"Update tool {tool_name} to {new_version}",
                                            dry_run=True
                                            )
                                                                            
                                        kwargs = {}
                                        if pr is not None:
                                            kwargs['target_url'] = pr['html_url']

                                        set_commit_state(
                                            gitlab_api_url=gitlab_api_url,
                                            proj_id=project['id'],
                                            commit_sha=last_commit['id'],
                                            name='Galaxy tool',
                                            state='success', # 'skipped' is not supported in GL 17.5 used by renku,
                                            description='No updates in Galaxy tool',
                                            **kwargs
                                            )                                        
                                    
                                except Exception as e:
                                    if isinstance(e, sp.SubprocessError):
                                        logger.exception('%s. stderr: %s', e, r.stderr)

                                    kwargs = {}
                                    try:
                                        kwargs['target_url'] = pr['html_url']
                                    except:
                                        pass

                                    set_commit_state(
                                        gitlab_api_url=gitlab_api_url,
                                        proj_id=project['id'],
                                        commit_sha=last_commit['id'],
                                        name='Galaxy tool',
                                        state='failed',
                                        description=repr(e),
                                        **kwargs
                                        )
                                    
                                    raise

                                finally:
                                    sp.run(['git', 'restore', '--staged', '.'])
                                    sp.run(['git', 'clean', '-fd'], check=True)
                                    sp.run(['git', 'checkout', target_branch])
                                    sp.run(['git', 'branch', '-D', upd_branch_name])
                                    sp.run(['git', 'restore', '--staged', '.'])
                                    sp.run(['git', 'clean', '-fd'], check=True)
                                
                                # if not changed:
                                #     continue
                                
                                deployed_tools[project['http_url_to_repo']] = {'last_commit_created_at': last_commit_created_at,
                                                                            'last_commit': last_commit['id'],
                                                                            'last_tool_version': new_version}
                                
                                oda_bot_runtime['deployed_tools'] = deployed_tools
                                with open(state_storage, 'w') as fd:
                                    yaml.dump(oda_bot_runtime, fd)
                except Exception as e:
                    logger.exception("unexpected exception: %s", traceback.format_exc())
                    logger.info("Cleanup all changes in the repo directory")
                    try: 
                        set_commit_state(
                            gitlab_api_url=gitlab_api_url,
                            proj_id=project['id'],
                            commit_sha=last_commit['id'],
                            name='Galaxy tool',
                            state='failed',
                            description=repr(e),
                            )
                    except NameError:
                        # if last_commit is unbound for any reason (normally should be set)
                        pass

                    sp.run(['git', 'clean', '-fd'])
                    logger.info("continue with the next repo")

                    continue
                    
        except Exception:
            logger.exception("unexpected exception: %s", traceback.format_exc())
            
        if loop > 0:
            logger.info("sleeping %s", loop)
            time.sleep(loop)
        else:
            break                        


# TODO:  test service status and dispatcher status
# oda-api -u https://dispatcher-staging.obsuks1.unige.ch get -i cta-example

def main():
    cli(obj={})

if __name__ == "__main__":
    main()
