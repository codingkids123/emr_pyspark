# Controller program to interact with AWS EMR and run PySpark pipeline.

import argparse
from subprocess import call

import boto3

from settings import *


emr_client = boto3.client('emr')


def start_cluster(cluster_name, instance_count, master_type, slave_type, src_folder):
    src_path = REMOTE_SRC + src_folder
    response = emr_client.run_job_flow(
        Name=cluster_name,
        LogUri=LOG_URI,
        ReleaseLabel=EMR_RELEASE_LABEL,
        Instances={
            'MasterInstanceType': master_type,
            'SlaveInstanceType': slave_type,
            'InstanceCount': instance_count,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': EC2_SUBNET_ID,
        },
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Zeppelin'},
            {'Name': 'Ganglia'},
        ],
        BootstrapActions=[
            {
                'Name': 'Upgrade Pip',
                'ScriptBootstrapAction': {
                    'Path': src_path + 'config/upgrade_pip.sh',
                },
            },
            {
                'Name': 'Config PySpark Runtime',
                'ScriptBootstrapAction': {
                    'Path': src_path + 'config/sync_node.sh',
                    'Args': [src_folder, BASE_FOLDER],
                },
            },
            {
                'Name': 'Copy Slave SSH Key',
                'ScriptBootstrapAction': {
                    'Path': 's3://elasticmapreduce/bootstrap-actions/run-if',
                    'Args': [
                        'instance.isMaster=false',
                        'cat %sconfig/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys' % LOCAL_SRC,
                    ],
                },
            },
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script'],
                },
            },
        ],
        Configurations=[
            {
                'Classification': 'spark-env',
                'Configurations': [
                    {
                        'Classification': 'export',
                        'Configurations': [],
                        'Properties': {
                            'PYTHONPATH': '$PYTHONPATH:' + LOCAL_SRC,
                        },
                    }
                ],
                'Properties': {},
            },
        ],
        VisibleToAllUsers=True,
        JobFlowRole=JOBFLOW_ROLE,
        ServiceRole=SERVICE_ROLE,
    )
    return response['JobFlowId']


def add_script_step_to_cluster(cluster_id, script, argument):
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': script.split('/')[-1],
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [script] + argument,
                },
            },
        ],
    )
    return response['StepIds']


def add_pyspark_step_to_cluster(cluster_id, job_file, argument):
    spark_submit_command = [
        'spark-submit',
        '--deploy-mode', 'cluster', '--master', 'yarn',
        '--conf', 'spark.yarn.submit.waitAppCompletion=true',
    ]
    if SPARK_SUBMIT_PACKAGES:
        spark_submit_command += ['--packages', ','.join(SPARK_SUBMIT_PACKAGES)]
    spark_submit_command += [LOCAL_SRC + job_file] + argument.split(' ')

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': job_file.split('/')[-1],
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': spark_submit_command,
                },
            },
        ],
    )
    return response['StepIds']


def stop_cluster(cluster_id):
    response = emr_client.terminate_job_flows(
        JobFlowIds=[cluster_id]
    )
    return response


def start(args):
    print('Start an EMR cluster')
    cluster_id = start_cluster(args.cluster_name, args.instance_count, args.master_type, args.slave_type,
                               args.src_folder)
    print('Cluster id: %s' % cluster_id)


def step(args):
    print('Add PySpark step (%s) to EMR cluster %s' % (args.job_file, args.cluster_id))
    step_ids = add_pyspark_step_to_cluster(args.cluster_id, args.job_file, args.argument)
    print('Step ids: %s' % step_ids)


def stop(args):
    print('Stop EMR cluster %s' % args.cluster_id)
    response = stop_cluster(args.cluster_id)
    print(response)


def push(args):
    destination = REMOTE_SRC + args.src_folder
    print('Push code to %s' % destination)
    call(['aws', 's3', 'sync', '--delete', '.', destination])
    if args.cluster_id:
        add_script_step_to_cluster(args.cluster_id, LOCAL_SRC + 'config/sync_cluster.sh',
                                   [args.src_folder, BASE_FOLDER])


def main():
    parser = argparse.ArgumentParser(description='AWS EMR Controller')
    subparsers = parser.add_subparsers()

    subparser1 = subparsers.add_parser('start', description='Start an EMR cluster.')
    subparser1.add_argument('-c', '--instance_count', default=3, help='instance count, including master')
    subparser1.add_argument('-m', '--master_type', default='m4.xlarge', help='type of master instance')
    subparser1.add_argument('-s', '--slave_type', default='m4.xlarge', help='type of slave instance')
    subparser1.add_argument('-n', '--cluster_name', help='cluster name', required=True)
    subparser1.add_argument('-r', '--src_folder', help='source folder', required=True)
    subparser1.set_defaults(func=start)

    subparser2 = subparsers.add_parser('step', description='Add a PySpark step to EMR cluster.')
    subparser2.add_argument('-j', '--cluster_id', help='cluster id', required=True)
    subparser2.add_argument('-f', '--job_file', help='pyspark job file', required=True)
    subparser2.add_argument('-a', '--argument', help='argument', default='')
    subparser2.set_defaults(func=step)

    subparser3 = subparsers.add_parser('stop', description='Stop an EMR cluster.')
    subparser3.add_argument('-j', '--cluster_id', help='cluster id', required=True)
    subparser3.set_defaults(func=stop)

    subparser4 = subparsers.add_parser('stop', description='Push code to S3 and cluster.')
    subparser4.add_argument('-r', '--src_folder', help='source folder', required=True)
    subparser4.add_argument('-j', '--cluster_id', help='cluster id')
    subparser4.set_defaults(func=push)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
