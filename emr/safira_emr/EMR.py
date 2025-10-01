from __future__ import annotations
from pydantic import BaseModel
from typing import List, Any

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.exceptions import ClientError

import sys
import os
import time
from dotenv import load_dotenv

from .exceptions.ScriptListEmptyError import ScriptListEmptyError
from .exceptions.FaliedStepError import FailedStepError
from .exceptions.MissingAWSCredentialsError import MissingAWSCredentialsError
from .config import *

import logging

from rich import print

logger = logging.getLogger(__name__)
path = os.path.dirname(__file__)


class EMRClusterConfig(BaseModel):
    instane_type: str
    instance_count: int
    release_label: str
    ebs_volume_size: int


class EMR(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    name: str | None
    id: str | None
    client: BaseClient | None

    boto_session: Any | None
    bucket: ServiceResource | None
    keep_bucket: bool | None
    security_groups_ids: dict | None

    aws_access_key_id: str
    aws_secret_access_key: str

    CONFIG_BUCKET : str = "ephemeral-emr-config-safira"

    @property
    def bootstrap_actions(self) -> List[dict]:
        return [
            {
                "Name": "install-dependencies",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{self.CONFIG_BUCKET}/bootstrap/install_libraries.sh"
                },
            }
        ]

    @property
    def bucket_name(self):
        return self.bucket.name

    def describe_cluster(self):
        """
        Gets detailed information about a cluster.

        :param cluster_id: The ID of the cluster to describe.
        :param emr_client: The Boto3 EMR client object.
        :return: The retrieved cluster information.
        """
        try:
            response = self.client.describe_cluster(ClusterId=self.id)
            cluster = response["Cluster"]
            logger.info("Got data for cluster %s.", cluster["Name"])
        except ClientError:
            logger.error("Couldn't get data for cluster %s.", self.id)
            raise
        else:
            return cluster

    def status_poller(self, intro: str, done_status: List[str], func: function):
        """
        Polls a function for status, sleeping for 10 seconds between each query,
        until the specified status is returned.

        :param intro: An introductory sentence that informs the reader what we're
                    waiting for.
        :param done_status: The status we're waiting for. This function polls the status
                            function until it returns the specified status.
        :param func: The function to poll for status. This function must eventually
                    return the expected done_status or polling will continue indefinitely.
        """
        logger.setLevel(logging.WARNING)
        status = ""
        print(intro)
        print("Current status: ", end="")
        while status not in done_status:
            prev_status = status
            status = func()
            if prev_status == status:
                print(".", end="")
            else:
                print(status, end="")
            sys.stdout.flush()
            time.sleep(10)
        print()
        logger.setLevel(logging.INFO)

        if status not in ["COMPLETED", "WAITING", "TERMINATED"]:
            logger.error(f"Step failed with status: {status}")
            raise FailedStepError(status)

    def create_bucket(self) -> ServiceResource:
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name="us-east-1"
        )
        try:
            bucket_name = f"{self.name}-bucket-{time.time_ns()}"
            bucket = s3_resource.create_bucket(Bucket=bucket_name)
            bucket.wait_until_exists()
            logger.info("Created bucket %s.", bucket_name)
        except ClientError:
            logger.error("Couldn't create bucket %s.", bucket_name)
            raise

        self.bucket = bucket

    def upload_scripts(self, script_file_names: List[str]) -> List[str]:
        if len(script_file_names) < 1:
            raise ScriptListEmptyError()

        script_keys = [
            f"scripts/{script_file_name.split('/')[-1]}" for script_file_name in script_file_names
        ]

        for script_file_name, script_key in zip(script_file_names, script_keys):
            try:
                self.bucket.upload_file(
                    script_file_name, script_key
                )
                logger.info(
                    "Uploaded script %s to %s.",
                    script_file_name,
                    f"{self.bucket.name}/{script_key}",
                )
            except ClientError:
                logger.error(
                    "Couldn't upload %s to %s.", script_file_name, self.bucket.name
                )
                raise
        return script_keys

    def delete_bucket(self):
        try:
            self.bucket.objects.delete()
            self.bucket.delete()
            logger.info("Emptied and removed bucket %s.", self.bucket.name)
        except ClientError:
            logger.error("Couldn't remove bucket %s.", self.bucket.name)
            raise

    def describe_step(self, step_id: str):
        """
        Gets detailed information about the specified step, including the current state of
        the step.

        :param cluster_id: The ID of the cluster.
        :param step_id: The ID of the step.
        :param emr_client: The Boto3 EMR client object.
        :return: The retrieved information about the specified step.
        """
        try:
            response = self.client.describe_step(
                ClusterId=self.id, StepId=step_id)
            step = response["Step"]
            logger.info("Got data for step %s.", step_id)
        except ClientError:
            logger.error("Couldn't get data for step %s.", step_id)
            raise
        else:
            return step

    def fetch_security_groups_id(self, security_groups: dict):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name="us-east-1"
        )
        security_groups_ids = {}
        try:
            for k in security_groups.keys():
                response = ec2.describe_security_groups(
                    Filters=[
                        dict(Name="group-name", Values=[security_groups[k]])]
                )
                group_id = response["SecurityGroups"][0]["GroupId"]
                security_groups_ids[k] = group_id
        except ClientError:
            logger.error("Couldn't find security group with name %s.", k)
            raise
        self.security_groups_ids = security_groups_ids

    def run_step(
        self, name: str, action_on_failure: str, script_uri: str, script_args: List[str]
    ):

        try:
            response = self.client.add_job_flow_steps(
                JobFlowId=self.id,
                Steps=[
                    {
                        "Name": name,
                        "ActionOnFailure": action_on_failure,
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": [
                                "spark-submit",
                                "--deploy-mode",
                                "cluster",
                                "--jars",
                                f"s3://{self.CONFIG_BUCKET}/jars/*.jar",
                                script_uri,
                                *script_args,
                            ],
                        },
                    }
                ],
            )
            step_id = response["StepIds"][0]
            logger.info("Started step with ID %s", step_id)
        except ClientError:
            logger.error("Couldn't start step %s with URI %s.",
                         name, script_uri)
            raise
        else:
            self.status_poller(
                f"Waiting for step {name} to complete...",
                ["COMPLETED", "FAILED", "CANCELLED",],
                lambda: self.describe_step(step_id)["Status"]["State"],
            )
            return step_id

    @classmethod
    def create(
        cls,
        name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        security_groups: dict = {
            "manager": "EphemeralEMRSecurityGroup-Manager",
            "worker": "EphemeralEMRSecurityGroup-Worker",
        },
        job_flow_role_name: str = "EphemeralEMRJobFlowRole",
        service_role_name: str = "EphemeralEMRServiceRole",
        cluster_config: dict = {
            "instane_type": INSTANE_TYPE,
            "instance_count": INSTANCE_COUNT,
            "release_label": RELEASE_LABEL,
            "ebs_volume_size": EBS_VOLUME_SIZE
        },
        keep_bucket: bool = False
    ):
        """ """
        load_dotenv()

        emr = EMR(name=name, keep_bucket=keep_bucket, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        emr_cluster_config = EMRClusterConfig.parse_obj(cluster_config)
        emr.aws_access_key_id = aws_access_key_id
        emr.aws_secret_access_key = aws_secret_access_key
        emr.fetch_security_groups_id(security_groups)

        emr.client = boto3.client(
            "emr",
            aws_access_key_id=emr.aws_access_key_id,
            aws_secret_access_key=emr.aws_secret_access_key,
            region_name="us-east-1"
        )

        emr.create_bucket()

        bootstrap_actions = emr.bootstrap_actions

        try:
            response = emr.client.run_job_flow(
                Name=name,
                LogUri=f"s3://{emr.bucket.name}/logs",
                ReleaseLabel=emr_cluster_config.release_label,
                Instances={
                    "MasterInstanceType": emr_cluster_config.instane_type,
                    "SlaveInstanceType": emr_cluster_config.instane_type,
                    "InstanceCount": emr_cluster_config.instance_count,
                    "KeepJobFlowAliveWhenNoSteps": True,
                    "EmrManagedMasterSecurityGroup": emr.security_groups_ids["manager"],
                    "EmrManagedSlaveSecurityGroup": emr.security_groups_ids["worker"],
                },
                Applications=[{"Name": app} for app in ["Hadoop", "Hive", "Spark"]],
                JobFlowRole=job_flow_role_name,
                ServiceRole=service_role_name,
                EbsRootVolumeSize=emr_cluster_config.ebs_volume_size,
                VisibleToAllUsers=True,
                BootstrapActions=bootstrap_actions,
            )
            cluster_id = response["JobFlowId"]
            logger.info("Created cluster %s.", cluster_id)
        except ClientError:
            logger.error("Couldn't create cluster.")
            emr.delete_bucket()
            raise
        else:
            emr.id = cluster_id
            emr.status_poller(
                "Waiting for cluster, this typically takes several minutes...",
                ["WAITING", "TERMINATED_WITH_ERRORS"],
                lambda: emr.describe_cluster()["Status"]["State"],
            )
            return emr

    def terminate_cluster(self):
        """
        Terminates a cluster. This terminates all instances in the cluster and cannot
        be undone. Any data not saved elsewhere, such as in an Amazon S3 bucket, is lost.

        :param cluster_id: The ID of the cluster to terminate.
        :param emr_client: The Boto3 EMR client object.
        """
        try:
            self.client.terminate_job_flows(JobFlowIds=[self.id])
            self.status_poller(
                "Waiting for cluster to terminate.",
                ["TERMINATED"],
                lambda: self.describe_cluster()["Status"]["State"],
            )
            logger.info("Terminated cluster %s.", self.id)
            if self.keep_bucket == False:
                logger.info("Deleting bucket")
                self.delete_bucket()
        except ClientError:
            logger.error("Couldn't terminate cluster %s.", self.id)
            raise
