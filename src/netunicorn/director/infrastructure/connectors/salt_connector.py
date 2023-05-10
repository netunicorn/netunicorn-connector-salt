from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Optional, Tuple, Any

import aiohttp
import yaml
from netunicorn.base.architecture import Architecture
from netunicorn.base.deployment import Deployment
from netunicorn.base.environment_definitions import DockerImage, ShellExecution
from netunicorn.base.nodes import CountableNodePool, Node
from returns.result import Failure, Result, Success

from netunicorn.director.base.connectors.protocol import (
    NetunicornConnectorProtocol,
)
from netunicorn.director.base.connectors.types import StopExecutorRequest


class SaltConnector(NetunicornConnectorProtocol):
    def __init__(
        self,
        connector_name: str,
        config_file: str | None,
        netunicorn_gateway: str,
        logger: Optional[logging.Logger] = None,
    ):
        self.connector_name = connector_name
        self.config_file = config_file
        self.netunicorn_gateway = netunicorn_gateway

        self.config = {}
        if config_file:
            with open(config_file, "r") as f:
                self.config = yaml.safe_load(f)

        self.PUBLIC_GRAINS: list[str] = self.config.get(
            "netunicorn.connector.salt.public_grains", ["location", "osarch", "kernel", "netunicorn-environments"]
        )

        self.endpoint = self.config.get(
            "netunicorn.connector.salt.endpoint"
        ).removesuffix("/")
        self.runpoint = self.endpoint + "/run"
        self.username = self.config.get("netunicorn.connector.salt.username")
        self.password = self.config.get("netunicorn.connector.salt.password")
        self.eauth = self.config.get("netunicorn.connector.salt.eauth")
        self.session = None

        if not logger:
            logging.basicConfig(level=logging.DEBUG)
            logger = logging.getLogger(__name__)
        self.logger = logger

    async def initialize(self) -> None:
        self.session = aiohttp.ClientSession(headers={"Accept": "application/json"})
        return

    async def health(self) -> Tuple[bool, str]:
        return True, "OK"

    async def shutdown(self) -> None:
        await self.session.close()
        return

    async def get_nodes(
            self,
            username: str,
            authentication_context: Optional[dict[str, str]] = None,
            *args: Any,
            **kwargs: Any,
    ) -> CountableNodePool:
        try:
            (
                await self.session.post(
                    self.runpoint,
                    json={
                        "client": "local",
                        "tgt": "*",
                        "fun": "saltutil.sync_grains",
                        "username": self.username,
                        "password": self.password,
                        "eauth": self.eauth,
                    },
                )
            ).raise_for_status()

            async with self.session.post(
                self.runpoint,
                json={
                    "client": "local",
                    "tgt": "*",
                    "fun": "grains.item",
                    "arg": self.PUBLIC_GRAINS,
                    "username": self.username,
                    "password": self.password,
                    "eauth": self.eauth,
                },
            ) as response:
                nodes = await response.json()
                nodes = nodes.get("return", [{}])[0]
        except Exception as e:
            self.logger.error(f"Exception during get_nodes: {e}")
            if 'response' in locals():
                self.logger.debug(await response.text())
            return CountableNodePool([])

        node_pool = []
        for node_name, node_grains in nodes.items():
            if not node_grains:
                continue
            instance = Node(
                name=node_name,
                properties=node_grains,
            )
            if "netunicorn-environments" in instance.properties and len(instance.properties["netunicorn-environments"]) == 0:
                del instance.properties["netunicorn-environments"]
            architecture = f'{instance.properties.get("kernel", "").lower()}/{instance.properties.get("osarch", "").lower()}'
            try:
                instance.architecture = Architecture(architecture)
            except Exception as e:
                self.logger.warning(
                    f"Unknown architecture {architecture} for node {instance.name}, {e}"
                )
                instance.architecture = Architecture.UNKNOWN
            node_pool.append(instance)
        self.logger.debug(f"Returned node pool of length: {len(node_pool)}")
        return CountableNodePool(node_pool)

    async def _start_deploying_docker_image(
        self, experiment_id: str, deployments_list: list[Deployment], image: str
    ) -> dict[str, Result[None, str]]:
        try:
            async with self.session.post(
                self.runpoint,
                json={
                    "client": "local",
                    "tgt": [x.node.name for x in deployments_list],
                    "tgt_type": "list",
                    "fun": "cmd.run",
                    "arg": [f"docker pull {image}"],
                    "username": self.username,
                    "password": self.password,
                    "eauth": self.eauth,
                    "full_return": True,
                },
            ) as response:
                salt_return = await response.json()
                self.logger.debug(salt_return)
                salt_return = salt_return.get("return", [{}])[0]

            assert isinstance(salt_return, dict) and len(salt_return) > 0
        except Exception as e:
            self.logger.error(
                f"Exception during deployment.\n"
                f"Experiment id: {experiment_id}\n"
                f"Error: {e}\n"
                f"Deployments: {deployments_list}"
            )
            self.logger.debug(await response.text())
            return {x.executor_id: Failure(str(e)) for x in deployments_list}

        results: dict[str, Result[None, str]] = {}
        for deployment in deployments_list:
            if salt_return.get(deployment.node.name, {}).get("retcode", 1) != 0:
                results[deployment.executor_id] = Failure(
                    str(salt_return.get(deployment.node.name, ""))
                )
                self.logger.error(
                    f"Error during deployment of executor {deployment.executor_id}, "
                    f"node {deployment.node}: {str(salt_return.get(deployment.node.name, ''))}"
                )
            else:
                results[deployment.executor_id] = Success(None)
                self.logger.debug(
                    f"Deployment of executor {deployment.executor_id} to node {deployment.node} successful"
                )
        self.logger.debug(f"Finished deployment of {image}, results: {results}")
        return results

    @staticmethod
    def __all_salt_results_are_correct(
        results: list[dict[str, dict[str, int | str]]], node_name: str
    ) -> bool:
        return (
            # results are not empty
            bool(results)
            # each result is a dict and has node name as a key
            and all(
                isinstance(x, dict) and x.get(node_name, None) is not None
                for x in results
            )
            # all results have return code 0
            and all(
                isinstance(x[node_name], dict) and x[node_name].get("retcode", 1) == 0
                for x in results
            )
        )

    async def _start_deploying_shell_execution(
        self, deployment: Deployment
    ) -> dict[str, Result[None, str]]:
        if not deployment.environment_definition.commands:
            self.logger.debug(
                f"Deployment of executor {deployment.executor_id} to node {deployment.node} successful, no commands to execute"
            )
            return {deployment.executor_id: Success(None)}

        try:
            # consequent, not in parallel
            results = []
            for command in deployment.environment_definition.commands:
                async with self.session.post(
                    self.runpoint,
                    json={
                        "client": "local",
                        "tgt": deployment.node.name,
                        "fun": "cmd.run",
                        "arg": [command],
                        "username": self.username,
                        "password": self.password,
                        "eauth": self.eauth,
                        "full_return": True,
                    },
                ) as response:
                    salt_return = await response.json()
                    self.logger.debug(salt_return)
                    salt_return = salt_return.get("return", [{}])[0]
                    results.append(salt_return)
        except Exception as e:
            self.logger.error(
                f"Exception during deployment of executor {deployment.executor_id}, node {deployment.node}: {e}"
            )
            results = [e]
        self.logger.debug(
            f"Deployment of executor {deployment.executor_id} to node {deployment.node}, result: {results}"
        )

        if not self.__all_salt_results_are_correct(results, deployment.node.name):
            exception = f"Failed to create environment, see exception arguments for the log: {results}"
            self.logger.error(exception)
            self.logger.debug(f"Deployment: {deployment}")
            return {deployment.executor_id: Failure(exception)}

        self.logger.info(
            f"Deployment of executor {deployment.executor_id} to node {deployment.node} finished"
        )
        return {deployment.executor_id: Success(None)}

        pass

    async def deploy(
        self,
        username: str,
        experiment_id: str,
        deployments: list[Deployment],
        deployment_context: Optional[dict[str, str]] = None,
        authentication_context: Optional[dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Result[None, str]]:
        docker_deployments = []
        shell_deployments = []
        for deployment in deployments:
            if isinstance(deployment.environment_definition, DockerImage):
                docker_deployments.append(deployment)
            elif isinstance(deployment.environment_definition, ShellExecution):
                shell_deployments.append(deployment)
            else:
                self.logger.error(
                    f"Unknown environment definition: {deployment.environment_definition}"
                )

        # 1. take all docker deployments and create a dict of image -> list of nodes
        images_dict = defaultdict(list)
        for deployment in docker_deployments:
            images_dict[deployment.environment_definition.image].append(deployment)

        # 2. for each image, pull it on all nodes
        results = {}
        for image, deployments_list in images_dict.items():
            results.update(
                await self._start_deploying_docker_image(
                    experiment_id, deployments_list, image
                )
            )

        # 3. for each shell deployment, execute the commands
        for deployment in shell_deployments:
            results.update(await self._start_deploying_shell_execution(deployment))

        self.logger.debug(
            f"Experiment {experiment_id} deployment successfully finished"
        )
        return results

    @staticmethod
    def __shell_runcommand(deployment: Deployment) -> str:
        env_vars = " ".join(
            f" {k}={v}"
            for k, v in deployment.environment_definition.runtime_context.environment_variables.items()
        )
        runcommand = f"{env_vars} python3 -m netunicorn.executor"
        return runcommand

    @staticmethod
    def __docker_runcommand(deployment: Deployment) -> str:
        env_vars = " ".join(
            f"-e {k}={v}"
            for k, v in deployment.environment_definition.runtime_context.environment_variables.items()
        )

        additional_arguments = " ".join(
            deployment.environment_definition.runtime_context.additional_arguments
        )

        ports = ""
        if deployment.environment_definition.runtime_context.ports_mapping:
            ports = " ".join(
                f"-p {k}:{v}"
                for k, v in deployment.environment_definition.runtime_context.ports_mapping.items()
            )

        runcommand = (
            f"docker run -d {env_vars} {ports} --name {deployment.executor_id} "
            f"{additional_arguments} {deployment.environment_definition.image}"
        )
        return runcommand

    async def _start_single_execution(
        self, experiment_id: str, deployment: Deployment
    ) -> Result[None, str]:
        self.logger.info(
            f"Starting execution with executor {deployment.executor_id}, node {deployment.node}"
        )

        if deployment.executor_id is None:
            return Failure("Executor id is None")

        deployment.environment_definition.runtime_context.environment_variables[
            "NETUNICORN_EXECUTOR_ID"
        ] = deployment.executor_id
        deployment.environment_definition.runtime_context.environment_variables[
            "NETUNICORN_EXPERIMENT_ID"
        ] = experiment_id
        deployment.environment_definition.runtime_context.environment_variables[
            "NETUNICORN_GATEWAY_ENDPOINT"
        ] = self.netunicorn_gateway

        if isinstance(deployment.environment_definition, DockerImage):
            runcommand = self.__docker_runcommand(deployment)
        elif isinstance(deployment.environment_definition, ShellExecution):
            runcommand = self.__shell_runcommand(deployment)
        else:
            return Failure(
                f"Unknown environment definition: {deployment.environment_definition}"
            )

        error = None
        result = ""
        try:
            self.logger.debug(f"Command: {runcommand}")
            async with self.session.post(
                self.runpoint,
                json={
                    "client": "local_async",
                    "tgt": deployment.node.name,
                    "fun": "cmd.run",
                    "arg": [runcommand],
                    "username": self.username,
                    "password": self.password,
                    "eauth": self.eauth,
                },
            ) as response:
                result = await response.json()
                result = result.get("return", [{}])[0]
            if not result:
                raise Exception(
                    f"CherryPy returned an empty response - usually that means that node is not available: {result}"
                )
            result = result['jid']
        except Exception as e:
            self.logger.error(
                f"Exception during deployment.\n"
                f"Experiment id: {experiment_id}\n"
                f"Error: {e}\n"
                f"Deployment: {deployment}"
            )
            self.logger.debug(await response.text())
            error = str(e)

        # don't need to wait shell executions to finish
        if not error and not isinstance(
            deployment.environment_definition, ShellExecution
        ):
            for _ in range(10):
                try:
                    async with self.session.post(
                        self.runpoint,
                        json={
                            "client": "runner",
                            "fun": "jobs.list_job",
                            "arg": [result],
                            "username": self.username,
                            "password": self.password,
                            "eauth": self.eauth,
                        },
                    ) as response:
                        data = await response.json()
                        data = data.get("return", [{}])[0]
                except Exception as e:
                    self.logger.error(f"Exception during job list: {e}")
                    self.logger.debug(await response.text())
                    error = str(e)
                    break
                if not isinstance(data, dict) or "Error" in data:
                    self.logger.error(f"Job list returned error: {data}")
                    error = str(data)
                    break
                data = data.get("Result", {})
                if data:
                    return_code = data.get(deployment.node.name, {}).get("retcode", 1)
                    if return_code != 0:
                        error = data.get(deployment.node.name, {}).get(
                            "return", "Unknown error"
                        )
                    self.logger.info(f"Job finished with result: {result}")
                    break
                await asyncio.sleep(2)
            else:
                self.logger.error(f"Job {result} timed out")
                error = f"Job {result} timed out"

        if error:
            self.logger.error(
                f"Failed to start executor {deployment.executor_id} on node {deployment.node}: {error}"
            )
            return Failure(error)
        return Success(None)

    async def execute(
        self,
        username: str,
        experiment_id: str,
        deployments: list[Deployment],
        execution_context: Optional[dict[str, str]] = None,
        authentication_context: Optional[dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Result[None, str]]:

        keys = [deployment.executor_id for deployment in deployments]
        # Start all deployments
        answers: tuple[Exception | Result[None, str]] = await asyncio.gather(  # type: ignore
            *(
                self._start_single_execution(experiment_id, deployment)
                for deployment in deployments
            )
        )

        results: dict[str, Result[None, str]] = {}
        for key, answer in zip(keys, answers):
            if isinstance(answer, Exception):
                answer = Failure(str(answer))
            results[key] = answer

        return results

    async def stop_executors(
        self,
        username: str,
        requests_list: list[StopExecutorRequest],
        cancellation_context: Optional[dict[str, str]],
        authentication_context: Optional[dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Result[None, str]]:

        try:
            for request in requests_list:
                self.logger.debug(
                    f"Stopping executor {request['executor_id']} on node {request['node_name']}"
                )
                (
                    await self.session.post(
                        self.runpoint,
                        json={
                            "client": "local_async",
                            "tgt": request["node_name"],
                            "fun": "cmd.run",
                            "arg": [f"docker stop {request['executor_id']}"],
                            "username": self.username,
                            "password": self.password,
                            "eauth": self.eauth,
                        },
                    )
                ).raise_for_status()
        except Exception as e:
            self.logger.error(f"Error stopping executors: {e}")
            return {request["node_name"]: Failure(str(e)) for request in requests_list}

        return {request["node_name"]: Success(None) for request in requests_list}

    async def cleanup(
            self,
            experiment_id: str,
            deployments: list[Deployment],
            *args: Any,
            **kwargs: Any
    ) -> None:
        deployments = [deployment for deployment in deployments if isinstance(deployment.environment_definition, DockerImage)]

        # stop containers
        for deployment in deployments:
            try:
                await self.session.post(
                    self.runpoint,
                    json={
                        "client": "local",
                        "tgt": deployment.node.name,
                        "fun": "cmd.run",
                        "arg": [f"docker stop {deployment.executor_id}; docker rm {deployment.executor_id}"],
                        "username": self.username,
                        "password": self.password,
                        "eauth": self.eauth,
                    },
                )
            except Exception as e:
                self.logger.exception(f"Error stopping container: {e}")

        # remove images
        for deployment in deployments:
            try:
                await self.session.post(
                    self.runpoint,
                    json={
                        "client": "local",
                        "tgt": deployment.node.name,
                        "fun": "cmd.run",
                        "arg": [f"docker rmi {deployment.environment_definition.image}"],
                        "username": self.username,
                        "password": self.password,
                        "eauth": self.eauth,
                    },
                )
            except Exception as e:
                self.logger.exception(f"Error removing image: {e}")


async def debug():
    from netunicorn.base import Pipeline, Experiment, Task

    class DummyTask(Task):
        def run(self):
            return 0

    connector = SaltConnector(
        "debug", "configuration-example.yaml", "https://example.com"
    )
    await connector.initialize()
    await connector.health()
    nodes = await connector.get_nodes("debug")
    print(nodes)
    nodes = nodes.take(1)
    pipeline = Pipeline([DummyTask()])
    pipeline.environment_definition.image = "ubuntu:latest"
    experiment = Experiment().map(pipeline, nodes)
    for deployment in experiment.deployment_map:
        deployment.executor_id = "debug"
    await connector.deploy('debug', 'debug', experiment.deployment_map)
    await connector.execute('debug', 'debug', experiment.deployment_map)
    await connector.shutdown()


# debug
if __name__ == "__main__":
    asyncio.run(debug())
