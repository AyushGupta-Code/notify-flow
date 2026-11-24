#!/usr/bin/env python3
"""Package and deploy the executable Step Functions path.

The only LocalStack-specific input is --endpoint-url. Omit it and use normal AWS
credentials/region configuration to run the same deployment against AWS.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_PACKAGES = [
    "asyncpg>=0.29",
    "sqlalchemy>=2.0.30",
    "httpx>=0.27",
    "structlog>=24.1",
    "prometheus-client>=0.20",
    "pyyaml>=6.0",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
]
HANDLERS = {
    "select-channel": "notify_flow.aws_lambda.select_channel_handler",
    "quiet-hours": "notify_flow.aws_lambda.quiet_hours_handler",
    "personalize": "notify_flow.aws_lambda.personalize_handler",
    "send": "notify_flow.aws_lambda.send_handler",
    "engagement": "notify_flow.aws_lambda.engagement_handler",
    "escalation": "notify_flow.aws_lambda.escalation_handler",
}


def package_lambda(output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="notify-flow-lambda-") as temporary:
        target = Path(temporary)
        uv = shutil.which("uv")
        installer = (
            [uv, "pip", "install", "--python", sys.executable]
            if uv
            else [sys.executable, "-m", "pip", "install"]
        )
        subprocess.run(
            [*installer, "--quiet", "--target", str(target), *RUNTIME_PACKAGES],
            check=True,
            env={
                **os.environ,
                "UV_CACHE_DIR": os.getenv("UV_CACHE_DIR", str(target / ".uv-cache")),
            },
        )
        shutil.copytree(ROOT / "src" / "notify_flow", target / "notify_flow")
        shutil.copytree(ROOT / "prompts", target / "prompts")
        shutil.copytree(ROOT / "models", target / "models")
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in target.rglob("*"):
                if (
                    path.is_file()
                    and "__pycache__" not in path.parts
                    and ".uv-cache" not in path.parts
                ):
                    archive.write(path, path.relative_to(target))


def client(service: str, args: argparse.Namespace):
    kwargs = {"region_name": args.region}
    if args.endpoint_url:
        kwargs["endpoint_url"] = args.endpoint_url
    return boto3.client(service, **kwargs)


def ensure_role(iam, name: str, service: str) -> str:
    try:
        return iam.get_role(RoleName=name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}
            ],
        }
        return iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"][
            "Arn"
        ]


def put_function(lam, name: str, handler: str, role: str, code: bytes, env: dict[str, str]) -> str:
    try:
        response = lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=role,
            Handler=handler,
            Code={"ZipFile": code},
            Timeout=30,
            MemorySize=512,
            Environment={"Variables": env},
            Architectures=["x86_64"],
        )
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=name, ZipFile=code, Publish=True)
        lam.get_waiter("function_updated").wait(FunctionName=name)
        response = lam.update_function_configuration(
            FunctionName=name,
            Role=role,
            Handler=handler,
            Timeout=30,
            MemorySize=512,
            Environment={"Variables": env},
        )
    lam.get_waiter("function_active_v2").wait(FunctionName=name)
    return response["FunctionArn"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint-url", default=os.getenv("AWS_ENDPOINT_URL"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--database-url", default=os.getenv("NF_DATABASE_URL", ""))
    parser.add_argument("--name", default="notify-flow")
    parser.add_argument("--zip", type=Path)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or NF_DATABASE_URL is required")

    owned_zip = args.zip is None
    package_path = args.zip or Path(tempfile.mkstemp(suffix=".zip")[1])
    if owned_zip:
        package_lambda(package_path)
    code = package_path.read_bytes()

    iam = client("iam", args)
    lam = client("lambda", args)
    sfn = client("stepfunctions", args)
    lambda_role = ensure_role(iam, f"{args.name}-lambda", "lambda.amazonaws.com")
    sfn_role = ensure_role(iam, f"{args.name}-sfn", "states.amazonaws.com")
    try:
        iam.attach_role_policy(
            RoleName=f"{args.name}-lambda",
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        )
    except ClientError:
        if not args.endpoint_url:
            raise
    if not args.endpoint_url:
        time.sleep(10)

    environment = {
        "NF_DATABASE_URL": args.database_url,
        "NF_PROMPT_REGISTRY_PATH": "/var/task/prompts/registry.yaml",
        "NF_CHANNEL_MODEL_PATH": "/var/task/models/channel_lr_v1.json",
        "NF_QUIET_HOURS_ENABLED": "false",
        "NF_LLM_PROVIDER": "mock",
        "NF_LLM_MODEL": "mock-copywriter-1",
    }
    arns = {
        key: put_function(
            lam,
            f"{args.name}-{suffix}",
            handler,
            lambda_role,
            code,
            environment,
        )
        for key, (suffix, handler) in zip(
            (
                "SelectChannelFunctionArn",
                "QuietHoursFunctionArn",
                "PersonalizeFunctionArn",
                "SendFunctionArn",
                "EngagementFunctionArn",
                "EscalationFunctionArn",
            ),
            HANDLERS.items(),
            strict=True,
        )
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": list(arns.values())}
        ],
    }
    iam.put_role_policy(
        RoleName=f"{args.name}-sfn",
        PolicyName="invoke-notify-flow-lambdas",
        PolicyDocument=json.dumps(policy),
    )
    definition = (ROOT / "aws" / "stepfunctions" / "notification.asl.json").read_text()
    for placeholder, arn in arns.items():
        definition = definition.replace(f"${{{placeholder}}}", arn)
    try:
        state_machine_arn = sfn.create_state_machine(
            name=args.name, definition=definition, roleArn=sfn_role, type="STANDARD"
        )["stateMachineArn"]
    except sfn.exceptions.StateMachineAlreadyExists:
        state_machine_arn = next(
            item["stateMachineArn"]
            for item in sfn.list_state_machines()["stateMachines"]
            if item["name"] == args.name
        )
        sfn.update_state_machine(
            stateMachineArn=state_machine_arn, definition=definition, roleArn=sfn_role
        )
    result = {
        "state_machine_arn": state_machine_arn,
        "lambda_arns": arns,
        "package_bytes": len(code),
    }
    print(json.dumps(result, indent=2))
    if owned_zip:
        package_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
