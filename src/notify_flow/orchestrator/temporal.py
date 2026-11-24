from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from notify_flow.orchestrator.base import StartResult, workflow_id_for
from notify_flow.types import NotificationInput
from notify_flow.workflows.notification import MemberNotificationWorkflow


class TemporalOrchestrator:
    name = "temporal"

    def __init__(self, client: Client, task_queue: str) -> None:
        self.client = client
        self.task_queue = task_queue

    async def start_notification(self, inp: NotificationInput) -> StartResult:
        wid = workflow_id_for(inp.event_id)
        try:
            await self.client.start_workflow(
                MemberNotificationWorkflow.run,
                inp,
                id=wid,
                task_queue=self.task_queue,
                # Never run a second workflow for the same event, even after the first closes.
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                memo={"correlation_id": inp.correlation_id or ""},
            )
        except WorkflowAlreadyStartedError:
            return StartResult(wid, started=False)
        return StartResult(wid, started=True)

    async def signal_engagement(self, workflow_id: str) -> None:
        handle = self.client.get_workflow_handle(workflow_id)
        await handle.signal(MemberNotificationWorkflow.engaged)
