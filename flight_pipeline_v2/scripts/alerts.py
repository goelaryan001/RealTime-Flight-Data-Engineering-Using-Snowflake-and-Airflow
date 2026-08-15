"""
Failure alerting for the flight pipeline DAG.

Uses a Slack incoming webhook — set the SLACK_WEBHOOK_URL Airflow Variable
(Admin > Variables in the UI, or via `airflow variables set`) to your
webhook URL. If it's not configured, this logs a warning instead of
silently doing nothing, so a missing alert channel is itself visible.
"""
import logging
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def slack_failure_alert(context):
    """Airflow on_failure_callback — posts a message to Slack when any task fails."""
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        logger.warning(
            "SLACK_WEBHOOK_URL not configured — task failure was NOT alerted. "
            "Set this Airflow Variable to enable Slack notifications."
        )
        return

    task_instance = context["task_instance"]
    message = {
        "text": (
            f":red_circle: *Flight pipeline task failed*\n"
            f"*DAG*: {task_instance.dag_id}\n"
            f"*Task*: {task_instance.task_id}\n"
            f"*Execution date*: {context['ds']}\n"
            f"*Log URL*: {task_instance.log_url}"
        )
    }

    try:
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        # Don't let a broken alert channel mask the original task failure
        logger.error(f"Failed to send Slack alert: {e}")
