import logging
from typing import Any, Dict
from instabase.metrics_utils.metric_constants import EMBEDDING_SOURCE_TYPE, UNKNOWN_ATTRIBUTE_VALUE
from instabase.provenance.registration import register_fn
from instabase.udf_utils.ib_intelligence.functions import IntelligencePlatform

from .save_filename_send_email_wrapper import reporting_end_flow_review_email


def get_unique_id(job_id: str) -> str:
  """Returns the weaviate class name from flow job id."""
  prefix = "AI_LABS_FLOW"

  # make sure to remove hyphens since weaviate class doesnt allow it
  unique_id = job_id.replace("-", "_")

  # Add prefix
  final_id = f"{prefix}_{unique_id}"
  return final_id


def delete_index(
    ib_intel: IntelligencePlatform,
    cached_index: str,
    app_id: str,
    **kwargs: Any,
) -> None:
  custom_result = ib_intel.run_model(
      model_name="ibllm",
      model_version="2.1.0",
      task="delete_class_index",
      cached_index=cached_index,
      source_id=app_id,
      source_type=EMBEDDING_SOURCE_TYPE,
      **kwargs)["custom_result"]
  if custom_result["error"]:
    raise ValueError(f"delete_class_index failed in the flow with "
                     f"traceback: {custom_result['traceback']}")





@register_fn(provenance=False)
def post_flow_cleanup(job_id: str, runtime_config: Dict, *args: Any,
                      **kwargs: Any) -> None:
  index_class_name = get_unique_id(job_id)




  # Delete index on weaviate, suppress exception and log it here.
  try:
    resp = reporting_end_flow_review_email(**kwargs)

    
    ip_sdk = IntelligencePlatform(kwargs, use_model_service_lite=True)
    app_id = runtime_config.get('app_id', UNKNOWN_ATTRIBUTE_VALUE)
    logging.info(
        f"Trying to delete weaviate index with class name: {index_class_name}")
    delete_index(ip_sdk, index_class_name, app_id, **kwargs)

  except Exception as e:
    logging.info(
        f"Error in deleting weaviate index: {index_class_name}. Exception: {str(e)}"
    )
