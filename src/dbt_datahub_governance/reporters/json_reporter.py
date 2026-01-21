"""JSON reporter for machine-readable output."""

import json
import sys
from typing import IO, Optional

from dbt_datahub_governance.models.governance import ValidationReport
from dbt_datahub_governance.reporters.base import BaseReporter


class JsonReporter(BaseReporter):
    """JSON reporter for machine-readable output."""

    def __init__(self, output: Optional[IO[str]] = None, pretty: bool = True) -> None:
        self.output = output or sys.stdout
        self.pretty = pretty

    def report(self, validation_report: ValidationReport) -> None:
        data = validation_report.to_dict()
        json_str = json.dumps(data, indent=2) if self.pretty else json.dumps(data)
        self.output.write(json_str + "\n")
