"""Run-report accumulation and formatting."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CreatedRow:
    title: str
    notion_url: str
    gong_url: str


@dataclass
class SkippedRow:
    title: str
    notion_page_id: str


@dataclass
class FailedRow:
    title: str  # may be Gong call ID if title is unknown
    error_class: str
    message: str


@dataclass
class RunReport:
    window_start: str
    window_end: str
    candidates_from_gong: int = 0
    external_customer: int = 0
    private_excluded: int = 0
    dry_run: bool = False
    created: list[CreatedRow] = field(default_factory=list)
    skipped: list[SkippedRow] = field(default_factory=list)
    failed: list[FailedRow] = field(default_factory=list)

    def format(self) -> str:
        lines = [
            "Gong → Notion run report",
            f"  Window: {self.window_start} → {self.window_end}",
            f"  Candidates from Gong:   {self.candidates_from_gong}",
            f"  External-customer:      {self.external_customer}",
            f"  Private (excluded):     {self.private_excluded}",
            f"  Created:                {len(self.created)}",
            f"  Already existed:        {len(self.skipped)}",
            f"  Failed:                 {len(self.failed)}",
            f"  Dry run: {'yes — no writes performed' if self.dry_run else 'no'}",
        ]
        if self.created:
            lines.append("")
            lines.append("Created:")
            for c in self.created:
                lines.append(f"  - {c.title} — {c.notion_url} (Gong: {c.gong_url})")
        if self.skipped:
            lines.append("")
            lines.append("Already existed:")
            for s in self.skipped:
                lines.append(f"  - {s.title} — https://www.notion.so/{s.notion_page_id.replace('-', '')}")
        if self.failed:
            lines.append("")
            lines.append("Failed:")
            for f in self.failed:
                lines.append(f"  - {f.title} — {f.error_class}: {f.message}")
        return "\n".join(lines)
