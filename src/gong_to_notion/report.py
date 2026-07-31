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
class GapRow:
    """A created call that's missing something a human should fix.

    `details` is a free-form list of one-line strings explaining what
    (e.g. unresolved SF Account IDs, attendee domains with no Agency).
    """
    title: str
    notion_url: str
    details: list[str] = field(default_factory=list)


@dataclass
class RunReport:
    window_start: str
    window_end: str
    candidates_from_gong: int = 0
    external_customer: int = 0
    private_excluded: int = 0
    dry_run: bool = False
    # Staff rows matched through `Other Emails` where this run's call was the
    # most recent one, so the address it used became the row's primary Email.
    staff_emails_promoted: int = 0
    created: list[CreatedRow] = field(default_factory=list)
    skipped: list[SkippedRow] = field(default_factory=list)
    failed: list[FailedRow] = field(default_factory=list)
    no_agency: list[GapRow] = field(default_factory=list)
    no_staff: list[GapRow] = field(default_factory=list)
    no_purpose: list[GapRow] = field(default_factory=list)

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
        if self.staff_emails_promoted:
            lines.append(
                f"  Staff primary email updated to a newer address: "
                f"{self.staff_emails_promoted}"
            )
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

        gap_total = len(self.no_agency) + len(self.no_staff) + len(self.no_purpose)
        if gap_total:
            lines.append("")
            lines.append(
                f"Gaps in created pages ({gap_total} — flagged for human follow-up):"
            )
            if self.no_agency:
                lines.append(f"  No Agency linked ({len(self.no_agency)}):")
                for g in self.no_agency:
                    lines.append(f"    - {g.title} — {g.notion_url}")
                    for d in g.details:
                        lines.append(f"        {d}")
            if self.no_staff:
                lines.append(f"  No Agency Staff linked ({len(self.no_staff)}):")
                for g in self.no_staff:
                    lines.append(f"    - {g.title} — {g.notion_url}")
                    for d in g.details:
                        lines.append(f"        {d}")
            if self.no_purpose:
                lines.append(
                    f"  No Purpose set ({len(self.no_purpose)} — run customer-interactions-judgment-fill):"
                )
                for g in self.no_purpose:
                    lines.append(f"    - {g.title} — {g.notion_url}")

        return "\n".join(lines)
