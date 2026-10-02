from dataclasses import asdict, dataclass


@dataclass
class SourceEntry:
    id: str
    path: str
    title: str
    type: str            # pdf|ppt|pptx|doc|docx|txt|folder
    source_class: str    # library|practice
    cleared: bool
    license_risk: str    # low|high
    year: int | None = None
    market: str | None = None
    level: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["class"] = d.pop("source_class")
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "SourceEntry":
        d = dict(d)
        d["source_class"] = d.pop("class")
        return cls(**d)
