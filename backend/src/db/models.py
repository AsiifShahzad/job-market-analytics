"""
SQLAlchemy ORM models for JobPulse AI.
Single source of truth — no duplicate definitions anywhere.
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, Boolean, Float, DateTime,
    ForeignKey, Text, Index, UniqueConstraint, func
)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship

Base = declarative_base()


class Job(Base):
    __tablename__ = "job"
    __table_args__ = (
        Index("idx_job_created_at", "created_at"),
        Index("idx_job_city_country", "city", "country"),
        Index("idx_job_remote", "remote"),
    )

    id: Mapped[str] = mapped_column(String(100), primary_key=True)  # Adzuna job ID
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    company: Mapped[str] = mapped_column(String(255), nullable=False)
    location_raw: Mapped[Optional[str]] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    country: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    salary_min: Mapped[Optional[float]] = mapped_column(Float)
    salary_max: Mapped[Optional[float]] = mapped_column(Float)
    salary_mid: Mapped[Optional[float]] = mapped_column(Float)
    remote: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    seniority: Mapped[Optional[str]] = mapped_column(String(50))
    url: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(50), default="adzuna")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # When the job was posted on Adzuna (from API "created" field)
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    # When we inserted it into our DB
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    # The keyword group semantic tag used to fetch the job
    search_keyword: Mapped[Optional[str]] = mapped_column(String(100), index=True)

    skills: Mapped[List["JobSkill"]] = relationship(
        "JobSkill", back_populates="job", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} title={self.title!r}>"


class Skill(Base):
    __tablename__ = "skill"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    category: Mapped[str] = mapped_column(
        String(50), nullable=False, default="tool",
        comment="language/framework/cloud/tool/data/soft"
    )

    job_skills: Mapped[List["JobSkill"]] = relationship(
        "JobSkill", back_populates="skill", cascade="all, delete-orphan"
    )
    snapshots: Mapped[List["SkillSnapshot"]] = relationship(
        "SkillSnapshot", back_populates="skill", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Skill id={self.id} name={self.name!r}>"


class JobSkill(Base):
    __tablename__ = "job_skill"
    __table_args__ = (
        UniqueConstraint("job_id", "skill_id", name="uq_job_skill"),
    )

    job_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("job.id", ondelete="CASCADE"), primary_key=True
    )
    skill_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("skill.id", ondelete="CASCADE"), primary_key=True
    )
    is_required: Mapped[bool] = mapped_column(Boolean, default=True)

    job: Mapped["Job"] = relationship("Job", back_populates="skills")
    skill: Mapped["Skill"] = relationship("Skill", back_populates="job_skills")


class SkillSnapshot(Base):
    """Weekly snapshot of skill demand — used for trend calculations."""
    __tablename__ = "skill_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "skill_id", "snapshot_date", "city", "country",
            name="uq_skill_snapshot"
        ),
        Index("idx_snapshot_date", "snapshot_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    skill_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("skill.id", ondelete="CASCADE"), nullable=False, index=True
    )
    snapshot_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    job_count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_salary_mid: Mapped[Optional[float]] = mapped_column(Float)
    city: Mapped[Optional[str]] = mapped_column(String(100))
    country: Mapped[Optional[str]] = mapped_column(String(50))

    skill: Mapped["Skill"] = relationship("Skill", back_populates="snapshots")


class PipelineRun(Base):
    """Audit trail — one row per GitHub Actions / manual pipeline execution."""
    __tablename__ = "pipeline_run"
    __table_args__ = (
        Index("idx_pipeline_started_at", "started_at"),
        Index("idx_pipeline_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running",
        comment="running / success / failed"
    )
    jobs_fetched: Mapped[int] = mapped_column(Integer, default=0)
    jobs_inserted: Mapped[int] = mapped_column(Integer, default=0)
    jobs_skipped: Mapped[int] = mapped_column(Integer, default=0)
    unique_skills: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    def __repr__(self) -> str:
        return f"<PipelineRun id={self.id} status={self.status}>"