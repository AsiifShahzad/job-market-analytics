"""
Database ORM models for JobPulse AI using SQLAlchemy async ORM
All models use mapped_column style with asyncpg driver support
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, Boolean, Float, DateTime, ForeignKey, 
    Text, Index, UniqueConstraint, func
)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship
from sqlalchemy.sql import expression

Base = declarative_base()


class Job(Base):
    """Job listing model - stores individual job postings from Adzuna API"""
    __tablename__ = "job"
    __table_args__ = (
        Index("idx_job_created_at", "created_at"),
        Index("idx_job_source", "source"),
        Index("idx_job_city_country", "city", "country"),
    )

    id: Mapped[str] = mapped_column(String(100), primary_key=True)  # Adzuna job_id
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    company: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    location_raw: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    country: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    salary_min: Mapped[Optional[float]] = mapped_column(Float)
    salary_max: Mapped[Optional[float]] = mapped_column(Float)
    salary_mid: Mapped[Optional[float]] = mapped_column(Float)  # Computed: (min+max)/2
    description: Mapped[str] = mapped_column(Text)
    seniority: Mapped[Optional[str]] = mapped_column(String(50), index=True)  # junior/mid/senior/lead
    remote: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    source: Mapped[str] = mapped_column(String(50), default="adzuna", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )

    # Relationships
    skills: Mapped[List["JobSkill"]] = relationship("JobSkill", back_populates="job", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, title={self.title}, company={self.company})>"


class Skill(Base):
    """Skills ontology - normalized skill names and categories"""
    __tablename__ = "skill"
    __table_args__ = (
        Index("idx_skill_category", "category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    category: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        default="tool",
        comment="language/framework/cloud/tool/data/soft"
    )

    # Relationships
    job_skills: Mapped[List["JobSkill"]] = relationship("JobSkill", back_populates="skill", cascade="all, delete-orphan")
    snapshots: Mapped[List["SkillSnapshot"]] = relationship("SkillSnapshot", back_populates="skill", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Skill(id={self.id}, name={self.name}, category={self.category})>"


class JobSkill(Base):
    """Junction table linking jobs to skills with requirement flag"""
    __tablename__ = "job_skill"
    __table_args__ = (
        UniqueConstraint("job_id", "skill_id", name="uq_job_skill"),
        Index("idx_job_skill_required", "is_required"),
    )

    job_id: Mapped[str] = mapped_column(String(100), ForeignKey("job.id", ondelete="CASCADE"), primary_key=True)
    skill_id: Mapped[int] = mapped_column(Integer, ForeignKey("skill.id", ondelete="CASCADE"), primary_key=True)
    is_required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    job: Mapped["Job"] = relationship("Job", back_populates="skills")
    skill: Mapped["Skill"] = relationship("Skill", back_populates="job_skills")

    def __repr__(self) -> str:
        return f"<JobSkill(job_id={self.job_id}, skill_id={self.skill_id}, required={self.is_required})>"


class SkillSnapshot(Base):
    """Time-series snapshots of skill demand metrics by city/country"""
    __tablename__ = "skill_snapshot"
    __table_args__ = (
        UniqueConstraint("skill_id", "snapshot_date", "city", "country", name="uq_skill_snapshot"),
        Index("idx_skill_snapshot_date", "snapshot_date"),
        Index("idx_skill_snapshot_city_country", "city", "country"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    skill_id: Mapped[int] = mapped_column(Integer, ForeignKey("skill.id", ondelete="CASCADE"), nullable=False, index=True)
    snapshot_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    job_count: Mapped[int] = mapped_column(Integer, nullable=False)  # How many jobs have this skill on this date
    avg_salary_mid: Mapped[Optional[float]] = mapped_column(Float)  # Average salary for jobs with this skill
    tfidf_score: Mapped[Optional[float]] = mapped_column(Float)  # TF-IDF importance score for this skill
    city: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    country: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    # Relationships
    skill: Mapped["Skill"] = relationship("Skill", back_populates="snapshots")

    def __repr__(self) -> str:
        return f"<SkillSnapshot(skill_id={self.skill_id}, date={self.snapshot_date}, count={self.job_count}, tfidf={self.tfidf_score})>"


class PipelineRun(Base):
    """Audit trail for pipeline executions"""
    __tablename__ = "pipeline_run"
    __table_args__ = (
        Index("idx_pipeline_status", "status"),
        Index("idx_pipeline_started_at", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True,
        default="running",
        comment="running/success/failed"
    )
    jobs_fetched: Mapped[int] = mapped_column(Integer, default=0)
    jobs_inserted: Mapped[int] = mapped_column(Integer, default=0)
    jobs_skipped: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    def __repr__(self) -> str:
        return f"<PipelineRun(id={self.id}, status={self.status}, fetched={self.jobs_fetched})>"
