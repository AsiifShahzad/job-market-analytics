"""Initial migration: create all core tables

Revision ID: 001_initial_schema
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create job table
    op.create_table(
        'job',
        sa.Column('id', sa.String(100), nullable=False),
        sa.Column('title', sa.String(255), nullable=False),
        sa.Column('company', sa.String(255), nullable=False),
        sa.Column('location_raw', sa.String(255), nullable=False),
        sa.Column('city', sa.String(100), nullable=True),
        sa.Column('country', sa.String(50), nullable=True),
        sa.Column('salary_min', sa.Float(), nullable=True),
        sa.Column('salary_max', sa.Float(), nullable=True),
        sa.Column('salary_mid', sa.Float(), nullable=True),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('seniority', sa.String(50), nullable=True),
        sa.Column('remote', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('source', sa.String(50), nullable=False, server_default='adzuna'),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_job_created_at', 'job', ['created_at'])
    op.create_index('idx_job_source', 'job', ['source'])
    op.create_index('idx_job_city_country', 'job', ['city', 'country'])
    op.create_index('ix_job_title', 'job', ['title'])
    op.create_index('ix_job_company', 'job', ['company'])
    op.create_index('idx_job_seniority', 'job', ['seniority'])
    op.create_index('idx_job_remote', 'job', ['remote'])

    # Create skill table
    op.create_table(
        'skill',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('category', sa.String(50), nullable=False, server_default='tool'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name')
    )
    op.create_index('ix_skill_name', 'skill', ['name'])
    op.create_index('idx_skill_category', 'skill', ['category'])

    # Create job_skill junction table
    op.create_table(
        'job_skill',
        sa.Column('job_id', sa.String(100), nullable=False),
        sa.Column('skill_id', sa.Integer(), nullable=False),
        sa.Column('is_required', sa.Boolean(), nullable=False, server_default='true'),
        sa.ForeignKeyConstraint(['job_id'], ['job.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['skill_id'], ['skill.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('job_id', 'skill_id'),
        sa.UniqueConstraint('job_id', 'skill_id', name='uq_job_skill')
    )
    op.create_index('idx_job_skill_required', 'job_skill', ['is_required'])

    # Create skill_snapshot table
    op.create_table(
        'skill_snapshot',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('skill_id', sa.Integer(), nullable=False),
        sa.Column('snapshot_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('job_count', sa.Integer(), nullable=False),
        sa.Column('avg_salary_mid', sa.Float(), nullable=True),
        sa.Column('city', sa.String(100), nullable=True),
        sa.Column('country', sa.String(50), nullable=True),
        sa.ForeignKeyConstraint(['skill_id'], ['skill.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('skill_id', 'snapshot_date', 'city', 'country', name='uq_skill_snapshot')
    )
    op.create_index('idx_skill_snapshot_date', 'skill_snapshot', ['snapshot_date'])
    op.create_index('idx_skill_snapshot_city_country', 'skill_snapshot', ['city', 'country'])
    op.create_index('idx_skill_snapshot_skill_id', 'skill_snapshot', ['skill_id'])

    # Create pipeline_run table
    op.create_table(
        'pipeline_run',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='running'),
        sa.Column('jobs_fetched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('jobs_inserted', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('jobs_skipped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_pipeline_status', 'pipeline_run', ['status'])
    op.create_index('idx_pipeline_started_at', 'pipeline_run', ['started_at'])


def downgrade() -> None:
    op.drop_table('pipeline_run')
    op.drop_table('skill_snapshot')
    op.drop_table('job_skill')
    op.drop_table('skill')
    op.drop_table('job')
