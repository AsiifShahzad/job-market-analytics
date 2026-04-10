"""Migration to add tfidf_score column to skill_snapshot

Revision ID: 002_add_tfidf_score
Revises: 001_initial_schema
Create Date: 2024-01-02 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '002_add_tfidf_score'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('skill_snapshot', sa.Column('tfidf_score', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('skill_snapshot', 'tfidf_score')
