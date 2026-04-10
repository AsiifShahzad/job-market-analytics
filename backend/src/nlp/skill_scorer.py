"""
TF-IDF based skill importance scoring for pipeline runs.
Computes per-skill importance scores across all job descriptions in a pipeline run,
stores results in SkillSnapshot.tfidf_score in Neon PostgreSQL.
"""

import structlog
from typing import Dict, List, Optional, Tuple
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from datetime import datetime

logger = structlog.get_logger(__name__)


class SkillScorer:
    """TF-IDF based skill importance scorer for job market analytics."""
    
    def __init__(self, min_df: int = 2, max_df: float = 0.95):
        """
        Initialize TF-IDF vectorizer with sane defaults.
        
        Args:
            min_df: Minimum document frequency (absolute count)
            max_df: Maximum document frequency (proportion, 0.0-1.0)
        """
        self.min_df = min_df
        self.max_df = max_df
        self.vectorizer = TfidfVectorizer(
            min_df=min_df,
            max_df=max_df,
            max_features=500,
            stop_words="english",
            token_pattern=r"\b[a-z][a-z0-9+#.\-]*\b",  # Match skill patterns
            lowercase=True,
        )
        self.corpus = []
        self.skill_names = []
        self.tfidf_matrix = None
    
    def add_documents(self, texts: List[str]) -> None:
        """
        Add documents (job descriptions) to corpus for TF-IDF fitting.
        
        Args:
            texts: List of job description strings
        """
        self.corpus.extend(texts)
        logger.info("documents_added_to_corpus", corpus_size=len(self.corpus))
    
    def fit(self) -> None:
        """
        Fit TF-IDF vectorizer on corpus. Must be called before scoring.
        """
        if not self.corpus:
            logger.warning("cannot_fit_empty_corpus")
            return
        
        try:
            self.tfidf_matrix = self.vectorizer.fit_transform(self.corpus)
            self.skill_names = list(self.vectorizer.get_feature_names_out())
            logger.info(
                "tfidf_vectorizer_fitted",
                corpus_size=len(self.corpus),
                vocab_size=len(self.skill_names),
                matrix_shape=self.tfidf_matrix.shape,
            )
        except Exception as e:
            logger.error("tfidf_fit_failed", error=str(e))
            raise
    
    def get_skill_importance_scores(self) -> Dict[str, float]:
        """
        Compute average TF-IDF score for each skill across entire corpus.
        
        Returns:
            Dict mapping skill name to average TF-IDF importance (0.0-1.0)
        """
        if self.tfidf_matrix is None:
            logger.warning("tfidf_matrix_not_fitted")
            return {}
        
        # Convert to dense array for easier computation
        dense_matrix = self.tfidf_matrix.toarray()
        
        # Compute mean TF-IDF score per feature (skill)
        mean_scores = np.mean(dense_matrix, axis=0)
        
        scores = {}
        for skill_name, score in zip(self.skill_names, mean_scores):
            if score > 0:  # Only include skills with non-zero scores
                scores[skill_name] = float(score)
        
        # Sort by importance
        sorted_scores = dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))
        
        logger.info(
            "skill_importance_computed",
            total_skills_with_scores=len(sorted_scores),
            top_3_skills=list(sorted_scores.keys())[:3],
            top_3_scores=[round(v, 4) for v in list(sorted_scores.values())[:3]],
        )
        
        return sorted_scores
    
    def get_top_skills(self, k: int = 10) -> List[Tuple[str, float]]:
        """
        Get top-k most important skills by TF-IDF score.
        
        Args:
            k: Number of top skills to return
            
        Returns:
            List of (skill_name, score) tuples sorted by score descending
        """
        scores = self.get_skill_importance_scores()
        return list(scores.items())[:k]


async def score_skills_for_run(
    db: AsyncSession,
    run_id: int,
    job_skill_records: List[Dict],
    descriptions: List[str],
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Score skills for a pipeline run using TF-IDF and update SkillSnapshot records.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        job_skill_records: List of {job_id, skill_name} dicts from JobSkill table
        descriptions: List of job descriptions for TF-IDF fitting
        log_context: Optional logging context
        
    Returns:
        Dict with scoring results and statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}))
    logger.info(
        "skill_scoring_started",
        run_id=run_id,
        job_skill_count=len(job_skill_records),
        description_count=len(descriptions),
    )
    
    try:
        # Initialize scorer and fit on descriptions
        scorer = SkillScorer(min_df=2, max_df=0.95)
        scorer.add_documents(descriptions)
        scorer.fit()
        
        # Get importance scores
        skill_scores = scorer.get_skill_importance_scores()
        top_10 = scorer.get_top_skills(k=10)
        
        logger.info(
            "tfidf_scores_computed",
            total_skills_scored=len(skill_scores),
            top_skills=[f"{skill} ({score:.3f})" for skill, score in top_10],
        )
        
        # Update SkillSnapshot records with TF-IDF scores
        # Note: This assumes SkillSnapshot records already exist from aggregation
        updated_count = 0
        for record in job_skill_records:
            skill_name = record.get("skill_name")
            tfidf_score = skill_scores.get(skill_name, 0.0)
            
            if tfidf_score > 0:
                # Update using raw SQL for efficiency with large batches
                # This assumes SkillSnapshot has (run_id, skill_name, tfidf_score)
                query = (
                    update("skill_snapshot")
                    .where((select("run_id").table == run_id) & (select("skill_name").table == skill_name))
                    .values(tfidf_score=tfidf_score)
                    .execution_options(synchronize_session=False)
                )
                await db.execute(query)
                updated_count += len([1 for r in job_skill_records if r.get("skill_name") == skill_name])
        
        await db.commit()
        
        logger.info(
            "skill_snapshot_tfidf_updated",
            run_id=run_id,
            updated_records=updated_count,
            unique_skills=len(skill_scores),
        )
        
        return {
            "run_id": run_id,
            "unique_skills_scored": len(skill_scores),
            "total_skill_instances_updated": updated_count,
            "top_10_skills": [(skill, float(score)) for skill, score in top_10],
            "scoring_timestamp": datetime.utcnow().isoformat(),
        }
        
    except Exception as e:
        logger.error("skill_scoring_failed", run_id=run_id, error=str(e))
        await db.rollback()
        raise


async def batch_score_skills(
    db: AsyncSession,
    run_id: int,
    batch_size: int = 1000,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Efficiently score all skills for a run in batches.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        batch_size: Batch size for processing
        log_context: Optional logging context
        
    Returns:
        Dict with overall scoring statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}))
    logger.info("batch_skill_scoring_started", run_id=run_id, batch_size=batch_size)
    
    try:
        # Fetch all descriptions for this run
        from src.db.models import Job
        
        query = select(Job.id, Job.description).where(Job.pipeline_run_id == run_id)
        result = await db.execute(query)
        job_descriptions = result.fetchall()
        
        descriptions = [desc for _, desc in job_descriptions if desc]
        
        if not descriptions:
            logger.warning("no_descriptions_found", run_id=run_id)
            return {"run_id": run_id, "descriptions_found": 0}
        
        # Fetch all job skills
        from src.db.models import JobSkill
        
        query = select(JobSkill.job_id, JobSkill.skill_id).where(JobSkill.pipeline_run_id == run_id)
        result = await db.execute(query)
        job_skills = result.fetchall()
        
        job_skill_records = [
            {"job_id": job_id, "skill_id": skill_id}
            for job_id, skill_id in job_skills
        ]
        
        logger.info(
            "data_fetched",
            descriptions_count=len(descriptions),
            job_skill_count=len(job_skill_records),
        )
        
        # Score skills
        result = await score_skills_for_run(
            db=db,
            run_id=run_id,
            job_skill_records=job_skill_records,
            descriptions=descriptions,
            log_context=log_context,
        )
        
        return result
        
    except Exception as e:
        logger.error("batch_scoring_failed", run_id=run_id, error=str(e))
        raise


def compute_skill_percentiles(scores: Dict[str, float]) -> Dict[str, float]:
    """
    Convert TF-IDF scores to percentiles for easier interpretation.
    
    Args:
        scores: Dict of skill_name -> tfidf_score
        
    Returns:
        Dict of skill_name -> percentile_rank (0-100)
    """
    if not scores:
        return {}
    
    sorted_scores = sorted(scores.values())
    percentiles = {}
    
    for skill, score in scores.items():
        # Calculate percentile rank
        rank = sum(1 for s in sorted_scores if s <= score)
        percentile = (rank / len(sorted_scores)) * 100
        percentiles[skill] = percentile
    
    return percentiles
