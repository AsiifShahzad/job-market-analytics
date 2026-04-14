"""
Seniority Level Extractor — Production-grade NLP module.

Detects experience levels from job titles and descriptions:
- junior / entry-level
- mid-level / intermediate
- senior
- lead / principal / staff

Features:
- Regex + keyword matching (no ML required)
- Confidence scoring for ambiguous cases
- Contextual analysis (years of experience mentioned)
- Handles compound titles (e.g., "Senior Staff Engineer")
- Fallback to "unspecified" for unclear cases

Author: NLP Engineer
Standards: PEP 8, production-ready
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Seniority Level Definitions & Confidence Scoring
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SeniorityResult:
    """Result of seniority extraction."""
    level: str  # "junior", "mid", "senior", "lead", "unspecified"
    confidence: float  # 0.0 - 1.0
    keywords_matched: list  # keywords that matched
    reasoning: str  # explanation of decision


# ── JUNIOR / ENTRY-LEVEL ──────────────────────────────────────────────────────

JUNIOR_PATTERNS = {
    "title_keywords": [
        r"\bjunior\b", r"\btr?ainee\b", r"\bengineering student\b",
        r"\bgrad\b", r"\bentr(?:y|ance)(-| )?level",
        r"\bentry\b", r"\bjr\.?\b(?!\w)", r"\bnewgrad\b",
        r"\bbeginning professional\b"
    ],
    "desc_keywords": [
        "entry level position", "early in career", "fresh graduate",
        "recent graduate", "first role", "newly graduated",
        "starts with basics", "foundational knowledge", "learns from",
        "first engineering position", "entry-level candidate",
        "less than 2 years", "0-2 years"
    ],
    "min_years": 0,
    "max_years": 2,
    "confidence_base": 0.85
}

# ── MID-LEVEL ─────────────────────────────────────────────────────────────────

MID_PATTERNS = {
    "title_keywords": [
        r"\bmid(-| )?level", r"\bintermediate\b", r"\bii\b",
        r"\bprincipal engineer\b", r"\bassociate\b",
        r"engineer ii", "software engineer ii"
    ],
    "desc_keywords": [
        "3-5 years", "4-6 years", "3 years experience", "5 years experience",
        "mid-career", "intermediate level", "independent contributor",
        "lead small projects", "mentor junior", "contribute to design",
        "solve complex problems", "take ownership"
    ],
    "min_years": 3,
    "max_years": 7,
    "confidence_base": 0.80
}

# ── SENIOR ────────────────────────────────────────────────────────────────────

SENIOR_PATTERNS = {
    "title_keywords": [
        r"\bsenior\b", r"\bsr\.?\b(?!\w)", r"\biii\b",
        r"\bseniority\b", r"\bprincipal\b",
        r"senior engineer", "senior developer", "senior architect",
        r"\bestablished professional\b"
    ],
    "desc_keywords": [
        "5+ years", "6+ years", "7+ years", "8 years", "9 years", "10 years",
        "5-7 years", "6-8 years", "7-10 years",
        "senior level", "extensive experience", "deep expertise",
        "architect solutions", "lead teams", "mentor team", "strategic",
        "make critical decisions", "guide technical direction", "own products",
        "proven track record", "advanced knowledge"
    ],
    "min_years": 6,
    "max_years": 15,
    "confidence_base": 0.85
}

# ── LEAD / PRINCIPAL / STAFF ──────────────────────────────────────────────────

LEAD_PATTERNS = {
    "title_keywords": [
        r"\blead\b", r"\bleading\b", r"\bstaff\b",
        r"\barchitect\b", r"\bvp\b", r"\bvp\s", r"\bdirector\b",
        r"\bhead of\b", r"\bchief\b", r"\bchief technology\b",
        r"\bfellow\b", r"\bprincipal engineer\b",
        "lead engineer", "lead architect", "staff engineer", "staff architect",
        r"\bmanager\b", r"\bsupervisor\b", r"\btrack record\b", r"\btrack\b"
    ],
    "desc_keywords": [
        "10+ years", "12+ years", "15+ years", "20+ years",
        "10-15 years", "15-20 years",
        "lead development", "lead engineering", "lead team",
        "guide technical", "set technical direction", "architecture",
        "exceptional track record", "thought leader", "industry expert",
        "mentor many", "build teams", "organizational impact",
        "executive", "strategic leadership", "vision", "innovation"
    ],
    "min_years": 10,
    "max_years": 50,
    "confidence_base": 0.88
}

# ── Confusing patterns (reduce confidence) ─────────────────────────────────────

AMBIGUOUS_PATTERNS = [
    r"(?:looking|seeking|hiring).{0,50}(?:developer|engineer)",  # Job posting phrasing
    r"(?:excellent|great|strong) communication",  # Soft skill emphasis
    r"(?:be|become).{0,30}(?:engineer|developer)",  # Aspirational language
]


# ══════════════════════════════════════════════════════════════════════════════
# Main Extractor
# ══════════════════════════════════════════════════════════════════════════════

def extract_seniority(title: str, description: str) -> SeniorityResult:
    """
    Extracts seniority level from job title and description.
    
    Algorithm:
    1. Check for explicit mentions in title (high weight)
    2. Check for experience years in description
    3. Analyze description keywords
    4. Score confidence and apply ambiguity penalties
    5. Return most confident level
    
    Args:
        title: Job title string
        description: Job description string
    
    Returns:
        SeniorityResult with level, confidence, keywords, and reasoning
    """
    if not title or not description:
        return SeniorityResult(
            level="unspecified",
            confidence=0.0,
            keywords_matched=[],
            reasoning="Missing title or description"
        )
    
    text_combined = f"{title} {description}".lower()
    
    # ────────────────────────────────────────────────────────────────────────
    # STEP 1: Check for ambiguous patterns (reduce confidence later)
    # ────────────────────────────────────────────────────────────────────────
    
    ambiguity_penalty = 0.0
    for pattern in AMBIGUOUS_PATTERNS:
        if re.search(pattern, text_combined, re.IGNORECASE):
            ambiguity_penalty += 0.15
    ambiguity_penalty = min(ambiguity_penalty, 0.4)  # Max 40% penalty
    
    # ────────────────────────────────────────────────────────────────────────
    # STEP 2: Extract experience years from description
    # ────────────────────────────────────────────────────────────────────────
    
    years_info = _extract_years_experience(description)
    
    # ────────────────────────────────────────────────────────────────────────
    # STEP 3: Score each seniority level
    # ────────────────────────────────────────────────────────────────────────
    
    scores = {
        "lead": _score_level(title, description, years_info, LEAD_PATTERNS),
        "senior": _score_level(title, description, years_info, SENIOR_PATTERNS),
        "mid": _score_level(title, description, years_info, MID_PATTERNS),
        "junior": _score_level(title, description, years_info, JUNIOR_PATTERNS),
    }
    
    # ────────────────────────────────────────────────────────────────────────
    # STEP 4: Apply ambiguity penalty
    # ────────────────────────────────────────────────────────────────────────
    
    for level in scores:
        if scores[level]["confidence"] > 0:
            scores[level]["confidence"] = max(
                scores[level]["confidence"] - ambiguity_penalty,
                0.0
            )
    
    # ────────────────────────────────────────────────────────────────────────
    # STEP 5: Select best match
    # ────────────────────────────────────────────────────────────────────────
    
    best_level = max(scores.items(), key=lambda x: x[1]["confidence"])
    level_name, level_data = best_level
    
    # Only return if confidence above threshold
    if level_data["confidence"] < 0.4:
        return SeniorityResult(
            level="unspecified",
            confidence=0.0,
            keywords_matched=[],
            reasoning="No clear seniority indicators found (confidence below threshold)"
        )
    
    return SeniorityResult(
        level=level_name,
        confidence=round(level_data["confidence"], 3),
        keywords_matched=level_data["keywords_matched"],
        reasoning=level_data["reasoning"]
    )


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions
# ══════════════════════════════════════════════════════════════════════════════

def _extract_years_experience(description: str) -> Optional[int]:
    """
    Extracts explicit years of experience from description.
    Looks for patterns like '5+ years', '5-7 years', '5 years experience', etc.
    
    Returns:
        int representing years mentioned, or None if not found
    """
    description_lower = description.lower()
    
    # Pattern: "5+ years", "5+ years of experience"
    match = re.search(r'(\d+)\+?\s*years', description_lower)
    if match:
        return int(match.group(1))
    
    # Pattern: "5-7 years", "5 to 7 years"
    match = re.search(r'(\d+)\s*(?:to|-|and)\s*(\d+)\s*years', description_lower)
    if match:
        # Return lower bound for conservativeness
        return int(match.group(1))
    
    return None


def _score_level(
    title: str,
    description: str,
    years_experience: Optional[int],
    level_patterns: dict
) -> dict:
    """
    Scores a specific seniority level based on keyword matching and years.
    
    Returns:
        dict with "confidence", "keywords_matched", "reasoning"
    """
    title_lower = title.lower()
    desc_lower = description.lower()
    
    title_matches = []
    desc_matches = []
    confidence = 0.0
    
    # ── Check title keywords (high weight) ──────────────────────────────────
    
    for pattern in level_patterns["title_keywords"]:
        if re.search(pattern, title_lower, re.IGNORECASE):
            title_matches.append(pattern)
            confidence += 0.40  # Title match = high confidence
    
    # ── Check description keywords ─────────────────────────────────────────
    
    for keyword in level_patterns["desc_keywords"]:
        if keyword.lower() in desc_lower:
            desc_matches.append(keyword)
            confidence += 0.15  # Description match = moderate confidence
    
    # ── Check years of experience ──────────────────────────────────────────
    
    if years_experience is not None:
        min_yrs = level_patterns["min_years"]
        max_yrs = level_patterns["max_years"]
        
        if min_yrs <= years_experience <= max_yrs:
            confidence += 0.25  # Years match exactly = good boost
        elif min_yrs <= years_experience <= max_yrs + 3:
            confidence += 0.10  # Slightly outside range = small boost
    
    # ── Apply base confidence multiplier ────────────────────────────────────
    
    confidence = min(confidence * level_patterns["confidence_base"], 1.0)
    
    all_matches = title_matches + desc_matches
    
    reasoning = _build_reasoning(
        level_patterns,
        title_matches,
        desc_matches,
        years_experience
    )
    
    return {
        "confidence": confidence,
        "keywords_matched": all_matches,
        "reasoning": reasoning
    }


def _build_reasoning(
    level_patterns: dict,
    title_matches: list,
    desc_matches: list,
    years_experience: Optional[int]
) -> str:
    """Builds human-readable explanation of seniority detection."""
    parts = []
    
    if title_matches:
        parts.append(f"Title keywords: {', '.join(title_matches[:2])}")
    
    if desc_matches:
        parts.append(f"Description: {', '.join(desc_matches[:2])}")
    
    if years_experience is not None:
        min_yrs = level_patterns["min_years"]
        max_yrs = level_patterns["max_years"]
        if min_yrs <= years_experience <= max_yrs:
            parts.append(f"{years_experience} years (expected: {min_yrs}-{max_yrs})")
    
    return " | ".join(parts) if parts else "No specific indicators"


# ══════════════════════════════════════════════════════════════════════════════
# Classification Helper (for database updates)
# ══════════════════════════════════════════════════════════════════════════════

def classify_seniority(title: str, description: str) -> str:
    """
    Simple wrapper that returns just the level string.
    
    Returns:
        One of: "junior", "mid", "senior", "lead", "unspecified"
    """
    result = extract_seniority(title, description)
    return result.level


# ══════════════════════════════════════════════════════════════════════════════
# Batch Processing (for ETL pipeline)
# ══════════════════════════════════════════════════════════════════════════════

def extract_seniority_batch(jobs: list[dict]) -> list[dict]:
    """
    Extracts seniority for a batch of jobs.
    
    Args:
        jobs: List of dict with "title" and "description" keys
    
    Returns:
        Same list with "seniority" key added to each job
    """
    for job in jobs:
        try:
            result = extract_seniority(
                job.get("title", ""),
                job.get("description", "")
            )
            job["seniority"] = result.level
            job["seniority_confidence"] = result.confidence
        except Exception as e:
            logger.warning(
                f"Seniority extraction failed for job",
                job_id=job.get("id"),
                error=str(e)
            )
            job["seniority"] = "unspecified"
            job["seniority_confidence"] = 0.0
    
    return jobs
