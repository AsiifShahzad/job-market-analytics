"""
NLP-based skill extraction with taxonomy and required/preferred categorization.
Replaces regex-only approach with language-aware pattern matching and JD section detection.
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import structlog

logger = structlog.get_logger(__name__)


# Comprehensive skill taxonomy (80+ skills across 6 categories)
SKILL_TAXONOMY = {
    # Languages
    "python": {"category": "language", "aliases": ["python", "py", "python3", "python 3"]},
    "typescript": {"category": "language", "aliases": ["typescript", "ts"]},
    "javascript": {"category": "language", "aliases": ["javascript", "js", "node.js", "nodejs", "es6", "es2020"]},
    "java": {"category": "language", "aliases": ["java"]},
    "csharp": {"category": "language", "aliases": ["c#", "csharp", "c sharp", ".net"]},
    "go": {"category": "language", "aliases": ["golang", "go lang"]},
    "rust": {"category": "language", "aliases": ["rust", "rustlang"]},
    "scala": {"category": "language", "aliases": ["scala"]},
    "r": {"category": "language", "aliases": ["\\br\\b", "r language", "r programming"]},
    "sql": {"category": "language", "aliases": ["sql", "tsql", "plsql", "t-sql"]},
    "bash": {"category": "language", "aliases": ["bash", "shell", "zsh", "shellscript"]},
    "php": {"category": "language", "aliases": ["php"]},
    "ruby": {"category": "language", "aliases": ["ruby", "rails", "rubyonrails"]},
    "kotlin": {"category": "language", "aliases": ["kotlin"]},
    "swift": {"category": "language", "aliases": ["swift"]},
    "c++": {"category": "language", "aliases": ["c\\+\\+", "cpp"]},
    
    # Frontend Frameworks & Libraries
    "react": {"category": "framework", "aliases": ["react", "reactjs", "react.js", "reactnative"]},
    "vue": {"category": "framework", "aliases": ["vue", "vuejs", "vue.js"]},
    "angular": {"category": "framework", "aliases": ["angular", "angularjs"]},
    "nextjs": {"category": "framework", "aliases": ["next.js", "nextjs", "next", "next js"]},
    "gatsby": {"category": "framework", "aliases": ["gatsby"]},
    "svelte": {"category": "framework", "aliases": ["svelte"]},
    "html": {"category": "framework", "aliases": ["html", "html5", "html 5"]},
    "css": {"category": "framework", "aliases": ["css", "css3", "tailwindcss", "tailwind", "bootstrap"]},
    
    # Backend Frameworks
    "fastapi": {"category": "framework", "aliases": ["fastapi", "fast api"]},
    "django": {"category": "framework", "aliases": ["django", "django rest"]},
    "flask": {"category": "framework", "aliases": ["flask"]},
    "spring": {"category": "framework", "aliases": ["spring", "spring boot", "springboot"]},
    "express": {"category": "framework", "aliases": ["express", "express.js"]},
    "nestjs": {"category": "framework", "aliases": ["nest.js", "nestjs", "nest"]},
    "dotnet": {"category": "framework", "aliases": [".net", "dotnet", "asp.net", "asp net"]},
    "graphql": {"category": "framework", "aliases": ["graphql", "graph ql"]},
    "grpc": {"category": "framework", "aliases": ["grpc"]},
    
    # ML/AI Frameworks
    "pytorch": {"category": "framework", "aliases": ["pytorch", "torch"]},
    "tensorflow": {"category": "framework", "aliases": ["tensorflow", "tf"]},
    "keras": {"category": "framework", "aliases": ["keras"]},
    "huggingface": {"category": "framework", "aliases": ["huggingface", "hugging face"]},
    "langchain": {"category": "framework", "aliases": ["langchain", "lang chain"]},
    "transformers": {"category": "framework", "aliases": ["transformers", "gpt", "bert"]},
    
    # Cloud Platforms & Services
    "aws": {"category": "cloud", "aliases": ["aws", "amazon", "amazon web services", "ec2", "lambda", "rds"]},
    "gcp": {"category": "cloud", "aliases": ["gcp", "google cloud", "cloud.google", "bigquery"]},
    "azure": {"category": "cloud", "aliases": ["azure", "microsoft azure", "cosmos db"]},
    "heroku": {"category": "cloud", "aliases": ["heroku"]},
    "vercel": {"category": "cloud", "aliases": ["vercel"]},
    "netlify": {"category": "cloud", "aliases": ["netlify"]},
    "digitalocean": {"category": "cloud", "aliases": ["digitalocean", "digital ocean"]},
    
    # Databases
    "postgresql": {"category": "data", "aliases": ["postgresql", "postgres", "pg", "psql"]},
    "mysql": {"category": "data", "aliases": ["mysql"]},
    "mongodb": {"category": "data", "aliases": ["mongodb", "mongo"]},
    "dynamodb": {"category": "data", "aliases": ["dynamodb", "dynamo"]},
    "cassandra": {"category": "data", "aliases": ["cassandra"]},
    "redis": {"category": "data", "aliases": ["redis", "memcached"]},
    "elasticsearch": {"category": "data", "aliases": ["elasticsearch", "elastic"]},
    "snowflake": {"category": "data", "aliases": ["snowflake"]},
    "bigquery": {"category": "data", "aliases": ["bigquery", "big query"]},
    "redshift": {"category": "data", "aliases": ["redshift"]},
    
    # Data Processing & Pipelines
    "spark": {"category": "data", "aliases": ["spark", "pyspark", "apache spark", "databricks"]},
    "hadoop": {"category": "data", "aliases": ["hadoop", "hive"]},
    "kafka": {"category": "data", "aliases": ["kafka", "apache kafka"]},
    "airflow": {"category": "data", "aliases": ["airflow", "apache airflow", "dagster", "prefect"]},
    "dbt": {"category": "data", "aliases": ["dbt", "data build tool"]},
    "pandas": {"category": "data", "aliases": ["pandas", "pd"]},
    "numpy": {"category": "data", "aliases": ["numpy", "np"]},
    "scikit-learn": {"category": "data", "aliases": ["scikit-learn", "sklearn", "scikit learn"]},
    "jupyter": {"category": "data", "aliases": ["jupyter", "notebook", "jupyter notebook"]},
    "plotly": {"category": "data", "aliases": ["plotly"]},
    "seaborn": {"category": "data", "aliases": ["seaborn"]},
    "matplotlib": {"category": "data", "aliases": ["matplotlib"]},
    
    # DevOps & Infrastructure
    "docker": {"category": "tool", "aliases": ["docker", "dockerfile"]},
    "kubernetes": {"category": "tool", "aliases": ["kubernetes", "k8s"]},
    "terraform": {"category": "tool", "aliases": ["terraform", "hcl"]},
    "ansible": {"category": "tool", "aliases": ["ansible"]},
    "git": {"category": "tool", "aliases": ["git", "github", "gitlab", "bitbucket"]},
    "jenkins": {"category": "tool", "aliases": ["jenkins"]},
    "circleci": {"category": "tool", "aliases": ["circle ci", "circleci"]},
    "github": {"category": "tool", "aliases": ["github", "github actions"]},
    "gitlab": {"category": "tool", "aliases": ["gitlab", "gitlab ci"]},
    "nginx": {"category": "tool", "aliases": ["nginx"]},
    "apache": {"category": "tool", "aliases": ["apache"]},
    
    # Soft Skills / Competencies
    "communication": {"category": "soft", "aliases": ["communication", "communicative", "written communication"]},
    "teamwork": {"category": "soft", "aliases": ["teamwork", "team player", "collaboration", "collaborative"]},
    "leadership": {"category": "soft", "aliases": ["leadership", "leader", "lead"]},
    "problem-solving": {"category": "soft", "aliases": ["problem solving", "problem-solving", "analytical"]},
    "agile": {"category": "soft", "aliases": ["agile", "scrum", "kanban"]},
    "mentoring": {"category": "soft", "aliases": ["mentoring", "mentor"]},
}


@dataclass
class ExtractedSkills:
    """Structured output from skill extraction."""
    required_skills: List[str]
    preferred_skills: List[str]
    all_skills: List[str]
    raw_matches: Dict[str, int]
    seniority_level: str
    is_remote: bool


# Job Description Section Detection Markers
REQUIRED_MARKERS = {
    r"\brequired\b",
    r"\bmust have\b",
    r"\brequired skills?\b",
    r"\bnecessary skills?\b",
    r"\bmandatory\b",
    r"\bqualifications?\b",
    r"\brequirements?\b",
}

PREFERRED_MARKERS = {
    r"\bpreferred\b",
    r"\bnice to have\b",
    r"\bdesirable\b",
    r"\bbonus\b",
    r"\ba plus\b",
    r"\bwould be great\b",
}

SENIORITY_PATTERNS = {
    "junior": r"\b(junior|jr\.?|entry.?level|0-2 years?|graduate)\b",
    "mid": r"\b(mid.?level|intermediate|senior|3-6 years?|5-7 years?)\b",
    "senior": r"\b(senior|staff|lead|tech lead|principal|architect|7\+ years?|10+ years?)\b",
}

REMOTE_MARKERS = {
    r"\bremote\b",
    r"\bwork from home\b",
    r"\bwfh\b",
    r"\bdistributed\b",
    r"\basynchronous\b",
}


def split_jd_sections(description: str) -> Dict[str, str]:
    """
    Split job description into required/preferred skills sections.
    
    Args:
        description: Full job description text
        
    Returns:
        Dict with 'required', 'preferred', 'general' sections
    """
    sections = {
        "required": "",
        "preferred": "",
        "general": description,
    }
    
    lines = description.split("\n")
    current_section = "general"
    section_content = []
    
    for line in lines:
        line_lower = line.lower()
        
        # Check for section markers
        if any(re.search(marker, line_lower) for marker in REQUIRED_MARKERS):
            if section_content:
                sections[current_section] = "\n".join(section_content)
                section_content = []
            current_section = "required"
            logger.debug("detected_required_section", line_sample=line[:50])
            continue
            
        if any(re.search(marker, line_lower) for marker in PREFERRED_MARKERS):
            if section_content:
                sections[current_section] = "\n".join(section_content)
                section_content = []
            current_section = "preferred"
            logger.debug("detected_preferred_section", line_sample=line[:50])
            continue
            
        section_content.append(line)
    
    # Store final section
    if section_content:
        sections[current_section] = "\n".join(section_content)
    
    return sections


def extract_seniority(title: str, description: str) -> str:
    """
    Extract seniority level from title and description.
    
    Args:
        title: Job title
        description: Job description
        
    Returns:
        Seniority level: 'junior', 'mid', or 'senior'
    """
    combined_text = f"{title} {description}".lower()
    
    # Check seniority patterns in order (senior > mid > junior)
    for level in ["senior", "mid", "junior"]:
        if re.search(SENIORITY_PATTERNS[level], combined_text):
            return level
    
    # Default to mid if unable to determine
    return "mid"


def is_remote_opportunity(description: str) -> bool:
    """
    Detect if job allows remote work.
    
    Args:
        description: Job description text
        
    Returns:
        True if remote work is mentioned
    """
    description_lower = description.lower()
    return any(re.search(marker, description_lower) for marker in REMOTE_MARKERS)


def extract_skills_from_text(text: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Extract skills from text using taxonomy matching.
    
    Args:
        text: Text to search for skills
        
    Returns:
        Tuple of (skill_list, raw_match_counts)
    """
    skills_found = []
    match_counts = {}
    text_lower = text.lower()
    
    for skill_name, skill_info in SKILL_TAXONOMY.items():
        aliases = skill_info["aliases"]
        for alias in aliases:
            # Case-insensitive regex match with word boundaries
            pattern = rf"\b{re.escape(alias)}\b"
            matches = len(re.findall(pattern, text_lower))
            
            if matches > 0:
                skills_found.append(skill_name)
                match_counts[skill_name] = match_counts.get(skill_name, 0) + matches
                logger.debug(
                    "skill_matched",
                    skill=skill_name,
                    alias=alias,
                    match_count=matches,
                )
    
    # Deduplicate while preserving order
    unique_skills = list(dict.fromkeys(skills_found))
    return unique_skills, match_counts


def extract_skills(
    title: str,
    description: str,
    log_context: Optional[Dict] = None,
) -> ExtractedSkills:
    """
    Extract skills, seniority, and remote status from job posting.
    
    Args:
        title: Job title
        description: Job description
        log_context: Optional logging context (e.g., job_id)
        
    Returns:
        ExtractedSkills dataclass with all fields populated
    """
    logger.info("skill_extraction_started", title_length=len(title), desc_length=len(description))
    
    # Split into required/preferred sections
    sections = split_jd_sections(description)
    
    # Extract skills from each section
    required_skills, required_matches = extract_skills_from_text(sections["required"])
    preferred_skills, preferred_matches = extract_skills_from_text(sections["preferred"])
    all_text_skills, all_matches = extract_skills_from_text(description)
    
    # Extract auxiliary features
    seniority = extract_seniority(title, description)
    is_remote = is_remote_opportunity(description)
    
    # Combine matches
    combined_matches = {**required_matches, **preferred_matches, **all_matches}
    
    logger.info(
        "skill_extraction_completed",
        required_count=len(required_skills),
        preferred_count=len(preferred_skills),
        total_unique=len(all_text_skills),
        seniority=seniority,
        is_remote=is_remote,
    )
    
    return ExtractedSkills(
        required_skills=required_skills,
        preferred_skills=preferred_skills,
        all_skills=all_text_skills,
        raw_matches=combined_matches,
        seniority_level=seniority,
        is_remote=is_remote,
    )
