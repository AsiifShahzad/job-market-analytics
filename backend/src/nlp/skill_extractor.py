"""
Skill extractor — pure whitelist approach.

NO spaCy, NO ML classifier, NO candidate detection.
Only matches skills from a hardcoded technical whitelist.
Soft skills (leadership, communication, etc.) are completely excluded.

This guarantees only real technical skills appear in the output:
Python, AWS, React, Docker, SQL, etc.
"""

import re
import logging
from dataclasses import dataclass
from typing import List, Dict

logger = logging.getLogger(__name__)


# ── Technical skills whitelist ────────────────────────────────────────────────
# Format: "Display Name": {"cat": "category", "aliases": ["alias1", "alias2"]}
# Aliases matched case-insensitively as whole words.
# Soft skills, company names, and generic terms are intentionally excluded.

SKILLS_WHITELIST: Dict[str, Dict] = {

    # ── Languages ──────────────────────────────────────────────────────────────
    "Python":       {"cat": "language", "aliases": ["python", "python3", "python 3"]},
    "JavaScript":   {"cat": "language", "aliases": ["javascript", "java script", "es6", "ecmascript"]},
    "TypeScript":   {"cat": "language", "aliases": ["typescript"]},
    "Java":         {"cat": "language", "aliases": ["java"]},
    "Go":           {"cat": "language", "aliases": ["golang", "go lang"]},
    "Rust":         {"cat": "language", "aliases": ["rust lang", "rust programming"]},
    "C++":          {"cat": "language", "aliases": ["c++", "cpp", "c plus plus"]},
    "C#":           {"cat": "language", "aliases": ["c#", "csharp", "c sharp"]},
    "PHP":          {"cat": "language", "aliases": ["php"]},
    "Ruby":         {"cat": "language", "aliases": ["ruby on rails", "ror"]},
    "Swift":        {"cat": "language", "aliases": ["swift", "swiftui"]},
    "Kotlin":       {"cat": "language", "aliases": ["kotlin"]},
    "Scala":        {"cat": "language", "aliases": ["scala"]},
    "R":            {"cat": "language", "aliases": ["r programming", "r language", "rstudio"]},
    "SQL":          {"cat": "language", "aliases": ["sql", "t-sql", "tsql", "pl/sql"]},
    "Bash":         {"cat": "language", "aliases": ["bash", "shell scripting", "shell script"]},
    "MATLAB":       {"cat": "language", "aliases": ["matlab"]},
    "Elixir":       {"cat": "language", "aliases": ["elixir"]},
    "Haskell":      {"cat": "language", "aliases": ["haskell"]},
    "Dart":         {"cat": "language", "aliases": ["dart"]},
    "Lua":          {"cat": "language", "aliases": ["lua"]},
    "Perl":         {"cat": "language", "aliases": ["perl"]},
    "Assembly":     {"cat": "language", "aliases": ["assembly language", "asm"]},

    # ── Web Frameworks ─────────────────────────────────────────────────────────
    "React":        {"cat": "framework", "aliases": ["react", "reactjs", "react.js"]},
    "Next.js":      {"cat": "framework", "aliases": ["next.js", "nextjs", "next js"]},
    "Vue":          {"cat": "framework", "aliases": ["vue.js", "vuejs", "nuxt.js", "nuxtjs"]},
    "Angular":      {"cat": "framework", "aliases": ["angular", "angularjs"]},
    "Svelte":       {"cat": "framework", "aliases": ["svelte", "sveltekit"]},
    "FastAPI":      {"cat": "framework", "aliases": ["fastapi", "fast api"]},
    "Django":       {"cat": "framework", "aliases": ["django", "django rest framework", "drf"]},
    "Flask":        {"cat": "framework", "aliases": ["flask"]},
    "Spring Boot":  {"cat": "framework", "aliases": ["spring boot", "spring framework", "spring mvc"]},
    "Express":      {"cat": "framework", "aliases": ["express.js", "expressjs"]},
    "NestJS":       {"cat": "framework", "aliases": ["nestjs", "nest.js"]},
    "Node.js":      {"cat": "framework", "aliases": ["node.js", "nodejs"]},
    "GraphQL":      {"cat": "framework", "aliases": ["graphql", "apollo graphql"]},
    "gRPC":         {"cat": "framework", "aliases": ["grpc", "protobuf", "protocol buffers"]},
    "REST API":     {"cat": "framework", "aliases": ["rest api", "restful api"]},
    "Tailwind CSS": {"cat": "framework", "aliases": ["tailwind css", "tailwindcss"]},
    "Laravel":      {"cat": "framework", "aliases": ["laravel"]},
    ".NET":         {"cat": "framework", "aliases": [".net core", "asp.net", "dotnet"]},
    "Redux":        {"cat": "framework", "aliases": ["redux", "redux toolkit"]},
    "jQuery":       {"cat": "framework", "aliases": ["jquery"]},
    "Flutter":      {"cat": "framework", "aliases": ["flutter"]},
    "React Native": {"cat": "framework", "aliases": ["react native"]},
    "Electron":     {"cat": "framework", "aliases": ["electron.js", "electronjs"]},

    # ── Cloud ──────────────────────────────────────────────────────────────────
    "AWS":          {"cat": "cloud", "aliases": ["aws", "amazon web services", "ec2", "s3 bucket", "aws lambda", "eks", "ecs", "cloudformation", "sagemaker", "cloudwatch"]},
    "Google Cloud": {"cat": "cloud", "aliases": ["gcp", "google cloud platform", "cloud run", "gke", "google kubernetes engine", "cloud functions"]},
    "Azure":        {"cat": "cloud", "aliases": ["microsoft azure", "azure devops", "azure functions", "aks", "azure kubernetes"]},
    "Vercel":       {"cat": "cloud", "aliases": ["vercel"]},
    "Heroku":       {"cat": "cloud", "aliases": ["heroku"]},
    "Cloudflare":   {"cat": "cloud", "aliases": ["cloudflare workers"]},
    "Netlify":      {"cat": "cloud", "aliases": ["netlify"]},
    "DigitalOcean": {"cat": "cloud", "aliases": ["digitalocean", "digital ocean"]},

    # ── Databases ──────────────────────────────────────────────────────────────
    "PostgreSQL":    {"cat": "data", "aliases": ["postgresql", "postgres"]},
    "MySQL":         {"cat": "data", "aliases": ["mysql", "mariadb"]},
    "MongoDB":       {"cat": "data", "aliases": ["mongodb", "mongo db"]},
    "Redis":         {"cat": "data", "aliases": ["redis"]},
    "Elasticsearch": {"cat": "data", "aliases": ["elasticsearch", "elastic search", "opensearch", "elk stack"]},
    "Snowflake":     {"cat": "data", "aliases": ["snowflake"]},
    "BigQuery":      {"cat": "data", "aliases": ["bigquery", "big query"]},
    "Cassandra":     {"cat": "data", "aliases": ["cassandra", "apache cassandra"]},
    "DynamoDB":      {"cat": "data", "aliases": ["dynamodb"]},
    "SQLite":        {"cat": "data", "aliases": ["sqlite"]},
    "Neo4j":         {"cat": "data", "aliases": ["neo4j"]},
    "Supabase":      {"cat": "data", "aliases": ["supabase"]},
    "Firebase":      {"cat": "data", "aliases": ["firebase", "firestore"]},
    "Oracle DB":     {"cat": "data", "aliases": ["oracle database", "oracle db"]},

    # ── Data & ML ──────────────────────────────────────────────────────────────
    "Pandas":            {"cat": "data", "aliases": ["pandas"]},
    "NumPy":             {"cat": "data", "aliases": ["numpy"]},
    "Apache Spark":      {"cat": "data", "aliases": ["apache spark", "pyspark", "spark sql"]},
    "Apache Kafka":      {"cat": "data", "aliases": ["apache kafka", "kafka streams"]},
    "Apache Airflow":    {"cat": "data", "aliases": ["apache airflow"]},
    "dbt":               {"cat": "data", "aliases": ["dbt", "data build tool"]},
    "TensorFlow":        {"cat": "data", "aliases": ["tensorflow", "keras"]},
    "PyTorch":           {"cat": "data", "aliases": ["pytorch"]},
    "scikit-learn":      {"cat": "data", "aliases": ["scikit-learn", "sklearn", "scikit learn"]},
    "Hugging Face":      {"cat": "data", "aliases": ["hugging face", "huggingface"]},
    "LangChain":         {"cat": "data", "aliases": ["langchain"]},
    "Machine Learning":  {"cat": "data", "aliases": ["machine learning"]},
    "Deep Learning":     {"cat": "data", "aliases": ["deep learning", "neural networks"]},
    "NLP":               {"cat": "data", "aliases": ["natural language processing"]},
    "Computer Vision":   {"cat": "data", "aliases": ["computer vision", "opencv", "object detection"]},
    "MLOps":             {"cat": "data", "aliases": ["mlops", "mlflow", "kubeflow"]},
    "Databricks":        {"cat": "data", "aliases": ["databricks"]},
    "Tableau":           {"cat": "data", "aliases": ["tableau"]},
    "Power BI":          {"cat": "data", "aliases": ["power bi", "powerbi"]},
    "Looker":            {"cat": "data", "aliases": ["looker"]},
    "Jupyter":           {"cat": "data", "aliases": ["jupyter", "jupyter notebook"]},
    "Generative AI":     {"cat": "data", "aliases": ["generative ai", "gen ai", "llm", "large language model"]},

    # ── DevOps & Tools ─────────────────────────────────────────────────────────
    "Docker":          {"cat": "tool", "aliases": ["docker", "dockerfile", "docker compose", "docker-compose"]},
    "Kubernetes":      {"cat": "tool", "aliases": ["kubernetes", "k8s", "kubectl", "helm"]},
    "Terraform":       {"cat": "tool", "aliases": ["terraform", "terragrunt"]},
    "Git":             {"cat": "tool", "aliases": ["git"]},
    "GitHub":          {"cat": "tool", "aliases": ["github"]},
    "GitHub Actions":  {"cat": "tool", "aliases": ["github actions"]},
    "GitLab":          {"cat": "tool", "aliases": ["gitlab", "gitlab ci"]},
    "CI/CD":           {"cat": "tool", "aliases": ["ci/cd", "continuous integration", "continuous delivery", "continuous deployment"]},
    "Jenkins":         {"cat": "tool", "aliases": ["jenkins"]},
    "Linux":           {"cat": "tool", "aliases": ["linux", "ubuntu", "debian", "centos"]},
    "Ansible":         {"cat": "tool", "aliases": ["ansible"]},
    "Prometheus":      {"cat": "tool", "aliases": ["prometheus"]},
    "Grafana":         {"cat": "tool", "aliases": ["grafana"]},
    "Datadog":         {"cat": "tool", "aliases": ["datadog"]},
    "Nginx":           {"cat": "tool", "aliases": ["nginx"]},
    "Celery":          {"cat": "tool", "aliases": ["celery"]},
    "RabbitMQ":        {"cat": "tool", "aliases": ["rabbitmq", "rabbit mq"]},
    "Webpack":         {"cat": "tool", "aliases": ["webpack"]},
    "Vite":            {"cat": "tool", "aliases": ["vite"]},
    "Jest":            {"cat": "tool", "aliases": ["jest"]},
    "Cypress":         {"cat": "tool", "aliases": ["cypress"]},
    "Selenium":        {"cat": "tool", "aliases": ["selenium"]},
    "Postman":         {"cat": "tool", "aliases": ["postman"]},
    "Jira":            {"cat": "tool", "aliases": ["jira"]},
    "Figma":           {"cat": "tool", "aliases": ["figma"]},
    "Xcode":           {"cat": "tool", "aliases": ["xcode"]},
    "Android Studio":  {"cat": "tool", "aliases": ["android studio"]},
    "Microservices":   {"cat": "tool", "aliases": ["microservices", "micro services", "service mesh"]},
    "WebAssembly":     {"cat": "tool", "aliases": ["webassembly", "wasm"]},
    "Agile/Scrum":     {"cat": "tool", "aliases": ["agile scrum", "scrum methodology"]},
    "System Design":   {"cat": "tool", "aliases": ["system design", "distributed systems"]},
    "iOS Dev":         {"cat": "platform", "aliases": ["ios development", "iphone development"]},
    "Android Dev":     {"cat": "platform", "aliases": ["android development", "android sdk"]},
}

# ── Build lookup ──────────────────────────────────────────────────────────────
# alias_lower → (canonical, category), sorted longest-first

_ALIAS_MAP: Dict[str, tuple] = {}
for _canonical, _info in SKILLS_WHITELIST.items():
    for _alias in _info["aliases"]:
        _ALIAS_MAP[_alias.lower()] = (_canonical, _info["cat"])

_SORTED_ALIASES = sorted(_ALIAS_MAP.items(), key=lambda x: len(x[0]), reverse=True)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ExtractedSkill:
    name: str
    category: str
    requirement_level: str = "mentioned"
    context_score: float = 0.3
    years_required: int = 0
    source: str = "whitelist"
    classifier_confidence: float = 1.0


@dataclass
class ExtractionResult:
    all_skills: List[str]
    required_skills: List[str]
    preferred_skills: List[str]
    details: List[ExtractedSkill]
    skill_count: int = 0

    def __post_init__(self):
        self.skill_count = len(self.all_skills)


# ── Context signals ───────────────────────────────────────────────────────────

_REQUIRED_RE = re.compile(
    r"\b(must have|required|mandatory|essential|proficient in|"
    r"expertise in|strong experience|proven experience|hands.on|"
    r"expert in|deep knowledge|minimum \d+ years?)\b",
    re.IGNORECASE,
)
_PREFERRED_RE = re.compile(
    r"\b(preferred|nice to have|bonus|plus|advantageous|desirable|"
    r"ideally|familiarity with|exposure to|is a plus)\b",
    re.IGNORECASE,
)


def _get_requirement_level(description: str, alias: str) -> tuple:
    """Check sentence context around alias. Returns (level, score)."""
    try:
        alias_re = re.compile(r"\b" + re.escape(alias) + r"\b", re.IGNORECASE)
    except re.error:
        return "mentioned", 0.3

    for sentence in re.split(r"[.!\n;]+", description):
        if not alias_re.search(sentence):
            continue
        if _REQUIRED_RE.search(sentence):
            return "required", 0.8
        if _PREFERRED_RE.search(sentence):
            return "preferred", 0.5

    return "mentioned", 0.3


# ── Main extractor ────────────────────────────────────────────────────────────

def extract_skills(title: str, description: str) -> ExtractionResult:
    """
    Extract technical skills using pure whitelist matching.
    Only returns skills explicitly listed in SKILLS_WHITELIST.
    Guarantees no company names, state codes, or soft skills in output.
    """
    text = f"{title}\n\n{description}"
    text_lower = text.lower()

    found: Dict[str, ExtractedSkill] = {}
    matched_spans: set = set()

    for alias_lower, (canonical, category) in _SORTED_ALIASES:
        try:
            escaped = re.escape(alias_lower)
            first_char = alias_lower[0]
            last_char = alias_lower[-1]
            prefix = r"\b" if re.match(r"\w", first_char) else r"(?<![.\w])"
            suffix = r"\b" if re.match(r"\w", last_char) else r"(?![.\w])"
            pattern = re.compile(prefix + escaped + suffix, re.IGNORECASE)
        except re.error:
            continue

        for match in pattern.finditer(text_lower):
            start, end = match.start(), match.end()

            # Skip spans already covered by a longer match
            if any(not (end <= s or start >= e) for s, e in matched_spans):
                continue

            matched_spans.add((start, end))

            if canonical not in found:
                level, score = _get_requirement_level(description, alias_lower)
                found[canonical] = ExtractedSkill(
                    name=canonical,
                    category=category,
                    requirement_level=level,
                    context_score=score,
                )
            else:
                level, score = _get_requirement_level(description, alias_lower)
                if score > found[canonical].context_score:
                    found[canonical].context_score = score
                    found[canonical].requirement_level = level

    details = sorted(found.values(), key=lambda s: s.context_score, reverse=True)
    all_skills = [s.name for s in details]
    required   = [s.name for s in details if s.requirement_level == "required"]
    preferred  = [s.name for s in details if s.requirement_level in ("preferred", "nice_to_have")]

    logger.debug("Whitelist extraction: total=%d required=%d", len(all_skills), len(required))

    return ExtractionResult(
        all_skills=all_skills,
        required_skills=required,
        preferred_skills=preferred,
        details=details,
    )


# ── Compatibility shims ───────────────────────────────────────────────────────

def get_skill_names(title: str, description: str) -> List[str]:
    return extract_skills(title, description).all_skills


def get_category(skill_name: str) -> str:
    entry = SKILLS_WHITELIST.get(skill_name)
    return entry["cat"] if entry else "tool"
