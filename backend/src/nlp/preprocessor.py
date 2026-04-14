import re
import spacy
import logging
from typing import List, Generator

logger = logging.getLogger(__name__)

# Load spaCy model. We disable components we don't need to speed up pipeline processing.
# 'ner' and 'parser' can be disabled if we only want lemmatization and basic POS tagging.
try:
    nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])
except OSError:
    logger.warning("spaCy 'en_core_web_sm' model not found. Run: python -m spacy download en_core_web_sm")
    # Fallback to a blank model if not installed, though lemmatization won't work well
    nlp = spacy.blank("en")

# Custom rule: Prevent tokenization of common technical terms that contain punctuation
# E.g., 'C++', 'Node.js', 'React.js', '.NET'
TECHNICAL_TERMS = ["c++", "c#", "node.js", "react.js", "vue.js", ".net", "next.js", "f#"]
for term in TECHNICAL_TERMS:
    nlp.tokenizer.add_special_case(term, [{"ORTH": term}])

def _clean_html(text: str) -> str:
    """Removes HTML tags and unescapes common entities."""
    if not text:
        return ""
    # Remove HTML tags using regex for high performance
    text = re.sub(r'<[^>]+>', ' ', text)
    # Handle basic HTML entities
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    # Remove extra whitespace
    return re.sub(r'\s+', ' ', text).strip()

def preprocess_batch(texts: List[str], batch_size: int = 50, n_process: int = 1) -> Generator[str, None, None]:
    """
    Optimized batch processor for job descriptions.
    
    Pipeline:
    1. Clean HTML
    2. Lowercase
    3. Process through spaCy via nlp.pipe()
    4. Extract lemmas while preserving technical terminology
    
    Args:
        texts: List of raw job descriptions
        batch_size: Number of texts to buffer in memory
        n_process: Number of CPU cores to use (set > 1 for multiprocessing)
        
    Yields:
        Cleaned, lemmatized string.
    """
    # 1 & 2: Pre-clean and lowercase entirely string-side before hitting spaCy
    # This is much faster than doing it inside spaCy.
    cleaned_texts = (_clean_html(t).lower() for t in texts)
    
    # 3: Stream through spaCy's optimized batch pipeline
    # The 'nlp.pipe' function is designed exactly for this type of workload.
    for doc in nlp.pipe(cleaned_texts, batch_size=batch_size, n_process=n_process):
        # 4: Extract lemmas
        # We target specific parts of speech where lemmatization adds value (nouns, verbs, adjectives).
        # We skip punctuation and spacing naturally.
        lemmas = []
        for token in doc:
            if token.is_space or token.is_punct:
                # Keep technical terms that happen to be classified as punct because of special rules
                if token.text in TECHNICAL_TERMS:
                    lemmas.append(token.text)
                continue
                
            # If the original text is a known preserved technical term, keep it exactly as-is.
            if token.text in TECHNICAL_TERMS:
                lemmas.append(token.text)
            else:
                # For standard words, grab the lemmatized root form.
                # If lemma is pronoun (-PRON-), we just use the lower text anyway to avoid noisy "-PRON-" string
                if token.lemma_ == "-PRON-":
                    lemmas.append(token.text)
                else:
                    lemmas.append(token.lemma_)
                    
        yield " ".join(lemmas)

def preprocess_text(text: str) -> str:
    """Convenience wrapper for processing a single string."""
    return next(preprocess_batch([text]))
