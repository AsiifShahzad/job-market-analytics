"""
Quick test/demo script for seniority extraction module.

Usage:
    python -m backend.src.nlp.test_seniority

Shows example extractions for various job titles and descriptions.
"""

from src.nlp.seniority import extract_seniority, classify_seniority

# Test cases: (title, description_snippet, expected_level)
TEST_CASES = [
    (
        "Junior Software Developer",
        "Entry-level position for recent graduates. We're looking for a junior developer with 0-2 years experience.",
        "junior"
    ),
    (
        "Software Engineer II",
        "Mid-level engineer with 3-5 years of experience. You'll lead small project teams and mentor junior members.",
        "mid"
    ),
    (
        "Senior Backend Engineer",
        "We seek an experienced backend engineer with 7+ years in production systems. You'll architect solutions and guide technical direction.",
        "senior"
    ),
    (
        "Staff Engineer / Principal Architect",
        "Looking for a staff engineer with 10+ years of proven track record. You'll set technical strategy and build high-performing teams.",
        "lead"
    ),
    (
        "Software Developer",
        "Looking for a software developer to build features. Experience required in Python and JavaScript.",
        "unspecified"
    ),
    (
        "Engineering Manager",
        "Manager role leading a team of 5-10 engineers. Must have team leadership experience.",
        "lead"
    ),
    (
        "Mid-Level Data Scientist",
        "Mid-career data scientist with 4 years experience. Take ownership of ML pipeline projects.",
        "mid"
    ),
    (
        "Trainee Software Engineer",
        "Fresh graduate trainee program. Learn from senior engineers over 1 year.",
        "junior"
    ),
]


def main():
    print("=" * 80)
    print("SENIORITY EXTRACTION TEST SUITE")
    print("=" * 80)
    print()
    
    correct = 0
    total = len(TEST_CASES)
    
    for title, description, expected in TEST_CASES:
        result = extract_seniority(title, description)
        is_correct = result.level == expected
        correct += is_correct
        
        status = "✓" if is_correct else "✗"
        
        print(f"{status} Title: {title}")
        print(f"   Description: {description[:80]}...")
        print(f"   Expected: {expected}")
        print(f"   Got: {result.level} (confidence: {result.confidence:.2f})")
        print(f"   Reasoning: {result.reasoning}")
        print()
    
    accuracy = (correct / total) * 100
    print("=" * 80)
    print(f"ACCURACY: {correct}/{total} ({accuracy:.0f}%)")
    print("=" * 80)


if __name__ == "__main__":
    main()
